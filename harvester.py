#!/usr/bin/env python3
"""24-hour Solana memecoin data harvester.

Pipeline:
    PumpPortal WebSocket -> new pump.fun mints
    RugCheck             -> static security (risk score, authorities)
    DexScreener          -> price / market cap / liquidity / 5m trades, every 60s
    Helius RPC           -> top-10 non-LP holder %, every 15 minutes
    SQLite               -> solana_memecoin_24h_dataset.db

Each token is watched for up to 3 hours after launch. All services used are
free tier. Holder percentages use live total supply as the circulating-supply
proxy, because no free API exposes true circulating supply.

Setup:
    cp .env.example .env      # fill in HELIUS_API_KEY
    pip install -r requirements.txt
    python harvester.py

Stop with Ctrl+C. The database runs in WAL mode, so it can be queried while
the harvester is still writing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

# ---------------------------------------------------------------- endpoints

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"
DEXSCREENER_TOKENS = "https://api.dexscreener.com/latest/dex/tokens/"
RUGCHECK_REPORT = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"
HELIUS_RPC_URL = "https://mainnet.helius-rpc.com/?api-key={key}"

SYSTEM_PROGRAM = "11111111111111111111111111111111"

# Raydium AMM v4 pools hold liquidity in token accounts owned by this shared
# authority. It is a plain system account, so the program-ownership heuristic
# below cannot detect it and it has to be excluded by address.
KNOWN_LP_AUTHORITIES = {
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
}

# Quote mints whose pairs give trustworthy USD prices. A scam token can create
# a pool quoted in a worthless asset, inflate that asset's nominal USD value,
# and thereby make its own liquidity and implied price look huge, so a trusted
# quote is preferred over raw liquidity.
TRUSTED_QUOTE_MINTS = {
    "So11111111111111111111111111111111111111112",   # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
}

# ---------------------------------------------------------------- tuning

WATCH_SECONDS = 3 * 60 * 60
DEX_POLL_SECONDS = 60
HOLDER_POLL_SECONDS = 15 * 60
DEX_BATCH_SIZE = 30

RUGCHECK_CONCURRENCY = 6
RUGCHECK_ATTEMPTS = 6
RUGCHECK_RETRY_DELAYS = (5.0, 10.0, 20.0, 40.0, 60.0)

HELIUS_RPS = 4.5                # stay under Helius' 5 requests/second
HELIUS_RPC_ATTEMPTS = 3

DEFAULT_RUN_HOURS = 24.0
DEFAULT_MAX_ACTIVE = 1500       # guardrail for free-tier limits and SQLite size
DB_FILENAME = "solana_memecoin_24h_dataset.db"

log = logging.getLogger("harvester")


# ---------------------------------------------------------------- helpers

def load_env(path: str = ".env") -> None:
    """Read simple KEY=VALUE lines from .env without extra dependencies."""
    env_file = Path(path)
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def to_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def to_int(value):
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def chunks(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def supply_ui(result) -> float | None:
    """Extract a human-readable supply from a getTokenSupply result."""
    value = (result or {}).get("value") or {}
    ui_amount = to_float(value.get("uiAmount"))
    if ui_amount is not None:
        return ui_amount
    amount = to_float(value.get("amount"))
    decimals = to_int(value.get("decimals"))
    if amount is not None and decimals is not None:
        return amount / (10 ** decimals)
    return None


def helius_endpoint(key: str) -> str:
    """Accept either a bare API key or a full dashboard RPC URL."""
    key = key.strip()
    if key.startswith("http://") or key.startswith("https://"):
        return key
    return HELIUS_RPC_URL.format(key=key)


# ---------------------------------------------------------------- database

class Database:
    """Small synchronous SQLite wrapper; all calls run on the event-loop thread."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._create_schema()

    def _create_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tokens (
                    mint TEXT PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    deployer_wallet TEXT,
                    created_at TIMESTAMP,
                    initial_rugcheck_score INTEGER,
                    mint_authority_disabled BOOLEAN,
                    freeze_authority_disabled BOOLEAN
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mint TEXT,
                    timestamp TIMESTAMP,
                    price_usd REAL,
                    market_cap REAL,
                    liquidity_usd REAL,
                    buys_5m INTEGER,
                    sells_5m INTEGER,
                    volume_5m REAL,
                    top_10_holder_percent REAL
                );

                CREATE INDEX IF NOT EXISTS idx_snapshots_mint_id
                    ON snapshots (mint, id);
                """
            )

    def add_token(
        self,
        mint: str,
        symbol: str | None,
        name: str | None,
        deployer_wallet: str | None,
        created_at: str,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO tokens
                    (mint, symbol, name, deployer_wallet, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(mint) DO NOTHING
                """,
                (mint, symbol, name, deployer_wallet, created_at),
            )

    def set_security(
        self,
        mint: str,
        score: int | None,
        mint_authority_disabled: int | None,
        freeze_authority_disabled: int | None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE tokens
                   SET initial_rugcheck_score = ?,
                       mint_authority_disabled = ?,
                       freeze_authority_disabled = ?
                 WHERE mint = ?
                """,
                (score, mint_authority_disabled, freeze_authority_disabled, mint),
            )

    def add_snapshot(
        self,
        mint: str,
        timestamp: str,
        price_usd: float | None,
        market_cap: float | None,
        liquidity_usd: float | None,
        buys_5m: int | None,
        sells_5m: int | None,
        volume_5m: float | None,
        top_10_holder_percent: float | None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO snapshots
                    (mint, timestamp, price_usd, market_cap, liquidity_usd,
                     buys_5m, sells_5m, volume_5m, top_10_holder_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mint,
                    timestamp,
                    price_usd,
                    market_cap,
                    liquidity_usd,
                    buys_5m,
                    sells_5m,
                    volume_5m,
                    top_10_holder_percent,
                ),
            )

    def set_holder_percent(self, mint: str, percent: float, timestamp: str) -> None:
        """Attach the newest holder reading to the latest snapshot for a mint."""
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE snapshots
                   SET top_10_holder_percent = ?
                 WHERE id = (
                       SELECT id FROM snapshots
                        WHERE mint = ?
                        ORDER BY id DESC
                        LIMIT 1
                 )
                """,
                (percent, mint),
            )
            if cursor.rowcount == 0:
                self.conn.execute(
                    """
                    INSERT INTO snapshots (mint, timestamp, top_10_holder_percent)
                    VALUES (?, ?, ?)
                    """,
                    (mint, timestamp, percent),
                )

    def counts(self) -> tuple[int, int]:
        tokens = self.conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
        snapshots = self.conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        return tokens, snapshots

    def close(self) -> None:
        self.conn.close()


# ---------------------------------------------------------------- rate limit

class RateLimiter:
    """Enforce a global minimum interval between RPC calls."""

    def __init__(self, rps: float) -> None:
        self._interval = 1.0 / rps
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if self._next_allowed > now:
                await asyncio.sleep(self._next_allowed - now)
                now = time.monotonic()
            self._next_allowed = now + self._interval


# ---------------------------------------------------------------- harvester

class Harvester:
    def __init__(
        self,
        db: Database,
        session: aiohttp.ClientSession,
        helius_url: str | None,
        max_active: int,
        run_seconds: float,
    ) -> None:
        self.db = db
        self.session = session
        self.helius_url = helius_url
        self.max_active = max_active
        self.run_seconds = run_seconds

        self.active: dict[str, float] = {}
        self.holder_pct: dict[str, float] = {}
        self.stop_event = asyncio.Event()
        self.background: set[asyncio.Task] = set()

        self.rpc_id = 0
        self.rpc_limiter = RateLimiter(HELIUS_RPS)
        self.rugcheck_semaphore = asyncio.Semaphore(RUGCHECK_CONCURRENCY)

        self.discovered = 0
        self.dropped = 0
        self.security_ok = 0
        self.security_failed = 0
        self.snapshots_written = 0

    # ------------------------------------------------------------ lifecycle

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self.discover_tokens(), name="discovery"),
            asyncio.create_task(self.poll_dexscreener(), name="dexscreener"),
            asyncio.create_task(self.report_stats(), name="stats"),
        ]
        if self.helius_url:
            tasks.append(asyncio.create_task(self.poll_holders(), name="holders"))
        else:
            log.warning(
                "HELIUS_API_KEY is not set: holder polling is disabled, "
                "everything else still runs."
            )

        try:
            await self._sleep(self.run_seconds)
        finally:
            self.stop_event.set()
            for task in tasks:
                task.cancel()
            for task in list(self.background):
                task.cancel()
            await asyncio.gather(*tasks, *self.background, return_exceptions=True)

            tokens, snapshots = self.db.counts()
            log.info(
                "shutdown summary: discovered=%d dropped=%d security_ok=%d "
                "security_failed=%d snapshots_written=%d db_tokens=%d db_snapshots=%d",
                self.discovered,
                self.dropped,
                self.security_ok,
                self.security_failed,
                self.snapshots_written,
                tokens,
                snapshots,
            )

    async def _sleep(self, seconds: float) -> None:
        if seconds <= 0 or self.stop_event.is_set():
            return
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------ discovery

    async def discover_tokens(self) -> None:
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                async with self.session.ws_connect(
                    PUMPPORTAL_WS, heartbeat=30
                ) as ws:
                    await ws.send_json({"method": "subscribeNewToken"})
                    log.info("PumpPortal connected; subscribed to new tokens")
                    backoff = 1.0

                    async for message in ws:
                        if self.stop_event.is_set():
                            break
                        if message.type == aiohttp.WSMsgType.TEXT:
                            try:
                                event = json.loads(message.data)
                            except json.JSONDecodeError:
                                continue
                            if isinstance(event, dict) and event.get("mint"):
                                try:
                                    self.handle_new_token(event)
                                except Exception:
                                    log.exception(
                                        "failed to register token %s",
                                        event.get("mint"),
                                    )
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            log.warning("PumpPortal websocket error: %s", message.data)
                            break
                        elif message.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSING,
                        ):
                            break
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
                log.warning("PumpPortal connection problem: %s", exc)
            except Exception:
                log.exception("unexpected PumpPortal error")

            if self.stop_event.is_set():
                break
            log.info("reconnecting to PumpPortal in %.0fs", backoff)
            await self._sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def handle_new_token(self, event: dict) -> None:
        mint = event.get("mint")
        if not mint or mint in self.active:
            return
        if len(self.active) >= self.max_active:
            self.dropped += 1
            if self.dropped == 1 or self.dropped % 500 == 0:
                log.warning(
                    "watchlist full at %d tokens; dropped %d new tokens so far",
                    self.max_active,
                    self.dropped,
                )
            return

        now = time.time()
        self.active[mint] = now + WATCH_SECONDS
        self.discovered += 1

        self.db.add_token(
            mint=mint,
            symbol=event.get("symbol"),
            name=event.get("name"),
            deployer_wallet=event.get("traderPublicKey"),
            created_at=utc_now(),
        )

        if self.discovered == 1 or self.discovered % 50 == 0:
            log.info(
                "discovered %d tokens so far; watching %d",
                self.discovered,
                len(self.active),
            )

        task = asyncio.create_task(self.fetch_security(mint))
        self.background.add(task)
        task.add_done_callback(self.background.discard)

    # ------------------------------------------------------------ rugcheck

    async def fetch_security(self, mint: str) -> None:
        url = RUGCHECK_REPORT.format(mint=mint)

        for attempt in range(1, RUGCHECK_ATTEMPTS + 1):
            if self.stop_event.is_set():
                return
            try:
                async with self.rugcheck_semaphore:
                    async with self.session.get(
                        url, timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status == 200:
                            report = await response.json(content_type=None)
                            self._store_security(mint, report)
                            return
                        if response.status in (400, 404):
                            log.debug(
                                "RugCheck has no report yet for %s (attempt %d)",
                                mint,
                                attempt,
                            )
                        elif response.status == 429:
                            log.debug(
                                "RugCheck rate limited on %s (attempt %d)",
                                mint,
                                attempt,
                            )
                        else:
                            log.warning(
                                "RugCheck HTTP %s for %s (attempt %d)",
                                response.status,
                                mint,
                                attempt,
                            )
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                log.warning(
                    "RugCheck request failed for %s (attempt %d): %s",
                    mint,
                    attempt,
                    exc,
                )
            except Exception:
                log.exception("RugCheck data could not be stored for %s", mint)
                return

            if attempt < RUGCHECK_ATTEMPTS:
                await self._sleep(RUGCHECK_RETRY_DELAYS[attempt - 1])

        self.security_failed += 1
        log.debug(
            "RugCheck gave up on %s after %d attempts", mint, RUGCHECK_ATTEMPTS
        )

    def _store_security(self, mint: str, report: dict) -> None:
        token = report.get("token")
        if isinstance(token, dict):
            mint_disabled = 1 if token.get("mintAuthority") is None else 0
            freeze_disabled = 1 if token.get("freezeAuthority") is None else 0
        else:
            mint_disabled = None
            freeze_disabled = None

        score = report.get("score_normalised")
        if score is None:
            score = report.get("score")

        self.db.set_security(
            mint,
            to_int(score),
            mint_disabled,
            freeze_disabled,
        )
        self.security_ok += 1

    # ------------------------------------------------------------ dexscreener

    async def poll_dexscreener(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                self.prune_active()
                mints = list(self.active)
                for batch in chunks(mints, DEX_BATCH_SIZE):
                    if self.stop_event.is_set():
                        break
                    await self.fetch_dex_batch(batch)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("DexScreener poll cycle failed")

            elapsed = time.monotonic() - started
            await self._sleep(max(0.0, DEX_POLL_SECONDS - elapsed))

    def prune_active(self) -> None:
        now = time.time()
        expired = [mint for mint, expiry in self.active.items() if expiry <= now]
        for mint in expired:
            self.active.pop(mint, None)
            self.holder_pct.pop(mint, None)
        if expired:
            log.debug(
                "watch window ended for %d tokens; still watching %d",
                len(expired),
                len(self.active),
            )

    async def fetch_dex_batch(self, mints: list[str]) -> None:
        url = DEXSCREENER_TOKENS + ",".join(mints)
        try:
            async with self.session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 429:
                    log.warning("DexScreener rate limited; skipping this batch")
                    return
                if response.status != 200:
                    log.warning(
                        "DexScreener HTTP %s for a batch of %d mints",
                        response.status,
                        len(mints),
                    )
                    return
                payload = await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log.warning("DexScreener request failed: %s", exc)
            return
        except Exception:
            log.exception("DexScreener response could not be parsed")
            return

        wanted = set(mints)
        best_pair: dict[str, tuple[tuple[bool, float], dict]] = {}

        for pair in payload.get("pairs") or []:
            if pair.get("chainId") != "solana":
                continue
            base = (pair.get("baseToken") or {}).get("address")
            # Only base-side pairs describe this mint's price, FDV and volume.
            # In a quote-side pair those fields belong to the other token.
            if base not in wanted:
                continue
            mint = base
            quote = (pair.get("quoteToken") or {}).get("address")
            liquidity = to_float((pair.get("liquidity") or {}).get("usd")) or 0.0
            rank = (quote in TRUSTED_QUOTE_MINTS, liquidity)
            current = best_pair.get(mint)
            if current is None or rank > current[0]:
                best_pair[mint] = (rank, pair)

        timestamp = utc_now()
        for mint, (_, pair) in best_pair.items():
            txns_5m = (pair.get("txns") or {}).get("m5") or {}
            self.db.add_snapshot(
                mint=mint,
                timestamp=timestamp,
                price_usd=to_float(pair.get("priceUsd")),
                market_cap=to_float(pair.get("fdv")),
                liquidity_usd=to_float((pair.get("liquidity") or {}).get("usd")),
                buys_5m=to_int(txns_5m.get("buys")),
                sells_5m=to_int(txns_5m.get("sells")),
                volume_5m=to_float((pair.get("volume") or {}).get("m5")),
                top_10_holder_percent=self.holder_pct.get(mint),
            )
            self.snapshots_written += 1

    # ------------------------------------------------------------ helius

    async def poll_holders(self) -> None:
        while not self.stop_event.is_set():
            started = time.monotonic()
            mints = list(self.active)
            log.info("holder poll started for %d tokens", len(mints))

            for mint in mints:
                if self.stop_event.is_set():
                    break
                try:
                    await self.update_holders(mint)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("holder check failed for %s: %s", mint, exc)

            elapsed = time.monotonic() - started
            log.info("holder poll finished in %.0fs", elapsed)
            await self._sleep(max(0.0, HOLDER_POLL_SECONDS - elapsed))

    async def update_holders(self, mint: str) -> None:
        """Compute the top-10 non-LP holder percentage for one mint."""
        largest = await self.rpc("getTokenLargestAccounts", [mint])
        accounts = (largest or {}).get("value") or []
        addresses = [
            account.get("address")
            for account in accounts
            if account.get("address")
        ][:20]
        if not addresses:
            return

        parsed = await self.rpc(
            "getMultipleAccounts", [addresses, {"encoding": "jsonParsed"}]
        )
        account_values = (parsed or {}).get("value") or []

        holders: list[tuple[str, float]] = []
        for account in account_values:
            if not account:
                continue
            try:
                info = account["data"]["parsed"]["info"]
                authority = info["owner"]
                amount = float(info["tokenAmount"]["uiAmount"] or 0)
            except (KeyError, TypeError, ValueError):
                continue
            holders.append((authority, amount))

        if not holders:
            return

        authorities = list({authority for authority, _ in holders})
        owners = await self.rpc(
            "getMultipleAccounts", [authorities, {"encoding": "jsonParsed"}]
        )
        owner_values = (owners or {}).get("value") or []

        # A normal wallet account is owned by the System Program. Pool and
        # vault authorities are PDAs owned by their DEX program, so excluding
        # non-system accounts removes most LP token accounts. Raydium v4's
        # shared authority is system-owned and excluded by address instead.
        authority_is_wallet: dict[str, bool] = {}
        for authority, account in zip(authorities, owner_values):
            owned_by = account.get("owner") if account else None
            authority_is_wallet[authority] = (
                owned_by == SYSTEM_PROGRAM
                and authority not in KNOWN_LP_AUTHORITIES
            )

        non_lp = [
            (authority, amount)
            for authority, amount in holders
            if authority_is_wallet.get(authority)
        ]
        non_lp.sort(key=lambda item: item[1], reverse=True)
        top_sum = sum(amount for _, amount in non_lp[:10])

        supply = supply_ui(await self.rpc("getTokenSupply", [mint]))
        if not supply or supply <= 0:
            return

        percent = max(0.0, min(100.0, top_sum / supply * 100.0))
        percent = round(percent, 4)
        self.holder_pct[mint] = percent
        self.db.set_holder_percent(mint, percent, utc_now())

    async def rpc(self, method: str, params: list):
        if not self.helius_url:
            raise RuntimeError("Helius endpoint is not configured")

        self.rpc_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self.rpc_id,
            "method": method,
            "params": params,
        }
        last_error: Exception | None = None

        for attempt in range(1, HELIUS_RPC_ATTEMPTS + 1):
            await self.rpc_limiter.wait()
            try:
                async with self.session.post(
                    self.helius_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status == 429:
                        last_error = RuntimeError("rate limited")
                        log.warning(
                            "Helius rate limited on %s; backing off", method
                        )
                        await asyncio.sleep(2.0 * attempt)
                        continue
                    if response.status != 200:
                        last_error = RuntimeError(f"HTTP {response.status}")
                        await asyncio.sleep(1.0 * attempt)
                        continue
                    data = await response.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                log.warning(
                    "Helius %s request failed (attempt %d): %s",
                    method,
                    attempt,
                    exc,
                )
                await asyncio.sleep(1.0 * attempt)
                continue

            if data.get("error"):
                raise RuntimeError(f"{method} RPC error: {data['error']}")
            return data.get("result")

        raise RuntimeError(f"Helius {method} failed: {last_error}")

    # ------------------------------------------------------------ reporting

    async def report_stats(self) -> None:
        while not self.stop_event.is_set():
            await self._sleep(300)
            if self.stop_event.is_set():
                break
            tokens, snapshots = self.db.counts()
            log.info(
                "status: watching=%d discovered=%d dropped=%d security=%d/%d "
                "snapshots=%d db_tokens=%d db_snapshots=%d",
                len(self.active),
                self.discovered,
                self.dropped,
                self.security_ok,
                self.security_ok + self.security_failed,
                self.snapshots_written,
                tokens,
                snapshots,
            )


# ---------------------------------------------------------------- entrypoint

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Harvest Solana memecoin data into a local SQLite database."
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=DEFAULT_RUN_HOURS,
        help=f"run length in hours (default: {DEFAULT_RUN_HOURS:g})",
    )
    parser.add_argument(
        "--db",
        default=DB_FILENAME,
        help=f"SQLite database path (default: {DB_FILENAME})",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=DEFAULT_MAX_ACTIVE,
        help=f"maximum tokens watched at once (default: {DEFAULT_MAX_ACTIVE})",
    )
    return parser.parse_args()


async def async_main(args: argparse.Namespace, db: Database, helius_key: str) -> None:
    endpoint = helius_endpoint(helius_key) if helius_key else None
    headers = {"User-Agent": "xynetic-sol-harvester/1.0"}

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60), headers=headers
    ) as session:
        harvester = Harvester(
            db=db,
            session=session,
            helius_url=endpoint,
            max_active=args.max_tokens,
            run_seconds=args.hours * 3600,
        )
        log.info(
            "starting %.2fh harvest -> %s (max %d active tokens, Helius %s)",
            args.hours,
            args.db,
            args.max_tokens,
            "enabled" if endpoint else "disabled",
        )
        await harvester.run()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    args = parse_args()
    load_env()
    helius_key = os.environ.get("HELIUS_API_KEY", "").strip()

    db = Database(args.db)
    try:
        asyncio.run(async_main(args, db, helius_key))
    except KeyboardInterrupt:
        log.info("interrupted by user")
    finally:
        tokens, snapshots = db.counts()
        log.info(
            "database %s holds %d tokens and %d snapshots",
            args.db,
            tokens,
            snapshots,
        )
        db.close()


if __name__ == "__main__":
    main()
