#!/usr/bin/env python3
"""Basic Solana memecoin safety screen using public RugCheck data.

Usage:
    python safety.py <mint address>

Heuristic screen only - this checks a few basic, publicly available metrics.
It is not a rugpull validator and not financial advice.
"""

import re
import sys
from datetime import datetime, timezone

import requests

API_BASE = "https://api.rugcheck.xyz"
TIMEOUT = 30

# Thresholds for the basic screen (tune after testing against more tokens).
MIN_LIQUIDITY_DANGER = 5_000    # USD
MIN_LIQUIDITY_WARN = 25_000
TOP10_DANGER = 80.0             # % of supply held by top 10 accounts
TOP10_WARN = 50.0
MIN_LP_LOCKED = 50.0            # % of liquidity locked
SCORE_DANGER = 60               # RugCheck normalised score, 0-100
SCORE_WARN = 30
TRANSFER_FEE_DANGER = 5.0       # %

MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


# ---------------------------------------------------------------- helpers

def short(address, chars: int = 5) -> str:
    if not address:
        return "none"
    return f"{address[:chars]}...{address[-chars:]}"


def fmt_usd(value) -> str:
    if value is None:
        return "n/a"
    if value >= 1_000_000:
        return f"${value / 1_000_000:,.2f}M"
    if value >= 1_000:
        return f"${value:,.0f}"
    return f"${value:,.2f}"


def fmt_pct(value, digits: int = 1) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}%"


def age_days(timestamp):
    if not timestamp:
        return None
    try:
        then = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - then).days


# ---------------------------------------------------------------- fetching

def fetch_report(mint: str) -> dict:
    """Full report: authorities, holders, markets, risks, score."""
    try:
        response = requests.get(f"{API_BASE}/v1/tokens/{mint}/report", timeout=TIMEOUT)
    except requests.RequestException as exc:
        sys.exit(f"network error: {exc}")

    if response.status_code == 429:
        sys.exit("RugCheck rate limit hit - wait a second and try again.")
    if response.status_code in (400, 404):
        sys.exit(
            f"No RugCheck report for {mint} (HTTP {response.status_code}). "
            "It may be an invalid or brand-new mint."
        )
    if response.status_code != 200:
        sys.exit(f"RugCheck returned HTTP {response.status_code}.")

    return response.json()


def fetch_summary(mint: str) -> dict:
    """Small summary; used because it carries the aggregate LP lock percentage."""
    try:
        response = requests.get(
            f"{API_BASE}/v1/tokens/{mint}/report/summary", timeout=TIMEOUT
        )
        if response.status_code == 200:
            return response.json()
    except requests.RequestException:
        pass
    return {}


def lp_locked_from_markets(report: dict):
    """Fallback: liquidity-weighted average of per-market LP lock values."""
    weighted = []
    for market in report.get("markets") or []:
        lp = market.get("lp") or {}
        pct = lp.get("lpLockedPct", market.get("lpLockedPct"))
        if pct is None:
            continue
        usd = float(lp.get("baseUSD") or 0) + float(lp.get("quoteUSD") or 0)
        weighted.append((float(pct), max(usd, 0.0)))

    if not weighted:
        return None
    total = sum(weight for _, weight in weighted)
    if total > 0:
        return sum(pct * weight for pct, weight in weighted) / total
    return sum(pct for pct, _ in weighted) / len(weighted)


# ---------------------------------------------------------------- checking

def check_token(mint: str) -> dict:
    report = fetch_report(mint)
    summary = fetch_summary(mint)

    token = report.get("token") or {}
    meta = report.get("tokenMeta") or {}
    holders = report.get("topHolders") or []
    risks = report.get("risks") or []

    mint_authority = token.get("mintAuthority") or report.get("mintAuthority")
    freeze_authority = token.get("freezeAuthority") or report.get("freezeAuthority")

    top10 = round(sum(float(h.get("pct") or 0) for h in holders[:10]), 2)
    largest = round(max((float(h.get("pct") or 0) for h in holders), default=0.0), 2)
    insiders = sum(1 for h in holders if h.get("insider"))

    liquidity = report.get("totalMarketLiquidity")
    lp_locked = summary.get("lpLockedPct")
    if lp_locked is None:
        lp_locked = lp_locked_from_markets(report)

    transfer_fee = float((report.get("transferFee") or {}).get("pct") or 0)
    creator_tokens = report.get("creatorTokens") or []

    metrics = {
        "name": meta.get("name") or "",
        "symbol": meta.get("symbol") or "",
        "score": report.get("score_normalised"),
        "risks": risks,
        "liquidity_usd": liquidity,
        "lp_locked_pct": lp_locked,
        "market_count": len(report.get("markets") or []),
        "total_holders": report.get("totalHolders"),
        "top10_pct": top10,
        "largest_pct": largest,
        "insider_holders": insiders,
        "mint_authority": mint_authority,
        "freeze_authority": freeze_authority,
        "metadata_mutable": bool(meta.get("mutable")),
        "transfer_fee_pct": transfer_fee,
        "price_usd": report.get("price"),
        "age_days": age_days(report.get("detectedAt")),
        "creator": report.get("creator"),
        "creator_token_count": len(creator_tokens),
        "jup_verified": bool((report.get("verification") or {}).get("jup_verified")),
        "rugged": bool(report.get("rugged")),
    }

    findings = []

    def add(level: str, text: str) -> None:
        findings.append((level, text))

    if metrics["rugged"]:
        add("danger", "RugCheck marks this token as previously rugged")
    if mint_authority:
        add("danger", f"Mint authority is active ({short(mint_authority)}) - supply can be inflated")
    if freeze_authority:
        add("danger", "Freeze authority is active - holder accounts can be frozen")

    if liquidity is None or liquidity <= 0:
        add("danger", "No active liquidity market found")
    elif liquidity < MIN_LIQUIDITY_DANGER:
        add("danger", f"Very low liquidity ({fmt_usd(liquidity)})")
    elif liquidity < MIN_LIQUIDITY_WARN:
        add("warn", f"Low liquidity ({fmt_usd(liquidity)})")

    if top10 >= TOP10_DANGER:
        add("danger", f"Top 10 holders control {fmt_pct(top10)} of supply")
    elif top10 >= TOP10_WARN:
        add("warn", f"Top 10 holders control {fmt_pct(top10)} of supply")

    if lp_locked is None:
        add("warn", "LP lock status is unknown")
    elif lp_locked < MIN_LP_LOCKED:
        add("warn", f"Only {fmt_pct(lp_locked)} of liquidity is locked")

    if metrics["metadata_mutable"]:
        add("warn", "Token metadata is mutable")

    if transfer_fee >= TRANSFER_FEE_DANGER:
        add("danger", f"High transfer fee ({fmt_pct(transfer_fee)})")
    elif transfer_fee > 0:
        add("warn", f"Transfer fee of {fmt_pct(transfer_fee)}")

    if insiders:
        add("warn", f"{insiders} insider account(s) among top holders")
    if metrics["creator_token_count"]:
        add("warn", f"Creator has deployed {metrics['creator_token_count']} other token(s)")

    score = metrics["score"]
    if score is not None:
        if score >= SCORE_DANGER:
            add("danger", f"RugCheck risk score is high ({score}/100)")
        elif score >= SCORE_WARN:
            add("warn", f"RugCheck risk score is elevated ({score}/100)")

    findings.sort(key=lambda item: item[0] != "danger")  # dangers first
    levels = {level for level, _ in findings}
    if "danger" in levels:
        verdict = "HIGH RISK"
    elif "warn" in levels:
        verdict = "CAUTION"
    else:
        verdict = "LOW RISK"

    return {"mint": mint, "metrics": metrics, "findings": findings, "verdict": verdict}


# ---------------------------------------------------------------- output

def print_report(result: dict) -> None:
    m = result["metrics"]
    label = m["name"] or "Unknown token"
    if m["symbol"]:
        label += f" ({m['symbol']})"

    print()
    print(f"{label}  -  {short(result['mint'], 6)}")
    print(f"VERDICT: {result['verdict']}")
    print()

    markets = f" across {m['market_count']:,} markets" if m["market_count"] else ""
    print(f"  RugCheck score   {('n/a' if m['score'] is None else str(m['score']) + '/100')}"
          f"  ({len(m['risks'])} flagged risk(s))")
    print(f"  Liquidity        {fmt_usd(m['liquidity_usd'])}{markets}, "
          f"{fmt_pct(m['lp_locked_pct'])} LP locked")

    holders = f"top 10 = {fmt_pct(m['top10_pct'])}, largest = {fmt_pct(m['largest_pct'])}"
    if m["total_holders"]:
        holders = f"{m['total_holders']:,} total; " + holders
    print(f"  Holders          {holders}")

    mint_auth = "ACTIVE" if m["mint_authority"] else "revoked"
    freeze_auth = "ACTIVE" if m["freeze_authority"] else "revoked"
    print(f"  Authorities      mint: {mint_auth}, freeze: {freeze_auth}")

    metadata = "mutable" if m["metadata_mutable"] else "immutable"
    print(f"  Metadata         {metadata}; transfer fee {fmt_pct(m['transfer_fee_pct'])}")

    if m["age_days"] is not None:
        print(f"  Age              ~{m['age_days']:,} days")
    print(f"  Creator          {short(m['creator'])}"
          + (f" ({m['creator_token_count']} other token(s))" if m["creator_token_count"] else ""))
    if m["price_usd"]:
        print(f"  Price            ${m['price_usd']:.10g}")
    if m["jup_verified"]:
        print("  Verification     Jupiter verified")

    if m["risks"]:
        print("\n  RugCheck risks")
        for risk in m["risks"]:
            value = f" ({risk['value']})" if risk.get("value") else ""
            print(f"    [{risk.get('level', '?')}] {risk.get('name')}: "
                  f"{risk.get('description')}{value}")

    print("\n  What the screen noticed")
    if result["findings"]:
        for level, text in result["findings"]:
            print(f"    [{level}] {text}")
    else:
        print("    nothing flagged by these basic checks")

    print("\n  Heuristic screen from public RugCheck data - not a rugpull validator,")
    print("  and not financial advice. Always verify independently.")
    print()


# ---------------------------------------------------------------- entry

def main() -> None:
    mint = sys.argv[1] if len(sys.argv) > 1 else input("mint> ").strip()

    if not MINT_RE.match(mint):
        sys.exit(
            "That doesn't look like a Solana mint address "
            "(expected 32-44 base58 characters)."
        )

    print_report(check_token(mint))


if __name__ == "__main__":
    main()
