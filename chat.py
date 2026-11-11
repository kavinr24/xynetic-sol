#!/usr/bin/env python3
"""Minimal streaming OpenRouter chat REPL using NVIDIA Nemotron — for local testing.

Setup:
    cp .env.example .env      # then put your key in OPENROUTER_API_KEY
    pip install requests
    python chat.py

Reasoning and the answer both stream to the terminal as they are generated.
Commands:
    /clear   forget the conversation
    /exit    quit (also /quit)
"""

import json
import os
import sys
from pathlib import Path

import requests

API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "nvidia/nemotron-3-ultra-550b-a55b:free"

# Dim reasoning text only when writing to a real terminal.
COLOR = sys.stdout.isatty()
DIM = "\033[2m" if COLOR else ""
RESET = "\033[0m" if COLOR else ""


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


def ask_stream(messages: list[dict]) -> dict:
    """Stream one assistant turn, printing tokens live. Returns the history entry."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        sys.exit("OPENROUTER_API_KEY is not set. Put it in .env or export it.")

    content_parts: list[str] = []
    reasoning_details: dict[int, dict] = {}
    reasoning_open = False
    answer_open = False

    response = requests.post(
        API_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        data=json.dumps(
            {
                "model": MODEL,
                "messages": messages,
                "reasoning": {"enabled": True},
                "stream": True,
            }
        ),
        stream=True,
        timeout=(15, 300),  # (connect, read between chunks)
    )

    with response:
        if response.status_code != 200:
            raise SystemExit(f"HTTP {response.status_code}: {response.text}")

        for raw_line in response.iter_lines(decode_unicode=True):
            # Blank lines separate events; ": OPENROUTER PROCESSING" are keep-alives.
            if not raw_line or not raw_line.startswith("data:"):
                continue

            payload = raw_line[5:].strip()
            if payload == "[DONE]":
                break

            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError:
                continue

            if chunk.get("error"):
                if reasoning_open:
                    sys.stdout.write(RESET + "\n")
                raise SystemExit(f"stream error: {chunk['error']}")

            for choice in chunk.get("choices") or []:
                delta = choice.get("delta") or {}

                if delta.get("reasoning"):
                    if not reasoning_open:
                        sys.stdout.write(f"{DIM}[reasoning] ")
                        reasoning_open = True
                    sys.stdout.write(delta["reasoning"])
                    sys.stdout.flush()

                if delta.get("content"):
                    if reasoning_open:
                        sys.stdout.write(f"{RESET}\n\n")
                        reasoning_open = False
                    if not answer_open:
                        sys.stdout.write("nemotron> ")
                        answer_open = True
                    content_parts.append(delta["content"])
                    sys.stdout.write(delta["content"])
                    sys.stdout.flush()

                # Reasoning arrives in fragments; merge them by index so the
                # full details can be passed back on the next turn.
                for detail in delta.get("reasoning_details") or []:
                    index = detail.get("index", len(reasoning_details))
                    merged = reasoning_details.get(index, {})
                    merged.update(detail)
                    reasoning_details[index] = merged

    if reasoning_open:
        sys.stdout.write(RESET + "\n")
    if answer_open:
        print("\n")
    else:
        print("\n(no response)")

    # Keep the assistant turn intact so the model can continue its reasoning.
    entry = {"role": "assistant", "content": "".join(content_parts)}
    if reasoning_details:
        entry["reasoning_details"] = [reasoning_details[i] for i in sorted(reasoning_details)]
    return entry


def main() -> None:
    load_env()

    print(f"Nemotron chat using {MODEL} (streaming)")
    print("Commands: /clear to reset history, /exit to quit.\n")

    messages: list[dict] = []

    while True:
        try:
            user_input = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not user_input:
            continue
        if user_input in ("/exit", "/quit"):
            break
        if user_input == "/clear":
            messages.clear()
            print("(history cleared)\n")
            continue

        messages.append({"role": "user", "content": user_input})

        try:
            history_entry = ask_stream(messages)
        except requests.RequestException as exc:
            print(f"network error: {exc}\n")
            messages.pop()  # don't keep a turn that never got an answer
            continue
        except KeyboardInterrupt:
            print("\n(interrupted)\n")
            messages.pop()
            continue

        messages.append(history_entry)


if __name__ == "__main__":
    main()
