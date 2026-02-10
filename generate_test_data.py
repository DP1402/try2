#!/usr/bin/env python3
"""Generate realistic synthetic Telegram messages for testing the pipeline.
Uses Claude to produce messages that look like real Ukrainian strike reports."""

import json
import os
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
import anthropic

import config

load_dotenv()

CHANNELS = config.CHANNELS
START = config.START_DATE
END = config.END_DATE

PROMPT = """\
Generate realistic Telegram channel messages (in Russian and Ukrainian) \
reporting on Ukrainian strikes on Russian territory for the period {start} to {end}.

For each channel below, generate {per_channel} messages that reflect its typical style:
- @astrapress: concise, factual news updates
- @Crimeanwind: focus on Crimea events, local perspective
- @Tsaplienko: longer analysis pieces, sometimes emotional language
- @oper_ZSU: military-focused, technical details about weapons used
- @supernova_plus: mixed format, sometimes reposts with commentary
- @exilenova_plus: Ukrainian language mostly, focus on damage assessment

Mix of content should include:
1. ~40% genuine strike reports on Russian targets (oil refineries, fuel depots, \
military bases, airfields, power infrastructure, radar, ammo depots, maritime targets)
2. ~20% Russian strikes on Ukrainian cities (should be FILTERED OUT by pipeline)
3. ~15% generic military summaries ("X drones shot down") with no specific target
4. ~10% non-strike news (weather, politics, civilian events) that mention military keywords
5. ~10% follow-up/aftermath reports about ongoing fires from previous strikes
6. ~5% cross-channel duplicates (same event reported by 2-3 channels with different wording)

Real locations to use for strikes on Russia:
- Krasnodar Krai (Ilsky refinery, Slavyansk-on-Kuban fuel depot)
- Crimea (Dzhankoy, Saki airfield, Sevastopol naval base, Kerch)
- Belgorod Oblast (fuel depots, radar stations)
- Bryansk Oblast (ammunition depots)
- Kursk Oblast (power infrastructure, military bases)
- Saratov Oblast (oil refinery)
- Rostov Oblast (fuel storage)
- Voronezh Oblast (airfield)

For Russian strikes on Ukraine (to be filtered), use: Kharkiv, Kyiv, Odesa, Dnipro, Sumy.

Output: a JSON object where keys are channel names and values are arrays of message objects:
{{
  "channel_name": [
    {{"text": "message text in Russian or Ukrainian", "date": "2026-02-03T14:32:00", "hour": 14}},
    ...
  ]
}}

CRITICAL: Messages must be in Russian or Ukrainian (not English). Make them realistic \
with typical Telegram formatting, occasional emoji used by real channels, and varying lengths.
Return ONLY valid JSON, no other text.\
"""


def generate_messages():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY not set")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    os.makedirs(config.RAW_DIR, exist_ok=True)

    per_channel = 60  # ~60 messages per channel for 7 days ≈ 8-9/day

    print(f"Generating synthetic messages for {START.date()} to {END.date()}...")
    print(f"  {len(CHANNELS)} channels × ~{per_channel} messages each")

    prompt = PROMPT.format(
        start=START.strftime("%Y-%m-%d"),
        end=END.strftime("%Y-%m-%d"),
        per_channel=per_channel,
    )

    response = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=16384,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"Failed to parse response: {e}")
        print(f"Response start: {text[:500]}")
        # Save raw response for debugging
        with open(os.path.join(config.DATA_DIR, "raw_generation.txt"), "w") as f:
            f.write(text)
        sys.exit(1)

    total = 0
    for channel in CHANNELS:
        messages = data.get(channel, [])
        if not messages:
            print(f"  Warning: no messages for @{channel}")
            continue

        filepath = os.path.join(config.RAW_DIR, f"{channel}.jsonl")
        with open(filepath, "w", encoding="utf-8") as f:
            for i, msg in enumerate(messages):
                # Construct realistic message record
                msg_text = msg.get("text", "")
                msg_date = msg.get("date", "")

                # If no date given, assign one spread across the week
                if not msg_date:
                    day_offset = i % 7
                    hour = 6 + (i * 3) % 18
                    dt = START + timedelta(days=day_offset, hours=hour)
                    msg_date = dt.isoformat()

                record = {
                    "message_id": (CHANNELS.index(channel) + 1) * 10000 + i + 1,
                    "date": msg_date,
                    "text": msg_text,
                    "channel": channel,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                total += 1

        print(f"  @{channel}: {len(messages)} messages → {filepath}")

    print(f"\nTotal: {total} messages generated")
    return total


if __name__ == "__main__":
    generate_messages()
