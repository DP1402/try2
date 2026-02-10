import asyncio
import json
import os
from datetime import timezone

from telethon import TelegramClient

import config


def _get_last_seen_id(filepath: str) -> int:
    """Read the last message ID from an existing JSONL file."""
    last_id = 0
    if not os.path.exists(filepath):
        return last_id
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                last_id = max(last_id, msg.get("message_id", 0))
            except json.JSONDecodeError:
                continue
    return last_id


async def scrape_channels(api_id: int, api_hash: str):
    """Scrape all configured channels and save to JSONL files."""
    os.makedirs(config.RAW_DIR, exist_ok=True)

    client = TelegramClient("telegram_session", api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: Telegram session not authorized. Run manually once to log in.")
        return
    print("Telegram client connected.")

    for channel in config.CHANNELS:
        filepath = os.path.join(config.RAW_DIR, f"{channel}.jsonl")
        last_seen_id = _get_last_seen_id(filepath)

        print(f"\nScraping @{channel} (messages after ID {last_seen_id})...")

        count = 0
        start_utc = config.START_DATE.replace(tzinfo=timezone.utc)
        end_utc = config.END_DATE.replace(tzinfo=timezone.utc)

        with open(filepath, "a", encoding="utf-8") as f:
            # Use offset_date=END_DATE so Telegram gives us messages
            # starting from that date going backwards (much faster)
            async for message in client.iter_messages(
                channel,
                offset_date=end_utc,
                min_id=last_seen_id,
            ):
                msg_date = message.date.replace(tzinfo=timezone.utc)
                # Stop once we've gone past our start date
                if msg_date < start_utc:
                    break

                # Only keep text messages
                text = message.text or message.message or ""
                if not text.strip():
                    continue

                record = {
                    "message_id": message.id,
                    "date": message.date.isoformat(),
                    "text": text,
                    "channel": channel,
                }
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1

                if count % 1000 == 0:
                    print(f"  ...{count} messages saved from @{channel}")

        print(f"  Done: {count} new messages from @{channel}")

    await client.disconnect()
    print("\nScraping complete.")


def run(api_id: int, api_hash: str):
    asyncio.run(scrape_channels(api_id, api_hash))
