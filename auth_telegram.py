"""Run this once to authenticate your Telegram session. Then main.py works non-interactively."""
import asyncio
from dotenv import load_dotenv
import os
from telethon import TelegramClient

load_dotenv()

async def auth():
    client = TelegramClient(
        "telegram_session",
        int(os.getenv("TELEGRAM_API_ID")),
        os.getenv("TELEGRAM_API_HASH"),
    )
    await client.start()
    print("Authorized! Session saved. You can now run main.py.")
    await client.disconnect()

asyncio.run(auth())
