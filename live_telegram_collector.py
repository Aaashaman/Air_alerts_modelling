import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import aiosqlite
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError

# Force UTF-8 encoding on Windows console output to prevent Cyrillic encoding crashes
if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Load environment variables from .env
load_dotenv()

API_ID = int(os.getenv("TG_API_ID", "0"))
API_HASH = os.getenv("TG_API_HASH", "")
SESSION_NAME = "air_raid_collector_session"
DB_PATH = str(Path(__file__).parent / "telegram_intel.db")

TARGET_CHANNELS = [
    "kpszsu",           # Повітряні Сили ЗС України
    "eRadarrua",        # єРадар
    "air_alert_ua",     # Повітряна Тривога
    "kyiv_golovne"      # Київ Головне
]

logging.basicConfig(
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("collector.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("Collector")


# ---------------------------------------------------------------------------
# DATABASE INITIALIZATION
# ---------------------------------------------------------------------------
async def init_db(db_path: str):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                channel_id INTEGER NOT NULL,
                channel_username TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                timestamp_utc TEXT NOT NULL,
                received_at_utc TEXT NOT NULL,
                text TEXT,
                is_edit INTEGER DEFAULT 0,
                views INTEGER,
                forwards INTEGER,
                PRIMARY KEY (channel_id, message_id)
            );
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_timestamp 
            ON messages (timestamp_utc);
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_channel 
            ON messages (channel_username, timestamp_utc);
        """)
        await db.commit()
    logger.info("SQLite database initialized in WAL mode.")


# ---------------------------------------------------------------------------
# MESSAGE STORAGE
# ---------------------------------------------------------------------------
async def save_message(db_path: str, msg_data: dict):
    query = """
        INSERT INTO messages (
            channel_id, channel_username, message_id, 
            timestamp_utc, received_at_utc, text, 
            is_edit, views, forwards
        ) VALUES (
            :channel_id, :channel_username, :message_id, 
            :timestamp_utc, :received_at_utc, :text, 
            :is_edit, :views, :forwards
        )
        ON CONFLICT(channel_id, message_id) DO UPDATE SET
            text = excluded.text,
            is_edit = 1,
            views = excluded.views,
            forwards = excluded.forwards;
    """
    async with aiosqlite.connect(db_path) as db:
        await db.execute(query, msg_data)
        await db.commit()


# ---------------------------------------------------------------------------
# CATCH-UP SYNC LOOP
# ---------------------------------------------------------------------------
async def sync_channel_history(client: TelegramClient, entity, username: str, db_path: str):
    channel_id = entity.id
    
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            "SELECT MAX(message_id) FROM messages WHERE channel_id = ?", 
            (channel_id,)
        )
        row = await cursor.fetchone()
        last_id = row[0] if row and row[0] else 0

    logger.info(f"Synchronizing @{username} starting from message ID {last_id}...")
    
    count = 0
    try:
        async for msg in client.iter_messages(entity, min_id=last_id, reverse=True):
            if not msg.text:
                continue
                
            msg_utc = msg.date.astimezone(timezone.utc).isoformat()
            now_utc = datetime.now(timezone.utc).isoformat()
            
            await save_message(db_path, {
                "channel_id": channel_id,
                "channel_username": username,
                "message_id": msg.id,
                "timestamp_utc": msg_utc,
                "received_at_utc": now_utc,
                "text": msg.text,
                "is_edit": 0 if not msg.edit_date else 1,
                "views": msg.views or 0,
                "forwards": msg.forwards or 0
            })
            count += 1
            
        logger.info(f"Backfill complete: @{username} fetched {count} missed records.")
    except FloodWaitError as e:
        logger.warning(f"FloodWait hit: sleeping for {e.seconds}s on @{username}.")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.error(f"Sync error on @{username}: {e}")


# ---------------------------------------------------------------------------
# MAIN COLLECTOR ROUTINE
# ---------------------------------------------------------------------------
async def main():
    if not API_ID or not API_HASH:
        raise ValueError("TG_API_ID and TG_API_HASH must be configured in your .env file.")

    await init_db(DB_PATH)

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    logger.info("Telethon client authenticated and active.")

    entities = {}
    for username in TARGET_CHANNELS:
        try:
            entity = await client.get_entity(username)
            entities[entity.id] = (entity, username)
            logger.info(f"Resolved channel: @{username} (ID: {entity.id})")
        except Exception as e:
            logger.error(f"Failed to resolve @{username}: {e}")

    # Backfill missed messages during offline gaps
    for channel_id, (entity, username) in entities.items():
        await sync_channel_history(client, entity, username, DB_PATH)

    channel_ids = list(entities.keys())

    @client.on(events.NewMessage(chats=channel_ids))
    async def handler_new_message(event):
        msg = event.message
        if not msg.text:
            return

        channel_id = msg.peer_id.channel_id
        _, username = entities.get(channel_id, (None, "unknown"))

        msg_utc = msg.date.astimezone(timezone.utc).isoformat()
        now_utc = datetime.now(timezone.utc).isoformat()

        data = {
            "channel_id": channel_id,
            "channel_username": username,
            "message_id": msg.id,
            "timestamp_utc": msg_utc,
            "received_at_utc": now_utc,
            "text": msg.text,
            "is_edit": 0,
            "views": msg.views or 0,
            "forwards": msg.forwards or 0
        }

        try:
            await save_message(DB_PATH, data)
            logger.info(f"[@{username} | ID {msg.id}] Captured live.")
        except Exception as e:
            logger.error(f"Error persisting message {msg.id}: {e}")

    logger.info("Collector listening for live updates. Press Ctrl+C to terminate.")
    
    try:
        await client.run_until_disconnected()
    finally:
        await client.disconnect()
        logger.info("Client cleanly disconnected.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process terminated by user (Ctrl+C). Exiting cleanly.")