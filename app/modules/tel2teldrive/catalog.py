"""Persistent catalogue and opt-in synchronization for readable channels.

Watching alone only stores metadata. Explicit selections or configured automatic
sync hand videos to the shared relay queue; neither path deletes source messages.
"""

import asyncio
import inspect
import mimetypes
import re
import time
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import aiosqlite
from telethon import utils
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, DocumentAttributeFilename, DocumentAttributeVideo

from app import database as db


SCAN_BATCH_SIZE = 100
POLL_INTERVAL = 60
MAX_CHANNELS = 100
TELEGRAM_TIMEOUT = 45
AUTO_BATCH_SIZE = 20
MAX_ACTIVE_TRANSFERS = 100
AUTO_MAX_ATTEMPTS = 3


def normalize_target_path(value):
    """Keep the cloud-path rules of config.normalize_teldrive_target_path.

    Do not import runtime configuration here: standalone catalog inspection must
    not load or rewrite the application's settings file.
    """
    if not isinstance(value, str):
        raise CatalogError(400, "TelDrive 目标目录必须是路径文本")
    text = value.strip()
    if not text:
        return ""
    parts = [part for part in text.split("/") if part]
    if (len(text) > 2048 or "://" in text or (len(text) > 1 and text[0].isalpha() and text[1] == ":")
            or "\\" in text or any(ord(char) < 32 or ord(char) == 127 for char in text)
            or any(part in (".", "..") for part in parts)):
        raise CatalogError(400, "TelDrive 目标目录无效，请使用 / 开头的云端目录路径")
    return "/" + "/".join(parts)


def normalize_sync_since(value):
    if not isinstance(value, str):
        raise CatalogError(400, "同步起点必须是包含时区的 ISO 时间")
    if not value.strip():
        return "", None
    try:
        date = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if date.tzinfo is None or date.utcoffset() is None:
            raise ValueError("missing timezone")
        date = date.astimezone(timezone.utc)
        return date.isoformat(), date.timestamp()
    except (ValueError, OverflowError):
        raise CatalogError(400, "同步起点必须是包含时区的有效 ISO 时间") from None


def _published_at(value):
    if isinstance(value, str):
        with suppress(ValueError):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


class CatalogError(Exception):
    def __init__(self, status_code: int, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.retry_after = retry_after


def _iso(timestamp: float | None = None) -> str:
    return datetime.fromtimestamp(timestamp if timestamp is not None else time.time(), timezone.utc).isoformat()


def parse_channel_reference(reference: Any) -> str | int:
    value = str(reference or "").strip()
    if len(value) > 512:
        raise CatalogError(400, "频道地址过长")
    if re.fullmatch(r"-100\d+", value):
        return int(value)
    if re.fullmatch(r"@?[A-Za-z][A-Za-z0-9_]{3,31}", value):
        return value.lstrip("@")
    if value.startswith(("t.me/", "telegram.me/")):
        value = "https://" + value
    parsed = urlparse(value)
    if parsed.scheme in {"http", "https"} and parsed.netloc.lower() in {"t.me", "telegram.me", "www.t.me"}:
        parts = parsed.path.strip("/").split("/")
        if parts and (parts[0] == "joinchat" or parts[0].startswith("+")):
            raise CatalogError(400, "请先在 Telegram 中加入频道，再填写频道用户名或频道 ID；不会自动加入邀请链接")
        if len(parts) in {2, 3} and parts[0] == "c" and parts[1].isdigit():
            if len(parts) == 2 or parts[2].isdigit():
                return -1000000000000 - int(parts[1])
        if parts and parts[0] == "s":
            parts = parts[1:]
        if len(parts) in {1, 2} and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", parts[0]):
            if len(parts) == 1 or parts[1].isdigit():
                return parts[0]
    raise CatalogError(400, "请填写 @频道用户名、t.me 频道链接或 -100 开头的频道 ID")


def video_file_info(message: Any) -> dict | None:
    """Only ordinary, fully accessible document videos can be queued."""
    media = getattr(message, "media", None)
    if media is None or getattr(media, "ttl_seconds", None) or getattr(message, "ttl_period", None):
        return None
    if "PaidMedia" in type(media).__name__ or getattr(media, "extended_media", None):
        return None
    document = getattr(message, "document", None) or getattr(media, "document", None)
    if document is None:
        return None
    attributes = getattr(document, "attributes", []) or []
    video_attribute = next((a for a in attributes if isinstance(a, DocumentAttributeVideo)), None)
    mime_type = str(getattr(document, "mime_type", "") or "")
    if not (getattr(message, "video", None) or mime_type.lower().startswith("video/") or video_attribute):
        return None
    size = int(getattr(document, "size", 0) or 0)
    if size <= 0:
        return None
    name = next((str(a.file_name) for a in attributes if isinstance(a, DocumentAttributeFilename) and a.file_name), "")
    # Filenames are untrusted metadata. The relay also sanitizes disk paths.
    name = name.replace("\\", "/").rsplit("/", 1)[-1].replace("\x00", "").strip()
    if name in {"", ".", ".."}:
        name = f"video_{int(message.id)}{mimetypes.guess_extension(mime_type) or '.mp4'}"
    return {"name": name[:512], "size": size, "mime_type": mime_type[:128],
            "duration": max(0, float(getattr(video_attribute, "duration", 0) or 0))}


class TelegramVideoCatalog:
    def __init__(self, client_getter, config_getter, relay_manager, logger, broker):
        self.client_getter = client_getter
        self.config_getter = config_getter
        self.relay_manager = relay_manager
        self.logger = logger
        self.broker = broker
        self._conn = None
        self._db_lock = asyncio.Lock()
        self._channel_locks: dict[int, asyncio.Lock] = {}
        self._add_lock = asyncio.Lock()
        self._poll_task = None
        self._cooldown_until = 0.0
        self._lifecycle_epoch = 0
        self._auto_lock = asyncio.Lock()
        self._poll_offset = 0

    @asynccontextmanager
    async def _database(self):
        # A separate connection prevents other modules from committing our
        # cursor update before all metadata in its batch has been stored.
        async with self._db_lock:
            if self._conn is None:
                conn = await aiosqlite.connect(str(db.DB_PATH), isolation_level=None)
                conn.row_factory = aiosqlite.Row
                try:
                    await conn.execute("PRAGMA busy_timeout=10000")
                    await conn.execute("PRAGMA journal_mode=WAL")
                    await conn.execute("""CREATE TABLE IF NOT EXISTS telegram_catalog_channels (
                        channel_id INTEGER PRIMARY KEY, title TEXT NOT NULL, username TEXT NOT NULL DEFAULT '',
                        enabled INTEGER NOT NULL DEFAULT 1, protected INTEGER NOT NULL DEFAULT 0,
                        newest_message_id INTEGER NOT NULL DEFAULT 0, oldest_message_id INTEGER NOT NULL DEFAULT 0,
                        history_complete INTEGER NOT NULL DEFAULT 0, last_scan TEXT, last_error TEXT,
                        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")
                    await conn.execute("""CREATE TABLE IF NOT EXISTS telegram_catalog_videos (
                        channel_id INTEGER NOT NULL, message_id INTEGER NOT NULL, file_name TEXT NOT NULL,
                        file_size INTEGER NOT NULL, mime_type TEXT NOT NULL, duration REAL NOT NULL DEFAULT 0,
                        caption TEXT NOT NULL DEFAULT '', date TEXT, protected INTEGER NOT NULL DEFAULT 0,
                        link TEXT NOT NULL DEFAULT '', PRIMARY KEY(channel_id, message_id))""")
                    await conn.execute("""CREATE TABLE IF NOT EXISTS telegram_catalog_meta (
                        key TEXT PRIMARY KEY, value TEXT NOT NULL)""")
                    # Safe for standalone catalog use; the application's init_db
                    # applies the relay's additional migrations during startup.
                    await conn.execute(db.CREATE_TELEGRAM_RELAY_JOBS_TABLE_SQL)
                    await self._migrate_sync_schema(conn)
                    async with conn.execute("SELECT value FROM telegram_catalog_meta WHERE key='cooldown_until'") as cursor:
                        row = await cursor.fetchone()
                        self._cooldown_until = float(row[0]) if row else 0.0
                except BaseException:
                    await conn.close()
                    raise
                self._conn = conn
            yield self._conn

    async def _migrate_sync_schema(self, conn):
        for table, definitions in {
            "telegram_catalog_channels": {
                "target_path": "TEXT NOT NULL DEFAULT ''", "auto_sync": "INTEGER NOT NULL DEFAULT 0",
                "sync_since": "TEXT NOT NULL DEFAULT ''", "sync_since_ts": "REAL",
                "oldest_scanned_ts": "REAL", "last_sync_error": "TEXT",
                "sync_backpressure": "INTEGER NOT NULL DEFAULT 0",
            },
            "telegram_catalog_videos": {"published_at": "REAL"},
            "telegram_relay_jobs": {"source_kind": "TEXT NOT NULL DEFAULT 'relay'"},
        }.items():
            async with conn.execute(f"PRAGMA table_info({table})") as cursor:
                existing = {row["name"] for row in await cursor.fetchall()}
            for name, definition in definitions.items():
                if name not in existing:
                    await conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
        # Legacy channels remain manual. Populate their displayed default time
        # without opting them in or modifying their existing history cursors.
        # One-time conversion is batched, preserving fractional UTC boundaries.
        async with conn.execute("SELECT value FROM telegram_catalog_meta WHERE key='sync_schema_v1'") as cursor:
            migrated = await cursor.fetchone()
        if not migrated:
            now = time.time()
            await conn.execute("UPDATE telegram_catalog_channels SET sync_since=?,sync_since_ts=? WHERE sync_since='' AND sync_since_ts IS NULL",
                               (_iso(now), now))
            async with conn.execute("SELECT channel_id,message_id,date FROM telegram_catalog_videos WHERE published_at IS NULL") as cursor:
                while rows := await cursor.fetchmany(500):
                    await conn.executemany("UPDATE telegram_catalog_videos SET published_at=? WHERE channel_id=? AND message_id=?",
                        [(_published_at(row["date"]), row["channel_id"], row["message_id"]) for row in rows])
            await conn.execute("INSERT OR REPLACE INTO telegram_catalog_meta(key,value) VALUES ('sync_schema_v1','1')")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_telegram_catalog_video_date ON telegram_catalog_videos(channel_id,published_at,message_id)")
        await conn.execute("""CREATE TABLE IF NOT EXISTS telegram_catalog_dispatches (
            channel_id INTEGER NOT NULL, message_id INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
            job_id TEXT, job_status TEXT, target_path TEXT NOT NULL DEFAULT '', error TEXT,
            attempts INTEGER NOT NULL DEFAULT 0, next_retry_at REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(channel_id,message_id))""")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_telegram_catalog_dispatch_pending ON telegram_catalog_dispatches(channel_id,state,next_retry_at)")
        # Receipt and job creation share the relay's SQLite transaction. A crash
        # after enqueue, or later deleting completed jobs, cannot lose this proof.
        for event, suffix in (("INSERT", "insert"), ("UPDATE OF status,error,target_path", "update")):
            await conn.execute(f"""CREATE TRIGGER IF NOT EXISTS telegram_catalog_job_{suffix}
                AFTER {event} ON telegram_relay_jobs WHEN NEW.source_kind='channel' BEGIN
                INSERT INTO telegram_catalog_dispatches(channel_id,message_id,state,job_id,job_status,target_path,error)
                VALUES(NEW.source_channel_id,NEW.source_message_id,'submitted',NEW.job_id,NEW.status,NEW.target_path,NEW.error)
                ON CONFLICT(channel_id,message_id) DO UPDATE SET state='submitted',job_id=excluded.job_id,
                    job_status=excluded.job_status,target_path=excluded.target_path,error=excluded.error,
                    updated_at=CURRENT_TIMESTAMP; END""")
        await conn.execute("""INSERT OR IGNORE INTO telegram_catalog_dispatches
            (channel_id,message_id,state,job_id,job_status,target_path,error)
            SELECT source_channel_id,source_message_id,'submitted',job_id,status,target_path,error
            FROM telegram_relay_jobs WHERE source_kind='channel'""")

    @staticmethod
    def _pending_statement(channel_id, message_ids=None):
        selection = ""
        params = [int(channel_id)]
        if message_ids is not None:
            selection = " AND v.message_id IN (" + ",".join("?" for _ in message_ids) + ")"
            params.extend(message_ids)
        return ("""INSERT OR IGNORE INTO telegram_catalog_dispatches(channel_id,message_id)
            SELECT v.channel_id,v.message_id FROM telegram_catalog_videos v
            JOIN telegram_catalog_channels c ON c.channel_id=v.channel_id
            WHERE c.channel_id=? AND c.auto_sync=1 AND c.enabled=1 AND c.sync_since_ts IS NOT NULL
              AND v.published_at>=c.sync_since_ts""" + selection, tuple(params))

    async def ensure_schema(self):
        async with self._database():
            pass

    async def _rows(self, query, params=()):
        async with self._database() as conn:
            async with conn.execute(query, params) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    async def _write(self, statements):
        async with self._database() as conn:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                for query, params in statements:
                    await conn.execute(query, params)
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise

    def _lock(self, channel_id):
        return self._channel_locks.setdefault(int(channel_id), asyncio.Lock())

    @staticmethod
    def _channel_dict(row, config=None):
        result = dict(row)
        for field in ("enabled", "protected", "history_complete", "auto_sync"):
            result[field] = bool(result[field])
        result["sync_status"] = (
            "paused" if not result["enabled"] else
            "manual" if not result["auto_sync"] else
            "relay_disabled" if not bool(getattr(config, "relay_enabled", False)) else
            "error" if result.get("last_sync_error") or result.get("last_error") else
            "backpressure" if result.get("sync_backpressure") else
            "backfilling" if TelegramVideoCatalog._needs_history(result) else
            "queued" if result.get("auto_pending_count") or result.get("auto_active_count") else "watching")
        return result

    @staticmethod
    def _needs_history(channel):
        since = channel.get("sync_since_ts")
        oldest = channel.get("oldest_scanned_ts")
        return bool(channel.get("auto_sync") and since is not None and not channel.get("history_complete")
                    and (oldest is None or oldest >= since))

    @staticmethod
    def _channel_select():
        return """SELECT c.*, (SELECT count(*) FROM telegram_catalog_videos v
            WHERE v.channel_id=c.channel_id) AS video_count,
            (SELECT count(*) FROM telegram_catalog_dispatches d JOIN telegram_catalog_videos v
              ON v.channel_id=d.channel_id AND v.message_id=d.message_id
              WHERE d.channel_id=c.channel_id AND d.state='pending' AND v.published_at>=c.sync_since_ts) AS auto_pending_count,
            (SELECT count(*) FROM telegram_catalog_dispatches d JOIN telegram_catalog_videos v
              ON v.channel_id=d.channel_id AND v.message_id=d.message_id
              WHERE d.channel_id=c.channel_id AND (d.state='failed' OR d.job_status IN ('failed','cancelled'))
                AND v.published_at>=c.sync_since_ts) AS auto_failed_count,
            (SELECT count(*) FROM telegram_relay_jobs j WHERE j.source_channel_id=c.channel_id
              AND j.source_kind='channel' AND j.status IN ('pending','downloading','uploading','cleaning')) AS auto_active_count
            FROM telegram_catalog_channels c"""

    async def _channel(self, channel_id):
        rows = await self._rows(self._channel_select() + " WHERE c.channel_id=?", (int(channel_id),))
        if not rows:
            raise CatalogError(404, "监控频道不存在或已移除")
        return self._channel_dict(rows[0], await self._config())

    async def _client(self):
        await self.ensure_schema()
        remaining = self._cooldown_until - time.time()
        if remaining > 0:
            raise CatalogError(429, f"Telegram 请求过于频繁，请在 {int(remaining) + 1} 秒后重试", int(remaining) + 1)
        client = self.client_getter()
        if inspect.isawaitable(client):
            client = await client
        if client is None or not client.is_connected():
            raise CatalogError(409, "请先启动 Telegram 监听并登录用户账号")
        async with asyncio.timeout(TELEGRAM_TIMEOUT):
            if not await client.is_user_authorized():
                raise CatalogError(409, "Telegram 用户账号尚未登录")
        return client

    async def _config(self):
        config = self.config_getter()
        return await config if inspect.isawaitable(config) else config

    @staticmethod
    def _check_source_channel(config, channel_id):
        raw_id = abs(int(channel_id)) - 1000000000000
        for field in ("telegram_channel_id", "teldrive_channel_id"):
            storage = abs(int(getattr(config, field, 0) or 0))
            if storage and storage in {abs(int(channel_id)), raw_id}:
                raise CatalogError(400, "TelDrive 存储频道用于自动回源，不能同时作为视频监控频道，请选择其他频道")

    async def _translate_error(self, error):
        if isinstance(error, CatalogError):
            return error
        if isinstance(error, FloodWaitError):
            seconds = max(1, int(error.seconds))
            self._cooldown_until = max(self._cooldown_until, time.time() + seconds)
            await self._write([("INSERT OR REPLACE INTO telegram_catalog_meta(key,value) VALUES ('cooldown_until',?)", (str(self._cooldown_until),))])
            return CatalogError(429, f"Telegram 请求过于频繁，请在 {seconds} 秒后重试", seconds)
        name = type(error).__name__
        if name in {"ChannelPrivateError", "ChannelInvalidError", "ChatAdminRequiredError", "UsernameInvalidError", "UsernameNotOccupiedError"} or isinstance(error, ValueError):
            return CatalogError(403, "当前 Telegram 账号无法读取该频道，请确认频道地址、加入状态和访问权限")
        if name in {"AuthKeyUnregisteredError", "SessionRevokedError", "UserDeactivatedError"}:
            return CatalogError(409, "Telegram 登录已失效，请重新登录")
        if isinstance(error, (ConnectionError, OSError, asyncio.TimeoutError)):
            return CatalogError(503, "Telegram 连接暂不可用，请稍后重试")
        if self.logger:
            self.logger.warning(f"Telegram catalog operation failed: {name}")
        return CatalogError(502, f"读取 Telegram 频道失败（{name}），请稍后重试")

    async def _resolve(self, client, reference):
        try:
            entity = await client.get_entity(reference)
        except ValueError:
            if not isinstance(reference, int):
                raise
            # Numeric private-channel links may not be cached in this session.
            # Looking through existing dialogs resolves them without joining.
            entity = None
            async for dialog in client.iter_dialogs(limit=1000):
                if utils.get_peer_id(dialog.entity) == reference:
                    entity = dialog.entity
                    break
            if entity is None:
                raise CatalogError(403, "当前会话找不到该频道，请先在 Telegram 中加入并打开它，或使用公开频道用户名")
        if not isinstance(entity, Channel) or not (entity.broadcast or entity.megagroup):
            raise CatalogError(400, "只支持 Telegram 频道和超级群组")
        return entity

    async def list_channels(self):
        rows = await self._rows(self._channel_select() + " ORDER BY c.created_at, c.channel_id")
        config = await self._config()
        return {"channels": [self._channel_dict(row, config) for row in rows],
                "cooldown_until": _iso(self._cooldown_until) if self._cooldown_until > time.time() else None}

    async def add_channel(self, reference):
        reference = parse_channel_reference(reference)
        async with self._add_lock:
            try:
                async with asyncio.timeout(TELEGRAM_TIMEOUT):
                    client = await self._client()
                    entity = await self._resolve(client, reference)
                    # A successful read, rather than an entity lookup alone, proves
                    # this session can read the history. No invitation is accepted.
                    await client.get_messages(entity, limit=1)
                channel_id = utils.get_peer_id(entity)
                self._check_source_channel(await self._config(), channel_id)
                async with self._lock(channel_id):
                    existing = await self._rows("SELECT channel_id FROM telegram_catalog_channels WHERE channel_id=?", (channel_id,))
                    if not existing:
                        count = await self._rows("SELECT count(*) AS count FROM telegram_catalog_channels")
                        if count[0]["count"] >= MAX_CHANNELS:
                            raise CatalogError(400, f"最多监控 {MAX_CHANNELS} 个频道，请先移除不用的频道")
                    now = time.time()
                    await self._write([("""INSERT INTO telegram_catalog_channels(channel_id,title,username,protected,sync_since,sync_since_ts)
                        VALUES (?,?,?,?,?,?) ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title,
                        username=excluded.username, protected=excluded.protected""",
                        (channel_id, str(entity.title)[:512], str(entity.username or ""), int(bool(entity.noforwards)), _iso(now), now))])
                    return await self._channel(channel_id)
            except Exception as error:
                raise await self._translate_error(error) from error

    async def update_channel(self, channel_id, enabled=None, *, target_path=None, auto_sync=None, sync_since=None):
        if enabled is not None and not isinstance(enabled, bool):
            raise CatalogError(400, "enabled 必须为布尔值")
        if auto_sync is not None and not isinstance(auto_sync, bool):
            raise CatalogError(400, "auto_sync 必须为布尔值")
        async with self._lock(channel_id):
            current = await self._channel(channel_id)
            updates = {}
            if enabled is not None:
                updates["enabled"] = int(enabled)
            if target_path is not None:
                updates["target_path"] = normalize_target_path(target_path)
            if sync_since is not None:
                updates["sync_since"], updates["sync_since_ts"] = normalize_sync_since(sync_since)
            if auto_sync is not None:
                updates["auto_sync"] = int(auto_sync)
                if auto_sync and not current["auto_sync"]:
                    updates["enabled"] = 1
            merged = {**current, **updates}
            if merged["auto_sync"] and (not merged["target_path"] or merged["sync_since_ts"] is None):
                raise CatalogError(400, "开启自动同步前，请选择 TelDrive 目标目录并设置有效的视频发布时间起点")
            if not updates:
                return current
            updates.update(last_sync_error=None, sync_backpressure=0)
            await self._write([("UPDATE telegram_catalog_channels SET " + ",".join(f"{key}=?" for key in updates) + " WHERE channel_id=?",
                                (*updates.values(), int(channel_id))), self._pending_statement(channel_id)])
            return await self._channel(channel_id)

    async def remove_channel(self, channel_id):
        async with self._lock(channel_id):
            await self._channel(channel_id)
            await self._write([
                ("DELETE FROM telegram_catalog_videos WHERE channel_id=?", (int(channel_id),)),
                ("DELETE FROM telegram_catalog_channels WHERE channel_id=?", (int(channel_id),)),
            ])
        # Already queued relay jobs intentionally remain intact.
        return {"removed": True}

    async def list_videos(self, channel_id, before_id=0, limit=50):
        channel = await self._channel(channel_id)
        before_id = max(0, int(before_id))
        limit = max(1, min(100, int(limit)))
        rows = await self._rows("""SELECT v.*, j.job_id, j.status AS job_status,
            j.download_progress, j.upload_progress, j.error AS job_error,
            d.state AS auto_status,d.error AS auto_error,d.job_id AS auto_job_id,d.job_status AS auto_job_status
            FROM telegram_catalog_videos v LEFT JOIN telegram_relay_jobs j
            ON j.source_channel_id=v.channel_id AND j.source_message_id=v.message_id
            LEFT JOIN telegram_catalog_dispatches d ON d.channel_id=v.channel_id AND d.message_id=v.message_id
            WHERE v.channel_id=? AND (?=0 OR v.message_id<?) ORDER BY v.message_id DESC LIMIT ?""",
            (int(channel_id), before_id, before_id, limit + 1))
        has_more = len(rows) > limit
        videos = rows[:limit]
        for video in videos:
            video["protected"] = bool(video["protected"])
        return {"channel": channel, "videos": videos, "has_more": has_more,
                "next_before_id": videos[-1]["message_id"] if has_more else None}

    @staticmethod
    def _video_statement(channel_id, entity, message, info):
        date = getattr(message, "date", None)
        if isinstance(date, datetime):
            date = date.replace(tzinfo=timezone.utc) if date.tzinfo is None else date
            date = date.astimezone(timezone.utc).isoformat()
        else:
            date = None
        username = str(entity.username or "")
        link = f"https://t.me/{username}/{int(message.id)}" if username else f"https://t.me/c/{entity.id}/{int(message.id)}"
        return ("""INSERT INTO telegram_catalog_videos
            (channel_id,message_id,file_name,file_size,mime_type,duration,caption,date,protected,link,published_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(channel_id,message_id) DO UPDATE SET
            file_name=excluded.file_name,file_size=excluded.file_size,mime_type=excluded.mime_type,
            duration=excluded.duration,caption=excluded.caption,date=excluded.date,protected=excluded.protected,link=excluded.link,published_at=excluded.published_at""",
            (int(channel_id), int(message.id), info["name"], info["size"], info["mime_type"], info["duration"],
             str(getattr(message, "message", "") or "")[:4096], date,
             int(bool(entity.noforwards or getattr(message, "noforwards", False))), link, _published_at(date)))

    async def scan_channel(self, channel_id, older=False):
        return await self._scan_channel(channel_id, older=older)

    async def _scan_channel(self, channel_id, older=False, automatic=False):
        async with self._lock(channel_id):
            channel = await self._channel(channel_id)
            if automatic and not channel["enabled"]:
                return {"channel": channel, "scanned": 0, "added": 0, "has_more": False}
            if older and channel["history_complete"]:
                return {"channel": channel, "scanned": 0, "added": 0, "has_more": False}
            try:
                self._check_source_channel(await self._config(), channel_id)
                kwargs = {"limit": SCAN_BATCH_SIZE}
                if older and channel["oldest_message_id"]:
                    kwargs["offset_id"] = channel["oldest_message_id"]
                elif channel["newest_message_id"]:
                    # Oldest unseen first: the cursor advances through a long
                    # reconnect gap instead of jumping over unseen messages.
                    kwargs.update(min_id=channel["newest_message_id"], reverse=True)
                async with asyncio.timeout(TELEGRAM_TIMEOUT):
                    client = await self._client()
                    entity = await self._resolve(client, int(channel_id))
                    messages = [message async for message in client.iter_messages(entity, **kwargs)]
                ids = [int(message.id) for message in messages if int(getattr(message, "id", 0)) > 0]
                statements = []
                video_ids = []
                for message in messages:
                    info = video_file_info(message)
                    if info:
                        statements.append(self._video_statement(channel_id, entity, message, info))
                        video_ids.append(int(message.id))
                newest = max([channel["newest_message_id"], *ids])
                oldest = min([value for value in [channel["oldest_message_id"], *ids] if value > 0], default=0)
                initial = not channel["newest_message_id"]
                complete = channel["history_complete"] or ((older or initial) and len(messages) < SCAN_BATCH_SIZE)
                dates = [stamp for stamp in [channel.get("oldest_scanned_ts"),
                         *(_published_at(getattr(message, "date", None)) for message in messages)] if stamp is not None]
                oldest_date = min(dates, default=None)
                statements.append(("""UPDATE telegram_catalog_channels SET newest_message_id=?,oldest_message_id=?,
                    history_complete=?,last_scan=?,last_error=NULL,title=?,username=?,protected=?,oldest_scanned_ts=? WHERE channel_id=?""",
                    (newest, oldest, int(complete), _iso(), str(entity.title)[:512], str(entity.username or ""),
                     int(bool(entity.noforwards)), oldest_date, int(channel_id))))
                if video_ids:
                    statements.append(self._pending_statement(channel_id, video_ids))
                await self._write(statements)
                updated = await self._channel(channel_id)
                return {"channel": updated, "scanned": len(messages),
                        "added": updated["video_count"] - channel["video_count"], "has_more": len(messages) == SCAN_BATCH_SIZE}
            except Exception as error:
                failure = await self._translate_error(error)
                await self._write([("UPDATE telegram_catalog_channels SET last_error=? WHERE channel_id=?", (failure.message, int(channel_id)))])
                raise failure from error

    async def enqueue_videos(self, channel_id, message_ids):
        if not isinstance(message_ids, list) or not 1 <= len(message_ids) <= 100:
            raise CatalogError(400, "每次请选择 1 至 100 个视频")
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in message_ids):
            raise CatalogError(400, "视频消息 ID 必须是正整数")
        ids = list(dict.fromkeys(message_ids))
        async with self._lock(channel_id):
            channel = await self._channel(channel_id)
            return await self._enqueue_locked(channel, ids)

    async def _enqueue_locked(self, channel, ids, *, automatic=False):
        channel_id = channel['channel_id']
        epoch = self._lifecycle_epoch
        config = await self._config()
        self._check_source_channel(config, channel_id)
        if not bool(getattr(config, "relay_enabled", False)):
            raise CatalogError(409, "请先在设置中启用 Telegram 下载上传功能")
        try:
            async with asyncio.timeout(TELEGRAM_TIMEOUT):
                client = await self._client()
                entity = await self._resolve(client, int(channel_id))
                # Fetch fresh messages; stored metadata cannot grant access to
                # deleted messages, expired documents or revoked membership.
                messages = await client.get_messages(entity, ids=ids)
        except Exception as error:
            raise await self._translate_error(error) from error
        if epoch != self._lifecycle_epoch:
            raise CatalogError(409, "Telegram 服务已停止或重启，请重新选择视频")
        # Runtime settings may change while Telegram is answering. Never
        # pass a pre-fetch enabled/config snapshot back to the relay.
        config = await self._config()
        self._check_source_channel(config, channel_id)
        if not bool(getattr(config, "relay_enabled", False)):
            raise CatalogError(409, "Telegram 下载上传功能已停用，请重新启用后选择视频")
        by_id = {int(message.id): message for message in messages if message is not None}
        known = await self._rows("SELECT message_id FROM telegram_catalog_videos WHERE channel_id=? AND message_id IN (" + ",".join("?" for _ in ids) + ")", (int(channel_id), *ids))
        known_ids = {row["message_id"] for row in known}
        results = []
        for message_id in ids:
            message = by_id.get(message_id)
            info = video_file_info(message) if message else None
            if message_id not in known_ids or info is None:
                results.append({"message_id": message_id, "success": False,
                                "error": "视频已删除、不可访问、带自毁限制或不在当前视频列表中", "error_code": 404})
                continue
            try:
                if automatic:
                    published = _published_at(getattr(message, "date", None))
                    if published is None or published < channel["sync_since_ts"]:
                        raise CatalogError(422, "视频发布时间不在当前自动同步范围内")
                    if await self._active_transfer_count() >= MAX_ACTIVE_TRANSFERS:
                        raise CatalogError(425, "传输队列已满，自动同步将在队列空闲后继续")
                config = await self._config()
                if epoch != self._lifecycle_epoch:
                    raise CatalogError(409, "Telegram 服务已停止或重启，请重新选择视频")
                self._check_source_channel(config, channel_id)
                if not bool(getattr(config, "relay_enabled", False)):
                    raise CatalogError(409, "Telegram 下载上传功能已停用，请重新启用后选择视频")
                # The relay owns the durable channel/message unique key.
                job = await self.relay_manager.enqueue_channel_video(config, message, info,
                    source_channel_id=int(channel_id), source_title=channel["title"],
                    target_path=normalize_target_path(channel.get("target_path") or getattr(config, "relay_target_path", "")
                        or getattr(config, "teldrive_target_path", "") or "/"))
                if job.get("status") in {"failed", "cancelled"}:
                    results.append({"message_id": message_id, "success": False, "job_id": job["job_id"],
                                    "status": job["status"], "error": "该视频已有失败或取消的任务，请到传输任务列表中重试"})
                    continue
                results.append({"message_id": message_id, "success": True,
                                "job_id": job["job_id"], "status": job.get("status", "pending")})
            except ValueError as error:
                results.append({"message_id": message_id, "success": False, "error": str(error)[:512], "error_code": 400})
            except Exception as error:
                failure = await self._translate_error(error)
                results.append({"message_id": message_id, "success": False, "error": failure.message, "error_code": failure.status_code})
        return {"results": results, "queued": sum(result["success"] for result in results),
                "failed": sum(not result["success"] for result in results)}

    async def _active_transfer_count(self):
        rows = await self._rows("SELECT count(*) AS count FROM telegram_relay_jobs WHERE status IN ('pending','downloading','uploading','cleaning')")
        count = rows[0]["count"]
        exists = await self._rows("SELECT 1 FROM sqlite_master WHERE type='table' AND name='tasks'")
        if exists:
            rows = await self._rows("SELECT count(*) AS count FROM tasks WHERE status IN ('pending','downloading','uploading')")
            count += rows[0]["count"]
        return count

    async def _dispatch_auto(self, channel_id, budget=AUTO_BATCH_SIZE):
        """Bounded dispatch; pending metadata remains durable across restarts."""
        if budget <= 0:
            return 0
        async with self._auto_lock:
            async with self._lock(channel_id):
                channel = await self._channel(channel_id)
                if not channel["enabled"] or not channel["auto_sync"]:
                    return 0
                config = await self._config()
                if not bool(getattr(config, "relay_enabled", False)):
                    return 0
                self._check_source_channel(config, channel_id)
                slots = max(0, MAX_ACTIVE_TRANSFERS - await self._active_transfer_count())
                if slots == 0:
                    await self._write([("UPDATE telegram_catalog_channels SET sync_backpressure=1 WHERE channel_id=?", (int(channel_id),))])
                    return 0
                candidates = await self._rows("""SELECT d.message_id,d.attempts FROM telegram_catalog_dispatches d
                    JOIN telegram_catalog_videos v ON v.channel_id=d.channel_id AND v.message_id=d.message_id
                    WHERE d.channel_id=? AND d.state='pending' AND d.next_retry_at<=? AND v.published_at>=?
                    ORDER BY v.published_at,d.message_id LIMIT ?""",
                    (int(channel_id), time.time(), channel["sync_since_ts"], min(int(budget), AUTO_BATCH_SIZE, slots)))
                if not candidates:
                    clear_error = not channel["auto_pending_count"] and not channel["auto_failed_count"]
                    await self._write([("UPDATE telegram_catalog_channels SET sync_backpressure=0,last_sync_error=CASE WHEN ? THEN NULL ELSE last_sync_error END WHERE channel_id=?",
                                        (int(clear_error), int(channel_id)))])
                    return 0
                ids = [candidate["message_id"] for candidate in candidates]
                attempts = {candidate["message_id"]: candidate["attempts"] for candidate in candidates}
                try:
                    response = await self._enqueue_locked(channel, ids, automatic=True)
                    results = response["results"]
                except Exception as error:
                    failure = await self._translate_error(error)
                    results = [{"message_id": mid, "success": False, "error": failure.message,
                                "error_code": failure.status_code} for mid in ids]
                statements = []
                last_error = None
                backpressure = False
                for result in results:
                    mid = result["message_id"]
                    if result["success"]:
                        # Real relay writes this receipt in its INSERT trigger.
                        # This fallback also supports custom relay adapters.
                        statements.append(("""UPDATE telegram_catalog_dispatches SET state='submitted',job_id=?,job_status=?,
                            target_path=?,error=NULL,updated_at=CURRENT_TIMESTAMP
                            WHERE channel_id=? AND message_id=? AND state='pending'""",
                            (result["job_id"], result.get("status", "pending"), channel["target_path"], int(channel_id), mid)))
                        continue
                    code = result.get("error_code", 400)
                    paused = code in {409, 425, 429}
                    attempt = attempts[mid] + (0 if paused else 1)
                    retry = paused or (code in {502, 503} and attempt < AUTO_MAX_ATTEMPTS)
                    next_retry = max(self._cooldown_until, time.time() + 60 * 2 ** min(attempt, 5)) if retry else 0
                    # Never overwrite an atomic submitted receipt if enqueue
                    # committed successfully and its later broadcast failed.
                    statements.append(("""UPDATE telegram_catalog_dispatches SET state=?,error=?,attempts=?,next_retry_at=?,
                        updated_at=CURRENT_TIMESTAMP WHERE channel_id=? AND message_id=? AND state='pending'""",
                        ("pending" if retry else "failed", result["error"], attempt, next_retry, int(channel_id), mid)))
                    backpressure = backpressure or code == 425
                    if code not in {409, 425}:
                        last_error = last_error or result["error"]
                statements.append(("UPDATE telegram_catalog_channels SET last_sync_error=?,sync_backpressure=? WHERE channel_id=?",
                                   (last_error, int(backpressure), int(channel_id))))
                await self._write(statements)
                # Attempts consume the round budget too, preventing a failing
                # channel from producing an unbounded request burst.
                return len(ids)

    async def _poll_once(self):
        channels = (await self.list_channels())["channels"]
        if channels:
            offset = self._poll_offset % len(channels)
            channels = channels[offset:] + channels[:offset]
            self._poll_offset = (offset + 1) % len(channels)
        budget = AUTO_BATCH_SIZE
        for channel in channels:
            if not channel["enabled"] or self._cooldown_until > time.time():
                continue
            try:
                scanned = await self._scan_channel(channel["channel_id"], automatic=True)
                if self._needs_history(scanned["channel"]) and await self._active_transfer_count() < MAX_ACTIVE_TRANSFERS:
                    await self._scan_channel(channel["channel_id"], older=True, automatic=True)
                budget -= await self._dispatch_auto(channel["channel_id"], budget)
            except CatalogError as error:
                if error.status_code in {409, 429}:
                    break

    async def start(self):
        await self.ensure_schema()
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll(), name="telegram-video-catalog")

    async def stop(self):
        self._lifecycle_epoch += 1
        if self._poll_task is not None:
            self._poll_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._poll_task
            self._poll_task = None
        async with self._db_lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None

    async def _poll(self):
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if self.logger:
                    self.logger.warning(f"Telegram catalog polling failed: {type(error).__name__}")
            await asyncio.sleep(POLL_INTERVAL)
