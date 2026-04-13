from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import mimetypes
import re
import secrets
import tomllib
from collections import deque
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from itertools import count
from pathlib import Path
from typing import Any


try:
    import psycopg2
except ModuleNotFoundError:
    psycopg2 = None

import qrcode
import qrcode.image.svg
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from fastapi.staticfiles import StaticFiles
from telethon import TelegramClient, events

from app import database as db
from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError
from telethon.password import compute_check
from telethon.tl.functions.account import GetPasswordRequest
from telethon.tl.functions.auth import CheckPasswordRequest, ExportLoginTokenRequest, ImportLoginTokenRequest
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
    auth,
)

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"
# 动态指向项目根目录的 config.toml
CONFIG_PATH = BASE_DIR.parent.parent.parent / "config.toml"
MAPPING_PATH = BASE_DIR / "file_msg_map.json"
DEFAULT_LOG_FILE = BASE_DIR / "runtime.log"
T2TD_ACTION_LOG_STREAM = "t2td_sync"
T2TD_ACTION_LOG_LIMIT_MULTIPLIER = 5
T2TD_ACTION_LOG_MIN_LIMIT = 500
INTERNAL_DELETE_GRACE_SECONDS = 120.0
MESSAGE_FETCH_BATCH_SIZE = 100

DEFAULT_CONFIG: dict[str, dict[str, Any]] = {
    "telegram": {
        "api_id": None,
        "api_hash": "",
        "channel_id": None,
        "session_name": "tel2teldrive_session",
    },
    "teldrive": {
        "api_host": "",
        "access_token": "",
        "channel_id": None,
        "sync_interval": 10,
        "sync_enabled": True,
        "max_scan_messages": 10000,
        "confirm_cycles": 3,

    },
    "telegram_db": {
        "host": "",
        "port": 5432,
        "user": "",
        "password": "",
        "name": "postgres",
    },
    "web": {
        "host": "0.0.0.0",
        "frontend_password": "",
        "frontend_monitor_port": 8200,
        "log_buffer_size": 400,
        "log_file": "runtime.log",
    },

}

FIELD_LABELS = {
    "telegram.api_id": "Telegram API ID",
    "telegram.api_hash": "Telegram API Hash",
    "telegram.channel_id": "Telegram 监听频道 ID",
    "telegram.session_name": "会话文件名",
    "teldrive.url": "TelDrive 地址",
    "teldrive.bearer_token": "TelDrive Bearer Token",
    "teldrive.channel_id": "TelDrive 频道 ID",
}

PHASE_LABELS = {
    "starting": "服务启动中",
    "awaiting_config": "等待网页配置",
    "connecting": "连接 Telegram",
    "awaiting_qr": "等待扫码登录",
    "awaiting_password": "等待两步验证",
    "authorized": "登录成功",
    "initializing": "初始化文件映射",
    "running": "实时监听中",
    "reconnecting": "连接中断，准备重连",
    "error": "服务异常",
    "stopped": "服务已停止",
}


@dataclass(slots=True)
class RuntimeConfig:
    config_exists: bool
    config_error: str | None
    telegram_api_id: int | None
    telegram_api_hash: str
    telegram_channel_id: int | None
    session_name: str
    teldrive_url: str
    bearer_token: str
    teldrive_channel_id: int | None
    sync_interval: int
    sync_enabled: bool
    max_scan_messages: int
    confirm_cycles: int
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    web_host: str
    frontend_password: str
    frontend_monitor_port: int
    log_buffer_size: int
    log_file: str

    missing_fields: list[str]

    @property
    def session_file(self) -> str:
        return f"{self.session_name}.session"

    @property
    def db_configured(self) -> bool:
        return bool(self.db_host)

    @property
    def db_enabled(self) -> bool:
        return self.db_configured and psycopg2 is not None

    @property
    def is_ready(self) -> bool:
        return not self.missing_fields and not self.config_error

    @property
    def log_file_path(self) -> Path:
        log_path = Path(self.log_file or "runtime.log")
        if not log_path.is_absolute():
            log_path = BASE_DIR / log_path
        return log_path


class ConfigStore:
    def __init__(self, path: Path):
        self.path = path
        self._config_exists = False
        self._config_error: str | None = None
        self._data = self._default_data()
        self.reload()

    def _default_data(self) -> dict[str, dict[str, Any]]:
        return json.loads(json.dumps(DEFAULT_CONFIG))

    def reload(self) -> RuntimeConfig:
        self._config_exists = self.path.exists()
        self._config_error = None
        raw: dict[str, Any] = {}
        if self._config_exists:
            try:
                with self.path.open("rb") as f:
                    loaded = tomllib.load(f)
                if isinstance(loaded, dict):
                    raw = loaded
            except Exception as exc:
                self._config_error = f"配置文件解析失败：{exc}"
                raw = {}
        self._data = self._normalize(raw)
        return self.runtime()

    def runtime(self) -> RuntimeConfig:
        return self._runtime_from_data(self._data)

    def runtime_from_payload(self, payload: Any, *, strict: bool = False) -> RuntimeConfig:
        if not isinstance(payload, dict):
            raise ValueError("配置数据格式错误")
        return self._runtime_from_data(self._normalize(payload, strict=strict))

    def _runtime_from_data(self, data: dict[str, dict[str, Any]]) -> RuntimeConfig:
        telegram = data["telegram"]
        teldrive = data["teldrive"]
        web = data["web"]
        missing_fields = self._collect_missing_fields(data)
        return RuntimeConfig(
            config_exists=self._config_exists,
            config_error=self._config_error,
            telegram_api_id=telegram["api_id"],
            telegram_api_hash=telegram["api_hash"],
            telegram_channel_id=telegram["channel_id"],
            session_name=telegram["session_name"],
            teldrive_url=teldrive["api_host"],
            bearer_token=teldrive["access_token"],
            teldrive_channel_id=teldrive["channel_id"],
            sync_interval=teldrive["sync_interval"],
            sync_enabled=teldrive["sync_enabled"],
            max_scan_messages=teldrive["max_scan_messages"],
            confirm_cycles=teldrive["confirm_cycles"],
            db_host=data.get("telegram_db", {}).get("host", ""),
            db_port=data.get("telegram_db", {}).get("port", 5432),
            db_user=data.get("telegram_db", {}).get("user", ""),
            db_password=data.get("telegram_db", {}).get("password", ""),
            db_name=data.get("telegram_db", {}).get("name", "postgres"),
            web_host=web["host"],
            frontend_password=web["frontend_password"],
            frontend_monitor_port=web["frontend_monitor_port"],
            log_buffer_size=web["log_buffer_size"],
            log_file=web["log_file"],
            missing_fields=missing_fields,
        )


    def payload(self) -> dict[str, Any]:
        runtime = self.runtime()
        return {
            "telegram": {
                "api_id": "" if runtime.telegram_api_id is None else runtime.telegram_api_id,
                "api_hash": runtime.telegram_api_hash,
                "channel_id": "" if runtime.telegram_channel_id is None else runtime.telegram_channel_id,
                "session_name": runtime.session_name,
            },
            "telegram_db": {
                "host": runtime.db_host,
                "port": runtime.db_port,
                "user": runtime.db_user,
                "password": runtime.db_password,
                "name": runtime.db_name,
            },
            "teldrive": {
                "api_host": runtime.teldrive_url,
                "access_token": runtime.bearer_token,
                "channel_id": "" if runtime.teldrive_channel_id is None else runtime.teldrive_channel_id,
                "sync_interval": runtime.sync_interval,
                "sync_enabled": runtime.sync_enabled,
                "max_scan_messages": runtime.max_scan_messages,
                "confirm_cycles": runtime.confirm_cycles,

            },
            "telegram_db": {
        "host": "",
        "port": 5432,
        "user": "",
        "password": "",
        "name": "postgres",
    },
    "web": {
                "host": runtime.web_host,
                "frontend_password": runtime.frontend_password,
                "frontend_monitor_port": runtime.frontend_monitor_port,
                "log_buffer_size": runtime.log_buffer_size,
                "log_file": runtime.log_file,
            },

            "meta": {
                "config_exists": runtime.config_exists,
                "config_ready": runtime.is_ready,
                "config_error": runtime.config_error,
                "missing_fields": runtime.missing_fields,
                "config_path": str(self.path),
            },
        }

    def save(self, payload: Any) -> RuntimeConfig:
        if not isinstance(payload, dict):
            raise ValueError("配置数据格式错误")
        self._data = self._normalize(payload, strict=True)
        self.path.write_text(self._dump_toml(self._data), encoding="utf-8")
        self._config_exists = True
        self._config_error = None
        return self.runtime()

    def _normalize(self, payload: dict[str, Any], *, strict: bool = False) -> dict[str, dict[str, Any]]:
        data = self._default_data()
        telegram_payload = payload.get("telegram") if isinstance(payload.get("telegram"), dict) else {}
        teldrive_payload = payload.get("teldrive") if isinstance(payload.get("teldrive"), dict) else {}
        telegram_db_payload = payload.get("telegram_db") if isinstance(payload.get("telegram_db"), dict) else {}
        web_payload = payload.get("web") if isinstance(payload.get("web"), dict) else {}

        telegram = data["telegram"]
        telegram["api_id"] = self._parse_optional_int(telegram_payload.get("api_id"), "Telegram API ID", strict=strict)
        telegram["api_hash"] = self._parse_string(telegram_payload.get("api_hash"))
        telegram["channel_id"] = self._parse_optional_int(telegram_payload.get("channel_id"), "Telegram 监听频道 ID", strict=strict)
        telegram["session_name"] = self._parse_string(
            telegram_payload.get("session_name"),
            fallback=DEFAULT_CONFIG["telegram"]["session_name"],
        )

        teldrive = data["teldrive"]
        teldrive["api_host"] = self._parse_string(teldrive_payload.get("api_host"))
        teldrive["access_token"] = self._parse_string(teldrive_payload.get("access_token"))
        teldrive["channel_id"] = self._parse_optional_int(teldrive_payload.get("channel_id"), "TelDrive 频道 ID", strict=strict)
        teldrive["sync_interval"] = self._parse_positive_int(
            teldrive_payload.get("sync_interval"),
            "删除同步轮询间隔",
            default=DEFAULT_CONFIG["teldrive"]["sync_interval"],
            strict=strict,
        )
        teldrive["sync_enabled"] = self._parse_bool(
            teldrive_payload.get("sync_enabled"),
            default=DEFAULT_CONFIG["teldrive"]["sync_enabled"],
        )
        teldrive["max_scan_messages"] = self._parse_positive_int(
            teldrive_payload.get("max_scan_messages"),
            "历史扫描上限",
            default=DEFAULT_CONFIG["teldrive"]["max_scan_messages"],
            strict=strict,
        )
        teldrive["confirm_cycles"] = self._parse_positive_int(
            teldrive_payload.get("confirm_cycles"),
            "确认周期",
            default=DEFAULT_CONFIG["teldrive"]["confirm_cycles"],
            strict=strict,
        )
        
        data.setdefault("telegram_db", {})
        telegram_db = data["telegram_db"]
        telegram_db["host"] = self._parse_string(telegram_db_payload.get("host"))
        telegram_db["port"] = self._parse_positive_int(telegram_db_payload.get("port"), "数据库端口", default=5432, strict=strict)
        telegram_db["user"] = self._parse_string(telegram_db_payload.get("user"))
        telegram_db["password"] = self._parse_string(telegram_db_payload.get("password"))
        telegram_db["name"] = self._parse_string(telegram_db_payload.get("name"), fallback="postgres")

        web = data["web"]
        web["host"] = self._parse_string(web_payload.get("host"), fallback=DEFAULT_CONFIG["web"]["host"])
        web["frontend_password"] = self._parse_string(web_payload.get("frontend_password"))
        web_port_value = web_payload.get("frontend_monitor_port", web_payload.get("port"))
        web["frontend_monitor_port"] = self._parse_positive_int(
            web_port_value,
            "前端监测端口",
            default=DEFAULT_CONFIG["web"]["frontend_monitor_port"],
            strict=strict,
        )
        web["log_buffer_size"] = self._parse_positive_int(
            web_payload.get("log_buffer_size"),
            "日志缓存条数",
            default=DEFAULT_CONFIG["web"]["log_buffer_size"],
            strict=strict,
        )
        web["log_file"] = self._parse_string(web_payload.get("log_file"), fallback=DEFAULT_CONFIG["web"]["log_file"])
        return data


    def _collect_missing_fields(self, data: dict[str, dict[str, Any]]) -> list[str]:
        missing: list[str] = []
        if data["telegram"]["api_id"] is None:
            missing.append(FIELD_LABELS["telegram.api_id"])
        if not data["telegram"]["api_hash"]:
            missing.append(FIELD_LABELS["telegram.api_hash"])
        if data["telegram"]["channel_id"] is None:
            missing.append(FIELD_LABELS["telegram.channel_id"])
        if not data["telegram"]["session_name"]:
            missing.append(FIELD_LABELS["telegram.session_name"])
        if not data["teldrive"]["api_host"]:
            missing.append("TelDrive API Host")
        if not data["teldrive"]["access_token"]:
            missing.append("TelDrive Access Token")
        if data["teldrive"]["channel_id"] is None:
            missing.append(FIELD_LABELS["teldrive.channel_id"])
        return missing

    def _parse_string(self, value: Any, *, fallback: str = "") -> str:
        if value is None:
            return fallback
        text = str(value).strip()
        return text or fallback

    def _parse_optional_int(self, value: Any, field_name: str, *, strict: bool) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError) as exc:
            if strict:
                raise ValueError(f"{field_name} 必须是整数") from exc
            return None

    def _parse_positive_int(self, value: Any, field_name: str, *, default: int, strict: bool) -> int:
        if value in (None, ""):
            return default
        try:
            result = int(str(value).strip())
        except (TypeError, ValueError) as exc:
            if strict:
                raise ValueError(f"{field_name} 必须是整数") from exc
            return default
        if result <= 0:
            if strict:
                raise ValueError(f"{field_name} 必须大于 0")
            return default
        return result

    def _parse_bool(self, value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
        return default

    def _format_toml_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int):
            return str(value)
        return json.dumps(str(value), ensure_ascii=False)

    def _dump_toml(self, data: dict[str, dict[str, Any]]) -> str:
        lines = ["# ================= Tel2TelDrive 配置文件 =================", ""]
        for section_name in ("telegram", "telegram_db", "teldrive", "web"):
            lines.append(f"[{section_name}]")
            for key, value in data[section_name].items():
                lines.append(f"{key} = {self._format_toml_value(value)}")
            lines.append("")
        return "\\n".join(lines).rstrip() + "\\n"


config_store = ConfigStore(CONFIG_PATH)
INITIAL_RUNTIME = config_store.runtime()
APP_BIND_HOST = INITIAL_RUNTIME.web_host
APP_BIND_PORT = INITIAL_RUNTIME.frontend_monitor_port
AUTH_COOKIE_NAME = "tel2teldrive_frontend_auth"
PUBLIC_PATHS = {"/", "/api/auth/status", "/api/auth/login"}


def frontend_auth_required(config: RuntimeConfig) -> bool:
    return bool(config.frontend_password)


def build_frontend_auth_cookie(password: str) -> str:
    raw = f"tel2teldrive::{CONFIG_PATH.resolve()}::{password}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def is_frontend_authenticated(request: Request, config: RuntimeConfig) -> bool:
    if not frontend_auth_required(config):
        return True
    current = request.cookies.get(AUTH_COOKIE_NAME, "")
    if not current:
        return False
    expected = build_frontend_auth_cookie(config.frontend_password)
    return secrets.compare_digest(current, expected)


def is_public_path(path: str) -> bool:
    return path in PUBLIC_PATHS or path == "/static" or path.startswith("/static/")


def iso_now() -> str:

    return datetime.now().astimezone().isoformat(timespec="seconds")


def format_local_time(value: str | None) -> str:
    if not value:
        return "--"
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def state_config_payload(config: RuntimeConfig) -> dict[str, Any]:
    return {
        "config_ready": config.is_ready,
        "config_exists": config.config_exists,
        "config_error": config.config_error,
        "missing_config_fields": config.missing_fields,
        "channel_id": config.telegram_channel_id,
        "session_file": config.session_file,
        "sync_enabled": config.sync_enabled,
        "sync_interval": config.sync_interval,
        "confirm_cycles": config.confirm_cycles,
        "max_scan_messages": config.max_scan_messages,
        "log_file": config.log_file_path.name,
    }


class DashboardBroker:
    def __init__(self, log_limit: int, config: RuntimeConfig):
        now = iso_now()
        self._logs: deque[dict[str, Any]] = deque(maxlen=log_limit)
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._state: dict[str, Any] = {
            "phase": "starting",
            "phase_label": PHASE_LABELS["starting"],
            "authorized": False,
            "needs_password": False,
            "qr_image": None,
            "qr_expires_at": None,
            "last_error": None,
            "updated_at": now,
            "service_started_at": now,
            "log_count": 0,
            "last_log_at": None,
            **state_config_payload(config),
        }

    def snapshot(self) -> dict[str, Any]:
        return dict(self._state)

    def logs_snapshot(self, limit: int = 200) -> list[dict[str, Any]]:
        data = list(self._logs)
        return data[-limit:]

    async def update_state(self, **kwargs: Any):
        if "phase" in kwargs and "phase_label" not in kwargs:
            kwargs["phase_label"] = PHASE_LABELS.get(kwargs["phase"], str(kwargs["phase"]))
        self._state.update(kwargs)
        self._state["updated_at"] = iso_now()
        await self._broadcast({"type": "state", "payload": self.snapshot()})

    def push_log(self, entry: dict[str, Any]):
        self._logs.append(entry)
        self._state["log_count"] = int(self._state.get("log_count", 0)) + 1
        self._state["last_log_at"] = entry["timestamp"]
        self._schedule_broadcast({"type": "log", "payload": entry})

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=256)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]):
        self._subscribers.discard(queue)

    def _schedule_broadcast(self, event: dict[str, Any]):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._broadcast(event))

    async def _broadcast(self, event: dict[str, Any]):
        stale: list[asyncio.Queue[dict[str, Any]]] = []
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                with suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    stale.append(queue)
        for queue in stale:
            self._subscribers.discard(queue)


class ActivityLogger:
    def __init__(self, broker: DashboardBroker, log_path: Path):
        self.broker = broker
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._counter = count(1)

    def set_log_path(self, log_path: Path):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def info(self, message: str):
        self._write("INFO", message)

    def warning(self, message: str):
        self._write("WARN", message)

    def error(self, message: str):
        self._write("ERROR", message)

    def _write(self, level: str, message: str):
        timestamp = iso_now()
        line = f"{format_local_time(timestamp)} [{level}] {message}"
        print(line, flush=True)
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\\n")
        self.broker.push_log(
            {
                "id": str(next(self._counter)),
                "timestamp": timestamp,
                "level": level,
                "message": message,
            }
        )


broker = DashboardBroker(INITIAL_RUNTIME.log_buffer_size, INITIAL_RUNTIME)
logger = ActivityLogger(broker, INITIAL_RUNTIME.log_file_path if INITIAL_RUNTIME.log_file else DEFAULT_LOG_FILE)


def normalize_message_ids(value: Any) -> list[int]:
    values = value
    if isinstance(values, dict):
        for key in ("msg_ids", "message_ids", "parts", "ids"):
            if key in values:
                values = values.get(key)
                break
        else:
            values = list(values.values())
    if values is None:
        return []
    if isinstance(values, (str, bytes)):
        values = [values]
    elif not isinstance(values, (list, tuple, set)):
        values = [values]

    result: list[int] = []
    seen: set[int] = set()
    for item in values:
        candidate = item.get("id") if isinstance(item, dict) else item
        try:
            msg_id = int(candidate)
        except (TypeError, ValueError):
            continue
        if msg_id <= 0 or msg_id in seen:
            continue
        seen.add(msg_id)
        result.append(msg_id)
    return result


def merge_message_ids(*groups: Any) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for group in groups:
        for msg_id in normalize_message_ids(group):
            if msg_id in seen:
                continue
            seen.add(msg_id)
            result.append(msg_id)
    return result


def normalize_mapping(mapping: Any) -> dict[str, list[int]]:
    if not isinstance(mapping, dict):
        return {}
    normalized: dict[str, list[int]] = {}
    for file_id, value in mapping.items():
        file_key = str(file_id).strip()
        if not file_key:
            continue
        msg_ids = normalize_message_ids(value)
        if msg_ids:
            normalized[file_key] = msg_ids
    return normalized


def load_mapping() -> dict[str, list[int]]:
    if MAPPING_PATH.exists():
        try:
            raw_mapping = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
            return normalize_mapping(raw_mapping)
        except Exception:
            return {}
    return {}


def save_mapping(mapping: dict[str, Any]):
    normalized = normalize_mapping(mapping)
    MAPPING_PATH.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def get_t2td_action_log_limit(config: RuntimeConfig) -> int:
    return max(T2TD_ACTION_LOG_MIN_LIMIT, int(config.log_buffer_size or 1) * T2TD_ACTION_LOG_LIMIT_MULTIPLIER)


_ignored_deleted_message_ids: dict[int, float] = {}
_ignored_deleted_file_ids: dict[str, float] = {}


def _cleanup_ignored_deletions(now_ts: float | None = None):
    current = now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp()
    expired_message_ids = [msg_id for msg_id, expires_at in _ignored_deleted_message_ids.items() if expires_at <= current]
    for msg_id in expired_message_ids:
        _ignored_deleted_message_ids.pop(msg_id, None)
    expired_file_ids = [file_id for file_id, expires_at in _ignored_deleted_file_ids.items() if expires_at <= current]
    for file_id in expired_file_ids:
        _ignored_deleted_file_ids.pop(file_id, None)


def remember_internal_deleted_message_ids(message_ids: Any):
    msg_ids = normalize_message_ids(message_ids)
    if not msg_ids:
        return
    now_ts = datetime.now(timezone.utc).timestamp()
    _cleanup_ignored_deletions(now_ts)
    expires_at = now_ts + INTERNAL_DELETE_GRACE_SECONDS
    for msg_id in msg_ids:
        _ignored_deleted_message_ids[msg_id] = expires_at


def filter_external_deleted_message_ids(message_ids: Any) -> list[int]:
    now_ts = datetime.now(timezone.utc).timestamp()
    _cleanup_ignored_deletions(now_ts)
    external_ids: list[int] = []
    for msg_id in normalize_message_ids(message_ids):
        expires_at = _ignored_deleted_message_ids.get(msg_id)
        if expires_at and expires_at > now_ts:
            _ignored_deleted_message_ids.pop(msg_id, None)
            continue
        external_ids.append(msg_id)
    return external_ids


def remember_internal_deleted_file_ids(file_ids: list[str]):
    if not file_ids:
        return
    now_ts = datetime.now(timezone.utc).timestamp()
    _cleanup_ignored_deletions(now_ts)
    expires_at = now_ts + INTERNAL_DELETE_GRACE_SECONDS
    for file_id in file_ids:
        file_key = str(file_id).strip()
        if file_key:
            _ignored_deleted_file_ids[file_key] = expires_at


def consume_internal_deleted_file_id(file_id: str) -> bool:
    file_key = str(file_id).strip()
    if not file_key:
        return False
    now_ts = datetime.now(timezone.utc).timestamp()
    _cleanup_ignored_deletions(now_ts)
    expires_at = _ignored_deleted_file_ids.get(file_key)
    if expires_at and expires_at > now_ts:
        _ignored_deleted_file_ids.pop(file_key, None)
        return True
    return False


def is_chunk_file(name: str) -> bool:
    return bool(re.search(r"\.\d+$", name))


def get_base_name(name: str) -> str:
    return re.sub(r"\.\d+$", "", name)


def is_md5_name(name: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{32}", name))


def build_qr_data_uri(login_url: str) -> str:
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_L, border=1, box_size=10)
    qr.add_data(login_url)
    image = qr.make_image(image_factory=qrcode.image.svg.SvgPathImage)
    buffer = BytesIO()
    image.save(buffer)
    svg_bytes = buffer.getvalue()
    encoded = base64.b64encode(svg_bytes).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


async def record_teldrive_action(
    config: RuntimeConfig,
    *,
    action: str,
    file_id: str,
    file_name: str,
    reason: str,
    message_ids: list[int],
    file_size: int | None = None,
    missing_message_ids: list[int] | None = None,
):
    payload = {
        "action": action,
        "file_id": file_id,
        "file_name": file_name,
        "reason": reason,
        "message_ids": normalize_message_ids(message_ids),
        "missing_message_ids": normalize_message_ids(missing_message_ids),
        "file_size": file_size,
        "source": "tel2teldrive_auto_sync",
        "occurred_at": iso_now(),
    }
    action_label = "自动新增" if action == "auto_add" else "自动删除"
    detail = f"TelDrive 文件{action_label}: {file_name} (file_id={file_id})"
    if payload["missing_message_ids"]:
        detail += f" | 缺失消息: {payload['missing_message_ids']}"
    logger.info(detail)
    try:
        await db.add_progress_log(
            action,
            payload,
            stream=T2TD_ACTION_LOG_STREAM,
            job_id=file_id,
            limit=get_t2td_action_log_limit(config),
        )
    except Exception as exc:
        logger.warning(f"写入 TelDrive 自动增删记录失败: {exc}")


async def add_file_to_teldrive(
    config: RuntimeConfig,
    file_name: str,
    file_size: int,
    mime_type: str,
    channel_id: int,
    message_id: int,
) -> str | None:
    headers = {
        "Authorization": f"Bearer {config.bearer_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "name": file_name,
        "type": "file",
        "path": "/",
        "mimeType": mime_type,
        "size": file_size,
        "channelId": channel_id,
        "parts": [{"id": message_id, "salt": ""}],
        "encrypted": False,
    }

    response = None
    try:
        response = requests.post(f"{config.teldrive_url}/api/files", headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        file_id = str(data.get("id", "")).strip()
        if file_id:
            mapping = load_mapping()
            mapping[file_id] = merge_message_ids(mapping.get(file_id), [message_id])
            save_mapping(mapping)
            await record_teldrive_action(
                config,
                action="auto_add",
                file_id=file_id,
                file_name=file_name,
                reason="telegram_new_message",
                message_ids=mapping[file_id],
                file_size=file_size,
            )
        return file_id or None
    except requests.exceptions.HTTPError:
        status_code = response.status_code if response is not None else "?"
        response_text = response.text if response is not None else ""
        logger.error(f"添加文件到 TelDrive 失败: HTTP {status_code} - {response_text}")
        return None
    except Exception as exc:
        logger.error(f"添加文件到 TelDrive 时出现异常: {exc}")
        return None


async def delete_file_from_teldrive(
    config: RuntimeConfig,
    *,
    file_id: str,
    file_name: str,
    message_ids: list[int],
    reason: str,
    missing_message_ids: list[int] | None = None,
    file_size: int | None = None,
) -> bool:
    response = None
    try:
        response = requests.post(
            f"{config.teldrive_url}/api/files/delete",
            headers={"Authorization": f"Bearer {config.bearer_token}", "Content-Type": "application/json"},
            json={"ids": [file_id]},
            timeout=30,
        )
        response.raise_for_status()
        remember_internal_deleted_file_ids([file_id])
        await record_teldrive_action(
            config,
            action="auto_delete",
            file_id=file_id,
            file_name=file_name,
            reason=reason,
            message_ids=message_ids,
            file_size=file_size,
            missing_message_ids=missing_message_ids,
        )
        return True
    except requests.exceptions.HTTPError:
        status_code = response.status_code if response is not None else "?"
        response_text = response.text if response is not None else ""
        logger.error(f"删除 TelDrive 文件失败: {file_name} (file_id={file_id}) HTTP {status_code} - {response_text}")
        return False
    except Exception as exc:
        logger.error(f"删除 TelDrive 文件异常: {file_name} (file_id={file_id}) - {exc}")
        return False


def extract_file_info(msg: Any) -> dict[str, Any] | None:
    media = msg.media
    if media is None:
        return None

    if isinstance(media, MessageMediaDocument):
        doc = media.document
        if doc is None:
            return None

        file_name = None
        mime_type = doc.mime_type or "application/octet-stream"
        file_size = doc.size

        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                file_name = attr.file_name
                break

        if not file_name:
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    ext = mimetypes.guess_extension(mime_type) or ".mp4"
                    file_name = f"video_{msg.id}{ext}"
                    break
                if isinstance(attr, DocumentAttributeAudio):
                    ext = mimetypes.guess_extension(mime_type) or ".mp3"
                    file_name = f"audio_{msg.id}{ext}"
                    break
            if not file_name:
                ext = mimetypes.guess_extension(mime_type) or ".bin"
                file_name = f"file_{msg.id}{ext}"

        return {
            "name": file_name,
            "size": file_size,
            "mime_type": mime_type,
        }

    if isinstance(media, MessageMediaPhoto):
        photo = media.photo
        if photo is None:
            return None
        largest = max(photo.sizes, key=lambda size: getattr(size, "size", 0), default=None)
        file_size = getattr(largest, "size", 0)
        return {
            "name": f"photo_{msg.id}.jpg",
            "size": file_size,
            "mime_type": "image/jpeg",
        }

    return None


def list_teldrive_dir(config: RuntimeConfig, path: str) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {config.bearer_token}"}
    items: list[dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "path": path,
            "op": "list",
            "perPage": 500,
            "page": page,
        }
        try:
            response = requests.get(f"{config.teldrive_url}/api/files", headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            logger.warning(f"获取 TelDrive 目录 {path} 失败: {exc}")
            return items

        items.extend(data.get("items", []))
        meta = data.get("meta", {})
        total_pages = meta.get("totalPages", 1)
        if page >= total_pages:
            break
        page += 1

    return items


def get_teldrive_files(config: RuntimeConfig) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    dirs_to_scan = ["/"]

    while dirs_to_scan:
        current_path = dirs_to_scan.pop()
        items = list_teldrive_dir(config, current_path)
        for item in items:
            item_type = item.get("type", "")
            item_id = item.get("id", "")
            item_name = item.get("name", "")
            item_size = item.get("size", 0)
            if item_type == "folder":
                sub_path = current_path.rstrip("/") + "/" + item_name
                dirs_to_scan.append(sub_path)
            elif item_id:
                result[item_id] = {"name": item_name, "size": item_size}

    return result


def query_db_mapping(config: RuntimeConfig) -> dict[str, list[int]]:
    if not config.db_enabled:
        return {}

    try:
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            user=config.db_user,
            password=config.db_password,
            database=config.db_name,
        )
        cur = conn.cursor()
        cur.execute("SELECT id, name, parts FROM teldrive.files WHERE type='file' AND parts IS NOT NULL")
        result: dict[str, list[int]] = {}
        skipped = 0
        for row in cur.fetchall():
            file_id, name, parts = str(row[0]), row[1], row[2]
            if is_md5_name(name):
                skipped += 1
                continue
            msg_ids = [part["id"] for part in parts if "id" in part]
            if msg_ids:
                result[file_id] = msg_ids
        conn.close()
        if skipped:
            logger.info(f"数据库映射中跳过 {skipped} 个 MD5 分片记录")
        return result
    except Exception as exc:
        logger.warning(f"TelDrive 数据库映射查询失败: {exc}")
        return {}


def query_db_msg_ids(config: RuntimeConfig) -> set[int]:
    if not config.db_enabled:
        return set()

    try:
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            user=config.db_user,
            password=config.db_password,
            database=config.db_name,
        )
        cur = conn.cursor()
        cur.execute("SELECT parts FROM teldrive.files WHERE type='file' AND parts IS NOT NULL")
        all_ids: set[int] = set()
        for (parts,) in cur.fetchall():
            for part in parts:
                if "id" in part:
                    all_ids.add(part["id"])
        conn.close()
        return all_ids
    except Exception as exc:
        logger.warning(f"TelDrive 消息 ID 查询失败: {exc}")
        return set()


def get_db_missing_fields(config: RuntimeConfig) -> list[str]:
    missing: list[str] = []
    if not config.db_host:
        missing.append("DB Host")
    if not config.db_user:
        missing.append("DB User")
    if not config.db_password:
        missing.append("DB Password")
    if not config.db_name:
        missing.append("DB Name")
    return missing


def test_database_connection(config: RuntimeConfig) -> dict[str, Any]:
    if psycopg2 is None:
        raise RuntimeError("当前环境未安装 psycopg2-binary，无法测试数据库连接")

    missing_fields = get_db_missing_fields(config)
    if missing_fields:
        raise ValueError(f"请先填写：{'、'.join(missing_fields)}")

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=config.db_host,
            port=config.db_port,
            user=config.db_user,
            password=config.db_password,
            database=config.db_name,
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user")
        database_name, user_name = cur.fetchone()
        return {
            "ok": True,
            "message": f"数据库连接成功：{database_name} / {user_name}@{config.db_host}:{config.db_port}",
        }
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(f"数据库连接失败：{exc}") from exc
    finally:
        if cur is not None:
            with suppress(Exception):
                cur.close()
        if conn is not None:
            with suppress(Exception):
                conn.close()


async def find_file_message_ids(
    client: TelegramClient,
    config: RuntimeConfig,
    file_names: list[str],
) -> dict[str, list[int]]:
    target_names = {name for name in file_names if name and not is_md5_name(name)}
    if not target_names:
        return {}

    found: dict[str, list[int]] = {name: [] for name in target_names}
    scanned = 0
    async for msg in client.iter_messages(config.telegram_channel_id, limit=config.max_scan_messages):
        scanned += 1
        try:
            file_info = extract_file_info(msg)
        except Exception:
            continue
        if file_info is None:
            continue

        name = file_info["name"]
        matched_name: str | None = None
        if name in target_names:
            matched_name = name
        elif is_chunk_file(name):
            base_name = get_base_name(name)
            if base_name in target_names:
                matched_name = base_name

        if not matched_name:
            continue

        found[matched_name] = merge_message_ids(found.get(matched_name), [msg.id])
        if scanned % 500 == 0:
            matched_count = sum(1 for ids in found.values() if ids)
            logger.info(f"文件映射扫描中: 已扫描 {scanned} 条消息，已命中 {matched_count}/{len(target_names)} 个文件")

    return {name: ids for name, ids in found.items() if ids}


def sync_mapping_from_db(config: RuntimeConfig, mapping: dict[str, list[int]], file_ids: list[str] | None = None) -> int:
    if not config.db_enabled:
        return 0
    db_mapping = query_db_mapping(config)
    if not db_mapping:
        return 0

    target_ids = {str(file_id).strip() for file_id in (file_ids or list(db_mapping.keys())) if str(file_id).strip()}
    updated = 0
    for file_id in target_ids:
        if file_id not in db_mapping:
            continue
        merged_ids = merge_message_ids(mapping.get(file_id), db_mapping[file_id])
        if merged_ids != normalize_message_ids(mapping.get(file_id)):
            mapping[file_id] = merged_ids
            updated += 1
    return updated


async def get_existing_message_ids(client: TelegramClient, channel_id: int, message_ids: list[int]) -> set[int] | None:
    """查询 Telegram 消息是否存在。查询失败时返回 None（保守策略），调用方应跳过删除判断。"""
    existing_ids: set[int] = set()
    normalized_ids = normalize_message_ids(message_ids)
    if not normalized_ids:
        return existing_ids

    for index in range(0, len(normalized_ids), MESSAGE_FETCH_BATCH_SIZE):
        batch = normalized_ids[index:index + MESSAGE_FETCH_BATCH_SIZE]
        try:
            messages = await client.get_messages(channel_id, ids=batch)
        except Exception as exc:
            logger.warning(f"批量查询 Telegram 消息状态失败: {exc}")
            # 查询异常时返回 None，表示无法确认消息状态，调用方应跳过删除
            return None
        for message in messages or []:
            if message is None or message.__class__.__name__ == "MessageEmpty":
                continue
            msg_id = getattr(message, "id", None)
            if isinstance(msg_id, int) and msg_id > 0:
                existing_ids.add(msg_id)
    return existing_ids


async def delete_teldrive_files_for_missing_messages(
    config: RuntimeConfig,
    missing_message_ids: list[int],
    *,
    td_files: dict[str, dict[str, Any]] | None = None,
    reason: str = "telegram_message_missing",
) -> int:
    missing_ids = set(normalize_message_ids(missing_message_ids))
    if not missing_ids:
        return 0

    mapping = load_mapping()
    current_teldrive_files = td_files if td_files is not None else get_teldrive_files(config)
    deleted_count = 0
    mapping_changed = False

    for file_id, tracked_ids in list(mapping.items()):
        stored_msg_ids = normalize_message_ids(tracked_ids)
        lost_ids = [msg_id for msg_id in stored_msg_ids if msg_id in missing_ids]
        if not lost_ids:
            continue

        file_info = current_teldrive_files.get(file_id)
        if not file_info:
            mapping.pop(file_id, None)
            mapping_changed = True
            continue

        file_name = str(file_info.get("name", "")).strip() or file_id
        logger.warning(f"检测到 Telegram 分块缺失，准备删除 TelDrive 文件: {file_name} (file_id={file_id})")
        deleted = await delete_file_from_teldrive(
            config,
            file_id=file_id,
            file_name=file_name,
            message_ids=stored_msg_ids,
            missing_message_ids=lost_ids,
            reason=reason,
            file_size=int(file_info.get("size", 0) or 0),
        )
        if deleted:
            mapping.pop(file_id, None)
            mapping_changed = True
            deleted_count += 1

    if mapping_changed:
        save_mapping(mapping)
    return deleted_count


async def build_initial_mapping(client: TelegramClient, config: RuntimeConfig):
    logger.info("开始构建文件映射")
    td_files = get_teldrive_files(config)
    mapping = load_mapping()

    stale_ids = [file_id for file_id in mapping if file_id not in td_files]
    if stale_ids:
        for file_id in stale_ids:
            mapping.pop(file_id, None)
        logger.info(f"已清理 {len(stale_ids)} 条过期映射")

    if config.db_enabled:
        updated = sync_mapping_from_db(config, mapping)
        if updated:
            save_mapping(mapping)
            logger.info(f"已从数据库刷新文件映射: {updated} 条")
            return
        logger.warning("数据库未返回可用映射，回退到频道扫描")

    target_names = [
        info.get("name", "")
        for info in td_files.values()
        if isinstance(info, dict) and info.get("name") and not is_md5_name(str(info.get("name")))
    ]
    if not target_names:
        save_mapping(mapping)
        logger.info("当前没有需要构建映射的 TelDrive 文件")
        return

    found_message_ids = await find_file_message_ids(client, config, target_names)
    matched_count = 0
    missing_count = 0
    for file_id, info in td_files.items():
        file_name = str(info.get("name", "")).strip() if isinstance(info, dict) else ""
        if not file_name or is_md5_name(file_name):
            continue
        msg_ids = found_message_ids.get(file_name)
        if msg_ids:
            mapping[file_id] = merge_message_ids(mapping.get(file_id), msg_ids)
            matched_count += 1
        else:
            missing_count += 1

    save_mapping(mapping)
    logger.info(f"映射构建完成: 匹配到 {matched_count} 个文件，未找到 {missing_count} 个，总计 {len(mapping)} 条")
    if missing_count:
        logger.warning(f"仍有 {missing_count} 个 TelDrive 文件未找到对应 Telegram 消息")


# 安全阈值常量：当缺失比例超过此值且缺失数量超过 MISSING_SAFE_ABS_THRESHOLD 时，判定为查询异常
MISSING_SAFE_RATIO = 0.5
MISSING_SAFE_ABS_THRESHOLD = 10


async def sync_deletions(client: TelegramClient, config: RuntimeConfig):
    # 安全校验：channel_id 无效时不启动删除同步
    if not config.telegram_channel_id:
        logger.warning(f"Telegram 频道 ID 为空或为 0，删除同步已禁用（当前值: {config.telegram_channel_id!r}）")
        return

    logger.info(f"删除同步已启动，轮询间隔 {config.sync_interval} 秒")
    prev_files = get_teldrive_files(config)
    prev_ids = set(prev_files.keys())
    logger.info(f"初始 TelDrive 快照共 {len(prev_ids)} 个文件")
    pending_deletions: dict[str, dict[str, Any]] = {}

    while True:
        await asyncio.sleep(config.sync_interval)
        curr_files = get_teldrive_files(config)
        mapping = load_mapping()
        tracked_message_ids = merge_message_ids(*mapping.values())
        if tracked_message_ids:
            existing_message_ids = await get_existing_message_ids(client, config.telegram_channel_id, tracked_message_ids)
            # 查询失败（返回 None）时跳过本轮删除判断，避免误删
            if existing_message_ids is None:
                logger.warning("Telegram 消息状态查询失败，本轮跳过删除同步")
            else:
                missing_message_ids = [msg_id for msg_id in tracked_message_ids if msg_id not in existing_message_ids]
                if missing_message_ids:
                    total = len(tracked_message_ids)
                    missing_count = len(missing_message_ids)
                    ratio = missing_count / total if total else 0
                    # 安全阈值：缺失比例过高时极可能是查询异常，拒绝执行删除
                    if ratio > MISSING_SAFE_RATIO and missing_count > MISSING_SAFE_ABS_THRESHOLD:
                        logger.error(
                            f"⚠️ 安全保护触发！缺失消息 {missing_count}/{total} ({ratio:.0%}) 超过安全阈值，"
                            f"疑似频道 ID 错误或网络异常，已阻止本轮所有删除操作"
                        )
                    else:
                        deleted_count = await delete_teldrive_files_for_missing_messages(
                            config,
                            missing_message_ids,
                            td_files=curr_files,
                            reason="telegram_part_deleted",
                        )
                        if deleted_count:
                            logger.warning(f"检测到 Telegram 文件分块缺失，已自动删除 {deleted_count} 个 TelDrive 文件")
                            curr_files = get_teldrive_files(config)
                            mapping = load_mapping()

        curr_ids = set(curr_files.keys())
        curr_names = {info["name"] for info in curr_files.values() if isinstance(info, dict) and info.get("name")}
        disappeared_ids = prev_ids - curr_ids
        new_ids = curr_ids - prev_ids

        logger.info(
            f"同步检查: 上次 {len(prev_ids)} 个 -> 本次 {len(curr_ids)} 个 | 新增 {len(new_ids)} | 消失 {len(disappeared_ids)}"
        )

        if disappeared_ids:
            for file_id in disappeared_ids:
                old_info = prev_files.get(file_id, {})
                old_name = old_info.get("name", "") if isinstance(old_info, dict) else ""
                if consume_internal_deleted_file_id(file_id):
                    mapping.pop(file_id, None)
                    continue
                if old_name and old_name in curr_names:
                    new_name_to_id = {
                        info["name"]: new_id
                        for new_id, info in curr_files.items()
                        if new_id in new_ids and isinstance(info, dict) and info.get("name")
                    }
                    old_messages = normalize_message_ids(mapping.pop(file_id, []))
                    if old_name in new_name_to_id:
                        new_file_id = new_name_to_id[old_name]
                        mapping[new_file_id] = merge_message_ids(mapping.get(new_file_id), old_messages)
                        logger.info(f"检测到文件迁移，已迁移映射: {old_name}")
                    save_mapping(mapping)
                elif file_id not in pending_deletions:
                    if is_md5_name(old_name):
                        continue
                    pending_deletions[file_id] = {
                        "name": old_name,
                        "msg_ids": normalize_message_ids(mapping.get(file_id, [])),
                        "count": 1,
                    }
                    logger.warning(f"文件消失待确认: {old_name} (1/{config.confirm_cycles})")

        confirmed_ids: list[str] = []
        for file_id, info in list(pending_deletions.items()):
            name = info["name"]
            if name in curr_names:
                logger.info(f"文件重新出现，取消删除: {name}")
                for new_id, new_info in curr_files.items():
                    if new_info["name"] == name and new_id not in mapping:
                        mapping[new_id] = merge_message_ids(mapping.get(new_id), info["msg_ids"])
                        logger.info(f"已恢复文件映射: {name}")
                        break
                del pending_deletions[file_id]
                mapping.pop(file_id, None)
                save_mapping(mapping)
                continue

            info["count"] += 1
            if info["count"] >= config.confirm_cycles:
                confirmed_ids.append(file_id)
            else:
                logger.warning(f"文件持续消失: {name} ({info['count']}/{config.confirm_cycles})")

        if confirmed_ids:
            msg_ids_to_delete: list[int] = []
            for file_id in confirmed_ids:
                info = pending_deletions.pop(file_id)
                msg_ids_to_delete = merge_message_ids(msg_ids_to_delete, info["msg_ids"])
                mapping.pop(file_id, None)

            if msg_ids_to_delete:
                logger.warning(
                    f"确认删除 {len(confirmed_ids)} 个文件，准备清理 {len(msg_ids_to_delete)} 条频道消息"
                )
                try:
                    remember_internal_deleted_message_ids(msg_ids_to_delete)
                    await client.delete_messages(config.telegram_channel_id, msg_ids_to_delete)
                    logger.info(f"已删除 {len(msg_ids_to_delete)} 条频道消息")
                except Exception as exc:
                    logger.error(f"删除频道消息失败: {exc}")
            save_mapping(mapping)

        if new_ids:
            mapping = load_mapping()
            updated = sync_mapping_from_db(config, mapping, list(new_ids)) if config.db_enabled else 0
            unresolved_file_ids = [file_id for file_id in new_ids if file_id not in mapping]
            if unresolved_file_ids:
                unresolved_names = [
                    curr_files[file_id]["name"]
                    for file_id in unresolved_file_ids
                    if file_id in curr_files and not is_md5_name(str(curr_files[file_id].get("name", "")))
                ]
                if unresolved_names:
                    found_message_ids = await find_file_message_ids(client, config, unresolved_names)
                    matched = 0
                    for file_id in unresolved_file_ids:
                        file_name = str(curr_files.get(file_id, {}).get("name", "")).strip()
                        msg_ids = found_message_ids.get(file_name)
                        if msg_ids:
                            mapping[file_id] = merge_message_ids(mapping.get(file_id), msg_ids)
                            matched += 1
                    if matched:
                        updated += matched
            if updated:
                save_mapping(mapping)
                logger.info(f"已为 {updated} 个 TelDrive 新文件建立/刷新映射")
            remaining = len([file_id for file_id in new_ids if file_id not in mapping])
            if remaining:
                logger.warning(f"仍有 {remaining} 个新文件暂无可用映射")

        prev_ids = curr_ids
        prev_files = curr_files


class Tel2TelDriveService:
    def __init__(self):
        self.client: TelegramClient | None = None
        self.sync_task: asyncio.Task[Any] | None = None
        self.stop_event = asyncio.Event()
        self.reload_event = asyncio.Event()
        self.refresh_qr_event = asyncio.Event()
        self.password_future: asyncio.Future[str] | None = None

    async def run_forever(self):
        logger.info("=" * 56)
        logger.info("Telegram 监听中转服务启动")
        logger.info("=" * 56)

        while not self.stop_event.is_set():
            config = config_store.runtime()
            logger.set_log_path(config.log_file_path)
            await broker.update_state(**state_config_payload(config))

            if config.db_configured and not config.db_enabled:
                logger.warning("检测到已配置 TelDrive 数据库，但本地未安装 psycopg2-binary，将回退到频道扫描模式")

            if not config.is_ready:
                message = config.config_error or "配置未完成，请先在“参数配置”页面中填写并保存参数。"

                await broker.update_state(
                    phase="awaiting_config",
                    authorized=False,
                    needs_password=False,
                    qr_image=None,
                    qr_expires_at=None,
                    last_error=message,
                    **state_config_payload(config),
                )
                signal = await self._wait_for_signal()
                if signal == "stop":
                    break
                continue

            self.reload_event.clear()
            self.client = TelegramClient(config.session_name, config.telegram_api_id, config.telegram_api_hash)
            try:
                await broker.update_state(
                    phase="connecting",
                    authorized=False,
                    needs_password=False,
                    qr_image=None,
                    qr_expires_at=None,
                    last_error=None,
                    **state_config_payload(config),
                )
                logger.info("正在连接 Telegram")
                await self.client.connect()

                if not await self.client.is_user_authorized():
                    logger.warning("当前会话未授权，进入扫码登录流程")
                    await self.authorize_with_dashboard(self.client, config)

                await broker.update_state(
                    phase="initializing",
                    authorized=True,
                    needs_password=False,
                    qr_image=None,
                    qr_expires_at=None,
                    last_error=None,
                    **state_config_payload(config),
                )
                await build_initial_mapping(self.client, config)
                self.register_handlers(self.client, config)

                if config.sync_enabled:
                    if not config.telegram_channel_id:
                        logger.warning(f"Telegram 频道 ID 无效 ({config.telegram_channel_id!r})，删除同步已跳过")
                    else:
                        self.sync_task = asyncio.create_task(sync_deletions(self.client, config))
                else:
                    logger.info("删除同步已关闭 (sync_enabled = false)")

                await broker.update_state(
                    phase="running",
                    authorized=True,
                    needs_password=False,
                    qr_image=None,
                    qr_expires_at=None,
                    last_error=None,
                    **state_config_payload(config),
                )
                logger.info(f"正在监听频道 {config.telegram_channel_id} 的新消息")
                await self.client.run_until_disconnected()
                if self.stop_event.is_set():
                    break
                if self.reload_event.is_set():
                    logger.info("检测到配置更新，正在重新加载服务")
                    continue
                logger.warning("Telegram 连接已断开")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self.reload_event.is_set() and not self.stop_event.is_set():
                    logger.info("配置已更新，准备重新加载 Telegram 连接")
                else:
                    logger.error(f"服务运行异常: {exc}")
                    await broker.update_state(
                        phase="error",
                        authorized=False,
                        needs_password=False,
                        qr_image=None,
                        qr_expires_at=None,
                        last_error=str(exc),
                        **state_config_payload(config),
                    )
            finally:
                await self._cleanup_client()

            if self.stop_event.is_set():
                break
            if self.reload_event.is_set():
                self.reload_event.clear()
                continue

            await broker.update_state(
                phase="reconnecting",
                authorized=False,
                needs_password=False,
                qr_image=None,
                qr_expires_at=None,
                last_error=None,
                **state_config_payload(config_store.runtime()),
            )
            logger.info("5 秒后尝试重新连接 Telegram")
            signal = await self._wait_for_signal(timeout=5)
            if signal == "stop":
                break
            if signal == "reload":
                continue

        await broker.update_state(phase="stopped", authorized=False, needs_password=False, qr_image=None)
        logger.info("Tel2TelDrive 服务已停止")

    async def stop(self):
        self.stop_event.set()
        self.reload_event.set()
        self.refresh_qr_event.set()
        if self.password_future and not self.password_future.done():
            self.password_future.cancel()
        # 取消同步任务
        if self.sync_task and not self.sync_task.done():
            self.sync_task.cancel()
            try:
                await self.sync_task
            except (asyncio.CancelledError, Exception):
                pass
        # 断开 Telegram 客户端连接，解除 run_until_disconnected 阻塞
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass

    async def request_reload(self):
        self.reload_event.set()
        self.refresh_qr_event.set()
        if self.password_future and not self.password_future.done():
            self.password_future.cancel()
        if self.client and self.client.is_connected():
            with suppress(Exception):
                await self.client.disconnect()

    async def request_qr_refresh(self):
        phase = broker.snapshot().get("phase")
        if phase != "awaiting_qr":
            raise RuntimeError("当前不是扫码登录状态，无需刷新二维码")
        await broker.update_state(
            phase="awaiting_qr",
            authorized=False,
            needs_password=False,
            qr_image=None,
            qr_expires_at=None,
            last_error=None,
        )
        self.refresh_qr_event.set()
        logger.info("管理员发起二维码刷新请求")


    async def submit_password(self, password: str):
        if not password:
            raise RuntimeError("两步验证密码不能为空")
        if not self.password_future or self.password_future.done():
            raise RuntimeError("当前无需输入两步验证密码")
        self.password_future.set_result(password)
        logger.info("已收到管理员提交的两步验证密码")

    async def _wait_for_signal(self, timeout: float | None = None) -> str | None:
        stop_task = asyncio.create_task(self.stop_event.wait())
        reload_task = asyncio.create_task(self.reload_event.wait())
        try:
            done, pending = await asyncio.wait(
                {stop_task, reload_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            pass

        if not done:
            for task in (stop_task, reload_task):
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            return None

        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        if stop_task in done and stop_task.result():
            return "stop"
        if reload_task in done and reload_task.result():
            self.reload_event.clear()
            return "reload"
        return None

    async def _cleanup_client(self):
        if self.sync_task:
            self.sync_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.sync_task
            self.sync_task = None

        if self.client:
            with suppress(Exception):
                if self.client.is_connected():
                    await self.client.disconnect()
            self.client = None

    def register_handlers(self, client: TelegramClient, config: RuntimeConfig):
        @client.on(events.NewMessage(chats=config.telegram_channel_id))
        async def on_new_message(event: Any):
            await self.handle_new_message(client, config, event.message)

        @client.on(events.MessageDeleted(chats=config.telegram_channel_id))
        async def on_message_deleted(event: Any):
            deleted_ids = filter_external_deleted_message_ids(getattr(event, "deleted_ids", None) or [])
            if not deleted_ids:
                return
            deleted_count = await delete_teldrive_files_for_missing_messages(
                config,
                deleted_ids,
                reason="telegram_message_deleted",
            )
            if deleted_count:
                logger.warning(f"检测到 Telegram 删除事件，已同步删除 {deleted_count} 个 TelDrive 文件")

    async def handle_new_message(self, client: TelegramClient, config: RuntimeConfig, msg: Any):
        file_info = extract_file_info(msg)
        if file_info is None:
            return

        name = file_info["name"]
        size = file_info["size"]
        logger.info(f"检测到新文件: {name} ({size:,} bytes)")

        if is_chunk_file(name):
            logger.info(f"分片文件已跳过: {name} -> {get_base_name(name)}")
            return

        if is_md5_name(name):
            logger.info(f"检测到 MD5 分片文件，已跳过: {name}")
            if config.db_enabled:
                known_ids = query_db_msg_ids(config)
                if msg.id in known_ids:
                    logger.info(f"msg_id={msg.id} 已在 TelDrive 数据库中登记")
                else:
                    logger.warning(f"msg_id={msg.id} 尚未在 TelDrive 数据库中找到记录")
            return

        mapping = load_mapping()
        td_files = get_teldrive_files(config)

        mapped_names = set()
        for file_id in mapping:
            info = td_files.get(file_id)
            file_name = info["name"] if info else ""
            if file_name:
                mapped_names.add(file_name)

        if name in mapped_names:
            logger.warning(f"检测到重复消息，准备删除: {name} (msg_id={msg.id})")
            try:
                remember_internal_deleted_message_ids([msg.id])
                await client.delete_messages(config.telegram_channel_id, [msg.id])
                logger.info(f"重复消息已删除: {name} (msg_id={msg.id})")
            except Exception as exc:
                logger.error(f"删除重复消息失败: {exc}")
            return

        existing_name_to_id = {info["name"]: file_id for file_id, info in td_files.items() if isinstance(info, dict) and info.get("name")}
        if name in existing_name_to_id:
            file_id = existing_name_to_id[name]
            mapping[file_id] = merge_message_ids(mapping.get(file_id), [msg.id])
            save_mapping(mapping)
            logger.info(f"TelDrive 已存在该文件，仅补充映射: {name}")
            return

        result = await add_file_to_teldrive(
            config,
            file_name=name,
            file_size=size,
            mime_type=file_info["mime_type"],
            channel_id=config.teldrive_channel_id,
            message_id=msg.id,
        )
        if result:
            logger.info(f"文件已添加到 TelDrive: {name}")
        else:
            logger.error(f"文件添加失败: {name}")

    async def authorize_with_dashboard(self, client: TelegramClient, config: RuntimeConfig):
        self.refresh_qr_event.clear()
        await broker.update_state(
            phase="awaiting_qr",
            authorized=False,
            needs_password=False,
            qr_image=None,
            qr_expires_at=None,
            last_error=None,
            **state_config_payload(config),
        )

        while not self.stop_event.is_set() and not self.reload_event.is_set():
            result = await client(
                ExportLoginTokenRequest(
                    api_id=config.telegram_api_id,
                    api_hash=config.telegram_api_hash,
                    except_ids=[],
                )
            )

            if await self._consume_login_result(client, result, config):
                return

            if not isinstance(result, auth.LoginToken):
                logger.warning("登录令牌返回异常，正在重试")
                await asyncio.sleep(2)
                continue

            token_b64 = base64.urlsafe_b64encode(result.token).decode("utf-8").rstrip("=")
            qr_image = build_qr_data_uri(f"tg://login?token={token_b64}")
            expires_at = result.expires.astimezone().isoformat(timespec="seconds")
            await broker.update_state(
                phase="awaiting_qr",
                authorized=False,
                needs_password=False,
                qr_image=qr_image,
                qr_expires_at=expires_at,
                last_error=None,
                **state_config_payload(config),
            )
            logger.info(f"已生成新的登录二维码，有效期至 {format_local_time(expires_at)}")

            while not self.stop_event.is_set() and not self.reload_event.is_set():
                if self.refresh_qr_event.is_set():
                    self.refresh_qr_event.clear()
                    logger.info("二维码已按管理员请求刷新")
                    break
                    
                if datetime.now(timezone.utc) >= result.expires:
                    logger.warning("登录二维码已过期，正在自动获取新码...")
                    break

                await asyncio.sleep(3)
                try:
                    poll_result = await client(
                        ExportLoginTokenRequest(
                            api_id=config.telegram_api_id,
                            api_hash=config.telegram_api_hash,
                            except_ids=[],
                        )
                    )
                    if await self._consume_login_result(client, poll_result, config):
                        return
                except SessionPasswordNeededError:
                    await self._complete_password_login(client, config)
                    return
                except Exception as exc:
                    message = str(exc)
                    if "SESSION_PASSWORD_NEEDED" in message:
                        await self._complete_password_login(client, config)
                        return
                    if "TOKEN_EXPIRED" in message:
                        logger.warning("登录二维码已过期，正在自动刷新")
                        break
                    raise

    async def _consume_login_result(self, client: TelegramClient, result: Any, config: RuntimeConfig) -> bool:
        if isinstance(result, auth.LoginTokenSuccess):
            await self._mark_authorized(config)
            return True
        if isinstance(result, auth.LoginTokenMigrateTo):
            await client._switch_dc(result.dc_id)
            migrated = await client(ImportLoginTokenRequest(token=result.token))
            if isinstance(migrated, auth.LoginTokenSuccess):
                await self._mark_authorized(config)
                return True
        return False

    async def _mark_authorized(self, config: RuntimeConfig):
        self.refresh_qr_event.clear()
        if self.password_future and not self.password_future.done():
            self.password_future.cancel()
        self.password_future = None
        await broker.update_state(
            phase="authorized",
            authorized=True,
            needs_password=False,
            qr_image=None,
            qr_expires_at=None,
            last_error=None,
            **state_config_payload(config),
        )
        logger.info("Telegram 登录成功")

    async def _complete_password_login(self, client: TelegramClient, config: RuntimeConfig):
        await broker.update_state(
            phase="awaiting_password",
            authorized=False,
            needs_password=True,
            qr_image=None,
            qr_expires_at=None,
            last_error=None,
            **state_config_payload(config),
        )
        logger.warning("账号启用了两步验证，请在管理页面输入密码")

        while not self.stop_event.is_set() and not self.reload_event.is_set():
            loop = asyncio.get_running_loop()
            self.password_future = loop.create_future()
            try:
                password = await self.password_future
            except asyncio.CancelledError:
                return
            finally:
                self.password_future = None

            try:
                pwd = await client(GetPasswordRequest())
                await client(CheckPasswordRequest(password=compute_check(pwd, password)))
                await self._mark_authorized(config)
                return
            except PasswordHashInvalidError:
                logger.error("两步验证密码错误，请重新输入")
                await broker.update_state(
                    phase="awaiting_password",
                    authorized=False,
                    needs_password=True,
                    last_error="两步验证密码错误，请重新输入",
                    **state_config_payload(config),
                )
            except Exception as exc:
                logger.error(f"两步验证登录失败: {exc}")
                await broker.update_state(
                    phase="awaiting_password",
                    authorized=False,
                    needs_password=True,
                    last_error=str(exc),
                    **state_config_payload(config),
                )


service = Tel2TelDriveService()


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(service.run_forever())
    try:
        yield
    finally:
        await service.stop()
        with suppress(asyncio.CancelledError):
            await task

# Legacy app.mount() removed to prevent static directory missing errors
