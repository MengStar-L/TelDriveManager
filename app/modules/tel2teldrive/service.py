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

import httpx
import qrcode
import qrcode.image.svg
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request

from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from app import database as db
from fastapi.staticfiles import StaticFiles
from telethon import TelegramClient, events

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
HEALTH_CHECK_PROBE_LIMIT_BYTES = 20 * 1024
MIN_HEALTH_CHECK_PROBE_LIMIT_BYTES = 1 * 1024
MAX_HEALTH_CHECK_PROBE_LIMIT_BYTES = 10 * 1024 * 1024
HEALTH_CHECK_PROBE_CHUNK_BYTES = 4 * 1024
HEALTH_CHECK_SCOPE_MODE_ALL = "all"
HEALTH_CHECK_SCOPE_MODE_FOLDER = "folder"
HEALTH_CHECK_SCOPE_MODE_FILES = "files"
HEALTH_CHECK_SCOPE_MODES = {
    HEALTH_CHECK_SCOPE_MODE_ALL,
    HEALTH_CHECK_SCOPE_MODE_FOLDER,
    HEALTH_CHECK_SCOPE_MODE_FILES,
}



DEFAULT_CONFIG: dict[str, dict[str, Any]] = {
    "telegram": {
        "api_id": None,
        "api_hash": "",
        "channel_id": None,
        "session_name": "tel2teldrive_session",
        "sync_interval": 10,
        "sync_enabled": True,
        "max_scan_messages": 10000,
        "confirm_cycles": 3,
        "health_check_enabled": False,
        "health_check_interval_hours": 24,
    },
    "teldrive": {
        "api_host": "",
        "access_token": "",
        "channel_id": None,
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
    health_check_enabled: bool
    health_check_interval_hours: int
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

    @property
    def health_check_interval_seconds(self) -> int:
        return max(1, self.health_check_interval_hours) * 3600



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
            sync_interval=telegram["sync_interval"],
            sync_enabled=telegram["sync_enabled"],
            max_scan_messages=telegram["max_scan_messages"],
            confirm_cycles=telegram["confirm_cycles"],
            health_check_enabled=telegram["health_check_enabled"],
            health_check_interval_hours=telegram["health_check_interval_hours"],
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
                "sync_interval": runtime.sync_interval,
                "sync_enabled": runtime.sync_enabled,
                "max_scan_messages": runtime.max_scan_messages,
                "confirm_cycles": runtime.confirm_cycles,
                "health_check_enabled": runtime.health_check_enabled,
                "health_check_interval_hours": runtime.health_check_interval_hours,
            },
            "teldrive": {
                "api_host": runtime.teldrive_url,
                "access_token": runtime.bearer_token,
                "channel_id": "" if runtime.teldrive_channel_id is None else runtime.teldrive_channel_id,
            },
            "telegram_db": {
                "host": runtime.db_host,
                "port": runtime.db_port,
                "user": runtime.db_user,
                "password": runtime.db_password,
                "name": runtime.db_name,
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
        telegram["sync_interval"] = self._parse_positive_int(
            telegram_payload.get("sync_interval", teldrive_payload.get("sync_interval")),
            "删除同步轮询间隔",
            default=DEFAULT_CONFIG["telegram"]["sync_interval"],
            strict=strict,
        )
        telegram["sync_enabled"] = self._parse_bool(
            telegram_payload.get("sync_enabled", teldrive_payload.get("sync_enabled")),
            default=DEFAULT_CONFIG["telegram"]["sync_enabled"],
        )
        telegram["max_scan_messages"] = self._parse_positive_int(
            telegram_payload.get("max_scan_messages", teldrive_payload.get("max_scan_messages")),
            "历史扫描上限",
            default=DEFAULT_CONFIG["telegram"]["max_scan_messages"],
            strict=strict,
        )
        telegram["confirm_cycles"] = self._parse_positive_int(
            telegram_payload.get("confirm_cycles", teldrive_payload.get("confirm_cycles")),
            "确认周期",
            default=DEFAULT_CONFIG["telegram"]["confirm_cycles"],
            strict=strict,
        )
        telegram["health_check_enabled"] = self._parse_bool(
            telegram_payload.get("health_check_enabled"),
            default=DEFAULT_CONFIG["telegram"]["health_check_enabled"],
        )
        telegram["health_check_interval_hours"] = self._parse_positive_int(
            telegram_payload.get("health_check_interval_hours"),
            "文件巡检间隔",
            default=DEFAULT_CONFIG["telegram"]["health_check_interval_hours"],
            strict=strict,
        )

        teldrive = data["teldrive"]
        teldrive["api_host"] = self._parse_string(teldrive_payload.get("api_host"))
        teldrive["access_token"] = self._parse_string(teldrive_payload.get("access_token"))
        teldrive["channel_id"] = self._parse_optional_int(teldrive_payload.get("channel_id"), "TelDrive 频道 ID", strict=strict)

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


def format_health_check_probe_bytes(value: int) -> str:
    size = max(1, int(value or 0))
    units = ("B", "KB", "MB", "GB")
    amount = float(size)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(amount)}{unit}"
            if amount.is_integer():
                return f"{int(amount)}{unit}"
            return f"{amount:.1f}{unit}"
        amount /= 1024
    return f"{size}B"


def normalize_health_check_probe_bytes(value: Any) -> int:
    if value in (None, ""):
        return HEALTH_CHECK_PROBE_LIMIT_BYTES
    try:
        result = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("文件头读取大小必须是整数") from exc
    if result < MIN_HEALTH_CHECK_PROBE_LIMIT_BYTES or result > MAX_HEALTH_CHECK_PROBE_LIMIT_BYTES:
        min_label = format_health_check_probe_bytes(MIN_HEALTH_CHECK_PROBE_LIMIT_BYTES)
        max_label = format_health_check_probe_bytes(MAX_HEALTH_CHECK_PROBE_LIMIT_BYTES)
        raise ValueError(f"文件头读取大小必须在 {min_label} 到 {max_label} 之间")
    return result


def normalize_teldrive_path(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/")
    if not text:
        raise ValueError("TelDrive 路径不能为空")
    text = re.sub(r"/+", "/", text)
    if not text.startswith("/"):
        text = f"/{text}"
    if text != "/":
        text = text.rstrip("/")
    return text or "/"


def parse_health_check_scope_paths(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    raw_values: list[str] = []
    if isinstance(value, (list, tuple, set)):
        for item in value:
            text = str(item or "").strip()
            if text:
                raw_values.extend(re.split(r"[\r\n,，;；]+", text))
    else:
        raw_values.extend(re.split(r"[\r\n,，;；]+", str(value)))

    result: list[str] = []
    seen: set[str] = set()
    for item in raw_values:
        text = str(item or "").strip()
        if not text:
            continue
        normalized = normalize_teldrive_path(text)
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def format_health_check_scope_label(mode: str, paths: list[str] | None = None) -> str:
    normalized_mode = str(mode or HEALTH_CHECK_SCOPE_MODE_ALL).strip().lower()
    normalized_paths = [normalize_teldrive_path(path) for path in (paths or []) if str(path or "").strip()]
    if normalized_mode == HEALTH_CHECK_SCOPE_MODE_ALL or not normalized_paths:
        return "全部文件"
    kind = "目录" if normalized_mode == HEALTH_CHECK_SCOPE_MODE_FOLDER else "文件"
    if len(normalized_paths) == 1:
        suffix = "（含子目录）" if normalized_mode == HEALTH_CHECK_SCOPE_MODE_FOLDER else ""
        return f"{kind} {normalized_paths[0]}{suffix}"
    preview = "、".join(normalized_paths[:2])
    if len(normalized_paths) > 2:
        preview = f"{preview} 等 {len(normalized_paths)} 个"
    else:
        preview = f"{preview} 共 {len(normalized_paths)} 个"
    return f"{kind} {preview}"


def normalize_health_check_scope(mode: Any, paths: Any) -> tuple[str, list[str], str]:
    normalized_mode = str(mode or HEALTH_CHECK_SCOPE_MODE_ALL).strip().lower()
    if normalized_mode not in HEALTH_CHECK_SCOPE_MODES:
        raise ValueError("巡检对象类型无效")
    if normalized_mode == HEALTH_CHECK_SCOPE_MODE_ALL:
        return HEALTH_CHECK_SCOPE_MODE_ALL, [], "全部文件"

    normalized_paths = parse_health_check_scope_paths(paths)
    if not normalized_paths:
        target_name = "目录" if normalized_mode == HEALTH_CHECK_SCOPE_MODE_FOLDER else "文件"
        raise ValueError(f"请至少填写一个 TelDrive {target_name}路径")
    if normalized_mode == HEALTH_CHECK_SCOPE_MODE_FILES and any(path == "/" for path in normalized_paths):
        raise ValueError("文件巡检路径不能是根目录 /")
    if normalized_mode == HEALTH_CHECK_SCOPE_MODE_FOLDER and "/" in normalized_paths:
        return HEALTH_CHECK_SCOPE_MODE_ALL, [], "全部文件"
    return normalized_mode, normalized_paths, format_health_check_scope_label(normalized_mode, normalized_paths)


def file_path_in_health_scope(file_path: str, mode: str, paths: list[str]) -> bool:
    normalized_file_path = normalize_teldrive_path(file_path)
    if mode == HEALTH_CHECK_SCOPE_MODE_ALL:
        return True
    if mode == HEALTH_CHECK_SCOPE_MODE_FILES:
        return normalized_file_path in set(paths)
    for base_path in paths:
        if base_path == "/":
            return True
        if normalized_file_path == base_path or normalized_file_path.startswith(f"{base_path}/"):
            return True
    return False


def filter_health_check_items(
    items: list[tuple[str, dict[str, Any]]],
    scope_mode: str,
    scope_paths: list[str],
) -> list[tuple[str, dict[str, Any]]]:
    if scope_mode == HEALTH_CHECK_SCOPE_MODE_ALL:
        return items
    normalized_paths = [normalize_teldrive_path(path) for path in scope_paths]
    if scope_mode == HEALTH_CHECK_SCOPE_MODE_FILES:
        path_set = set(normalized_paths)
        return [
            (file_id, info)
            for file_id, info in items
            if normalize_teldrive_path(info.get("path") or f"/{info.get('name') or file_id}") in path_set
        ]
    return [
        (file_id, info)
        for file_id, info in items
        if file_path_in_health_scope(
            info.get("path") or f"/{info.get('name') or file_id}",
            HEALTH_CHECK_SCOPE_MODE_FOLDER,
            normalized_paths,
        )
    ]


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
        "health_check_enabled": config.health_check_enabled,
        "health_check_interval_hours": config.health_check_interval_hours,
        "log_file": config.log_file_path.name,
    }


def health_summary_state_payload(summary: dict[str, Any] | None = None) -> dict[str, Any]:
    summary = summary or {}
    return {
        "health_check_total_files": int(summary.get("total_files") or 0),
        "health_check_ok_count": int(summary.get("ok_count") or 0),
        "health_check_suspect_count": int(summary.get("suspect_count") or 0),
        "health_check_invalid_count": int(summary.get("invalid_count") or 0),
        "health_check_error_count": int(summary.get("error_count") or 0),
        "health_check_last_checked_at": summary.get("last_checked_at"),
        "health_check_last_ok_at": summary.get("last_ok_at"),
        "health_check_checked_files": int(summary.get("checked_files") or summary.get("total_files") or 0),
    }



async def build_health_snapshot(limit: int = 50) -> dict[str, Any]:
    return {
        "summary": await db.get_file_health_summary(),
        "issues": await db.get_file_health_issues(limit=limit),
    }



class DashboardBroker:
    def __init__(self, log_limit: int, config: RuntimeConfig):
        now = iso_now()
        self._log_limit = log_limit
        self._logs: dict[str, deque[dict[str, Any]]] = {
            "service": deque(maxlen=log_limit),
            "health": deque(maxlen=log_limit),
        }
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
            "health_check_running": False,
            "health_check_current_file": None,
            "health_check_trigger": None,
            "health_check_last_started_at": None,
            "health_check_last_finished_at": None,
            "health_check_last_error": None,
            "health_check_last_checked_at": None,
            "health_check_probe_bytes": HEALTH_CHECK_PROBE_LIMIT_BYTES,
            "health_check_scope_mode": HEALTH_CHECK_SCOPE_MODE_ALL,
            "health_check_scope_paths": [],
            "health_check_scope_label": "全部文件",
            **state_config_payload(config),


            **health_summary_state_payload(),
        }


    def snapshot(self) -> dict[str, Any]:
        return dict(self._state)

    def logs_snapshot(self, limit: int = 200, *, stream: str = "service") -> list[dict[str, Any]]:
        data = list(self._logs.get(stream, ()))
        return data[-limit:]

    async def update_state(self, **kwargs: Any):
        if "phase" in kwargs and "phase_label" not in kwargs:
            kwargs["phase_label"] = PHASE_LABELS.get(kwargs["phase"], str(kwargs["phase"]))
        self._state.update(kwargs)
        self._state["updated_at"] = iso_now()
        await self._broadcast({"type": "state", "payload": self.snapshot()})

    def push_log(self, entry: dict[str, Any], *, stream: str = "service"):
        normalized_stream = "health" if stream == "health" else "service"
        payload = {**entry, "stream": normalized_stream}
        buffer = self._logs.setdefault(normalized_stream, deque(maxlen=self._log_limit))
        buffer.append(payload)
        if normalized_stream == "service":
            self._state["log_count"] = int(self._state.get("log_count", 0)) + 1
            self._state["last_log_at"] = payload["timestamp"]
        self._schedule_broadcast({"type": "log", "payload": payload})

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

    def info(self, message: str, *, stream: str = "service"):
        self._write("INFO", message, stream=stream)

    def warning(self, message: str, *, stream: str = "service"):
        self._write("WARN", message, stream=stream)

    def error(self, message: str, *, stream: str = "service"):
        self._write("ERROR", message, stream=stream)

    def _write(self, level: str, message: str, *, stream: str = "service"): 
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
            },
            stream=stream,
        )


broker = DashboardBroker(INITIAL_RUNTIME.log_buffer_size, INITIAL_RUNTIME)
logger = ActivityLogger(broker, INITIAL_RUNTIME.log_file_path if INITIAL_RUNTIME.log_file else DEFAULT_LOG_FILE)


def load_mapping() -> dict[str, list[int]]:
    if MAPPING_PATH.exists():
        try:
            return json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_mapping(mapping: dict[str, list[int]]):
    MAPPING_PATH.write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


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


def build_teldrive_auth_headers(config: RuntimeConfig) -> dict[str, str]:
    token = str(config.bearer_token or "").strip()
    if not token:
        return {}
    return {
        "Authorization": f"Bearer {token}",
        "Cookie": f"access_token={token}",
    }


def build_teldrive_file_path(parent_path: str, name: str) -> str:
    base = (parent_path or "/").strip() or "/"
    if base == "/":
        return f"/{name}"
    return f"{base.rstrip('/')}/{name}"


def add_file_to_teldrive(

    config: RuntimeConfig,
    file_name: str,
    file_size: int,
    mime_type: str,
    channel_id: int,
    message_id: int,
) -> str | None:
    headers = {
        **build_teldrive_auth_headers(config),
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

    try:
        response = requests.post(f"{config.teldrive_url}/api/files", headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        file_id = data.get("id", "")
        if file_id:
            mapping = load_mapping()
            mapping[file_id] = [message_id]
            save_mapping(mapping)
        return file_id or None
    except requests.exceptions.HTTPError:
        logger.error(f"添加文件到 TelDrive 失败: HTTP {response.status_code} - {response.text}")
        return None
    except Exception as exc:
        logger.error(f"添加文件到 TelDrive 时出现异常: {exc}")
        return None


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
    headers = build_teldrive_auth_headers(config)
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
            item_size = int(item.get("size") or 0)
            item_path = build_teldrive_file_path(current_path, item_name)
            if item_type == "folder":
                dirs_to_scan.append(item_path)
            elif item_id:
                result[item_id] = {
                    "name": item_name,
                    "size": item_size,
                    "path": item_path,
                    "mime_type": item.get("mimeType") or item.get("mime_type") or "",
                }

    return result


async def list_teldrive_dir_async(
    config: RuntimeConfig,
    path: str,
    client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
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
            response = await client.get(f"{config.teldrive_url}/api/files", params=params)
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


async def get_teldrive_files_async(
    config: RuntimeConfig,
    client: httpx.AsyncClient,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    dirs_to_scan = ["/"]

    while dirs_to_scan:
        current_path = dirs_to_scan.pop()
        items = await list_teldrive_dir_async(config, current_path, client)
        for item in items:
            item_type = item.get("type", "")
            item_id = item.get("id", "")
            item_name = item.get("name", "")
            item_size = int(item.get("size") or 0)
            item_path = build_teldrive_file_path(current_path, item_name)
            if item_type == "folder":
                dirs_to_scan.append(item_path)
            elif item_id:
                result[item_id] = {
                    "name": item_name,
                    "size": item_size,
                    "path": item_path,
                    "mime_type": item.get("mimeType") or item.get("mime_type") or "",
                }

    return result


async def probe_teldrive_file_download_async(
    config: RuntimeConfig,
    file_id: str,
    expected_size: int = 0,
    *,
    client: httpx.AsyncClient,
    probe_limit_bytes: int = HEALTH_CHECK_PROBE_LIMIT_BYTES,
) -> dict[str, Any]:
    checked_at = iso_now()
    started = datetime.now(timezone.utc)
    url = f"{config.teldrive_url}/api/files/{file_id}/download"
    normalized_probe_limit = normalize_health_check_probe_bytes(probe_limit_bytes)
    headers = {"Range": f"bytes=0-{normalized_probe_limit - 1}"}
    bytes_read = 0
    chunk_count = 0
    status_code: int | None = None
    content_type = ""
    expected = max(0, int(expected_size or 0))
    target_bytes = min(expected, normalized_probe_limit) if expected > 0 else normalized_probe_limit


    try:
        async with client.stream("GET", url, headers=headers) as response:
            status_code = response.status_code
            content_type = response.headers.get("Content-Type", "")
            if response.status_code >= 400:
                message = (await response.aread()).decode("utf-8", errors="ignore")[:300].strip()
                raise RuntimeError(message or f"HTTP {response.status_code}")

            async for chunk in response.aiter_bytes(chunk_size=HEALTH_CHECK_PROBE_CHUNK_BYTES):
                if not chunk:
                    continue
                remaining = max(0, target_bytes - bytes_read)
                consumed = min(len(chunk), remaining)
                if consumed <= 0:
                    break
                bytes_read += consumed
                chunk_count += 1

                if bytes_read >= target_bytes:
                    break

        if bytes_read < target_bytes:
            raise RuntimeError(f"读取字节不足: expected={target_bytes}, actual={bytes_read}")

        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        return {
            "success": True,
            "checked_at": checked_at,
            "status_code": status_code,
            "bytes_read": bytes_read,
            "chunk_count": chunk_count,
            "target_bytes": target_bytes,
            "duration_ms": duration_ms,
            "content_type": content_type,
            "error": None,
        }
    except Exception as exc:
        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        return {
            "success": False,
            "checked_at": checked_at,
            "status_code": status_code,
            "bytes_read": bytes_read,
            "chunk_count": chunk_count,
            "target_bytes": target_bytes,
            "duration_ms": duration_ms,
            "content_type": content_type,
            "error": str(exc),
        }


def probe_teldrive_file_download(

    config: RuntimeConfig,
    file_id: str,
    expected_size: int = 0,
) -> dict[str, Any]:

    checked_at = iso_now()
    started = datetime.now(timezone.utc)
    url = f"{config.teldrive_url}/api/files/{file_id}/download"
    headers = {
        **build_teldrive_auth_headers(config),
        "Range": f"bytes=0-{HEALTH_CHECK_PROBE_LIMIT_BYTES - 1}",
    }
    bytes_read = 0
    chunk_count = 0
    status_code: int | None = None
    content_type = ""
    expected = max(0, int(expected_size or 0))
    target_bytes = min(expected, HEALTH_CHECK_PROBE_LIMIT_BYTES) if expected > 0 else HEALTH_CHECK_PROBE_LIMIT_BYTES


    try:
        with requests.get(url, headers=headers, stream=True, timeout=(10, 60)) as response:
            status_code = response.status_code
            content_type = response.headers.get("Content-Type", "")
            if response.status_code >= 400:
                message = response.text[:300].strip()
                raise RuntimeError(message or f"HTTP {response.status_code}")

            for chunk in response.iter_content(chunk_size=HEALTH_CHECK_PROBE_CHUNK_BYTES):
                if not chunk:
                    continue
                remaining = max(0, target_bytes - bytes_read)
                consumed = min(len(chunk), remaining)
                if consumed <= 0:
                    break
                bytes_read += consumed
                chunk_count += 1

                if bytes_read >= target_bytes:
                    break

        if bytes_read < target_bytes:
            raise RuntimeError(f"读取字节不足: expected={target_bytes}, actual={bytes_read}")

        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        return {
            "success": True,
            "checked_at": checked_at,
            "status_code": status_code,
            "bytes_read": bytes_read,
            "chunk_count": chunk_count,
            "target_bytes": target_bytes,
            "duration_ms": duration_ms,
            "content_type": content_type,
            "error": None,
        }
    except Exception as exc:
        duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        return {
            "success": False,
            "checked_at": checked_at,
            "status_code": status_code,
            "bytes_read": bytes_read,
            "chunk_count": chunk_count,
            "target_bytes": target_bytes,
            "duration_ms": duration_ms,
            "content_type": content_type,
            "error": str(exc),
        }




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


async def find_chunk_messages(client: TelegramClient, config: RuntimeConfig, base_names: list[str]) -> list[int]:

    chunk_ids: list[int] = []
    base_set = set(base_names)

    async for msg in client.iter_messages(config.telegram_channel_id, limit=config.max_scan_messages):
        try:
            file_info = extract_file_info(msg)
        except Exception:
            continue
        if file_info is None:
            continue
        name = file_info["name"]
        if is_chunk_file(name) and get_base_name(name) in base_set:
            chunk_ids.append(msg.id)
            logger.info(f"匹配到分片消息: {name} (msg_id={msg.id})")

    return chunk_ids


async def build_initial_mapping(client: TelegramClient, config: RuntimeConfig):
    logger.info("开始构建文件映射")

    if config.db_enabled:
        db_mapping = query_db_mapping(config)
        if db_mapping:
            save_mapping(db_mapping)
            logger.info(f"已从数据库构建映射: {len(db_mapping)} 条")
            return
        logger.warning("数据库未返回可用映射，回退到频道扫描")

    td_files = get_teldrive_files(config)
    mapping = load_mapping()
    unmapped_ids = {file_id for file_id in td_files if file_id not in mapping}

    stale_ids = [file_id for file_id in mapping if file_id not in td_files]
    if stale_ids:
        for file_id in stale_ids:
            mapping.pop(file_id, None)
        save_mapping(mapping)
        logger.info(f"已清理 {len(stale_ids)} 条过期映射")

    md5_ids = {file_id for file_id in unmapped_ids if is_md5_name(td_files[file_id]["name"])}
    if md5_ids:
        logger.info(f"已跳过 {len(md5_ids)} 个 MD5 分片条目")
        unmapped_ids -= md5_ids

    if not unmapped_ids:
        logger.info(f"文件映射已完整，总计 {len(mapping)} 条")
        return

    logger.info(f"待匹配文件 {len(unmapped_ids)} 个，开始扫描频道历史")
    name_to_file_id = {td_files[file_id]["name"]: file_id for file_id in unmapped_ids}
    found = 0
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
        if name in name_to_file_id:
            file_id = name_to_file_id.pop(name)
            mapping[file_id] = [msg.id]
            found += 1
            if not name_to_file_id:
                break
        if scanned % 200 == 0:
            save_mapping(mapping)
            logger.info(f"映射扫描进度: 已扫描 {scanned} 条消息，已匹配 {found} 个文件")

    save_mapping(mapping)
    logger.info(f"映射扫描完成: 扫描 {scanned} 条消息，新增 {found} 条映射，总计 {len(mapping)} 条")
    if name_to_file_id:
        logger.warning(f"仍有 {len(name_to_file_id)} 个 TelDrive 文件未找到对应消息")


async def sync_deletions(client: TelegramClient, config: RuntimeConfig):
    logger.info(f"删除同步已启动，轮询间隔 {config.sync_interval} 秒")
    prev_files = get_teldrive_files(config)
    prev_ids = set(prev_files.keys())
    logger.info(f"初始 TelDrive 快照共 {len(prev_ids)} 个文件")
    pending_deletions: dict[str, dict[str, Any]] = {}

    while True:
        await asyncio.sleep(config.sync_interval)
        curr_files = get_teldrive_files(config)
        curr_ids = set(curr_files.keys())
        curr_names = {info["name"] for info in curr_files.values()}
        disappeared_ids = prev_ids - curr_ids
        new_ids = curr_ids - prev_ids

        logger.info(
            f"同步检查: 上次 {len(prev_ids)} 个 -> 本次 {len(curr_ids)} 个 | 新增 {len(new_ids)} | 消失 {len(disappeared_ids)}"
        )

        mapping = load_mapping()

        if disappeared_ids:
            for file_id in disappeared_ids:
                old_info = prev_files.get(file_id, {})
                old_name = old_info.get("name", "") if isinstance(old_info, dict) else ""
                if old_name and old_name in curr_names:
                    new_name_to_id = {
                        info["name"]: new_id
                        for new_id, info in curr_files.items()
                        if new_id in new_ids
                    }
                    old_messages = mapping.pop(file_id, [])
                    if old_name in new_name_to_id:
                        new_file_id = new_name_to_id[old_name]
                        mapping[new_file_id] = old_messages
                        logger.info(f"检测到文件迁移，已迁移映射: {old_name}")
                    save_mapping(mapping)
                elif file_id not in pending_deletions:
                    if is_md5_name(old_name):
                        continue
                    pending_deletions[file_id] = {
                        "name": old_name,
                        "msg_ids": mapping.get(file_id, []),
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
                        mapping[new_id] = info["msg_ids"]
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
            base_names_to_delete: list[str] = []
            for file_id in confirmed_ids:
                info = pending_deletions.pop(file_id)
                msg_ids_to_delete.extend(info["msg_ids"])
                base_names_to_delete.append(info["name"])
                mapping.pop(file_id, None)

            if base_names_to_delete:
                chunk_msg_ids = await find_chunk_messages(client, config, base_names_to_delete)
                if chunk_msg_ids:
                    msg_ids_to_delete.extend(chunk_msg_ids)
                    logger.info(f"额外匹配到 {len(chunk_msg_ids)} 条分片消息，将一起删除")

            if msg_ids_to_delete:
                logger.warning(
                    f"确认删除 {len(confirmed_ids)} 个文件，准备清理 {len(msg_ids_to_delete)} 条频道消息"
                )
                try:
                    await client.delete_messages(config.telegram_channel_id, msg_ids_to_delete)
                    logger.info(f"已删除 {len(msg_ids_to_delete)} 条频道消息")
                except Exception as exc:
                    logger.error(f"删除频道消息失败: {exc}")
            save_mapping(mapping)

        if new_ids:
            mapping = load_mapping()
            unmapped_ids = [file_id for file_id in new_ids if file_id not in mapping]
            if unmapped_ids and config.db_enabled:
                db_mapping = query_db_mapping(config)
                updated = 0
                for file_id in unmapped_ids:
                    if file_id in db_mapping:
                        mapping[file_id] = db_mapping[file_id]
                        updated += 1
                if updated:
                    save_mapping(mapping)
                    logger.info(f"已从数据库同步 {updated} 个新增文件映射")
                remaining = len(unmapped_ids) - updated
                if remaining:
                    logger.warning(f"仍有 {remaining} 个新文件暂无数据库记录")
            elif unmapped_ids:
                logger.warning(f"发现 {len(unmapped_ids)} 个新增文件未建立映射 (未配置数据库)")

        prev_ids = curr_ids
        prev_files = curr_files


class Tel2TelDriveService:
    def __init__(self):
        self.client: TelegramClient | None = None
        self.sync_task: asyncio.Task[Any] | None = None
        self.health_task: asyncio.Task[Any] | None = None
        self.stop_event = asyncio.Event()
        self.reload_event = asyncio.Event()
        self.refresh_qr_event = asyncio.Event()
        self.health_trigger_event = asyncio.Event()
        self.password_future: asyncio.Future[str] | None = None
        self._pending_health_check_probe_bytes: int | None = None
        self._pending_health_check_scope_mode = HEALTH_CHECK_SCOPE_MODE_ALL
        self._pending_health_check_scope_paths: list[str] = []


    async def run_forever(self):

        logger.info("=" * 56)
        logger.info("Telegram 监听中转服务启动")
        logger.info("=" * 56)

        while not self.stop_event.is_set():
            config = config_store.runtime()
            logger.set_log_path(config.log_file_path)
            await broker.update_state(**state_config_payload(config))
            await self.refresh_health_state()

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
                    self.sync_task = asyncio.create_task(sync_deletions(self.client, config))
                else:
                    logger.info("删除同步已关闭 (sync_enabled = false)")

                self.health_task = asyncio.create_task(self._health_check_loop(config))

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
        self.health_trigger_event.set()
        self._pending_health_check_probe_bytes = None
        self._pending_health_check_scope_mode = HEALTH_CHECK_SCOPE_MODE_ALL
        self._pending_health_check_scope_paths = []

        if self.password_future and not self.password_future.done():

            self.password_future.cancel()
        if self.sync_task and not self.sync_task.done():
            self.sync_task.cancel()
            try:
                await self.sync_task
            except (asyncio.CancelledError, Exception):
                pass
        if self.health_task and not self.health_task.done():
            self.health_task.cancel()
            try:
                await self.health_task
            except (asyncio.CancelledError, Exception):
                pass
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass

    async def request_reload(self):
        self.reload_event.set()
        self.refresh_qr_event.set()
        self.health_trigger_event.set()
        self._pending_health_check_probe_bytes = None
        self._pending_health_check_scope_mode = HEALTH_CHECK_SCOPE_MODE_ALL
        self._pending_health_check_scope_paths = []

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

    async def request_health_check(
        self,
        probe_bytes: Any = None,
        scope_mode: Any = None,
        scope_paths: Any = None,
    ) -> dict[str, Any]:
        if not self.client or not self.client.is_connected() or not broker.snapshot().get("authorized"):
            raise RuntimeError("Telegram 服务尚未就绪，暂时无法执行文件巡检")
        normalized_probe_bytes = normalize_health_check_probe_bytes(probe_bytes)
        normalized_scope_mode, normalized_scope_paths, scope_label = normalize_health_check_scope(scope_mode, scope_paths)
        probe_label = format_health_check_probe_bytes(normalized_probe_bytes)
        already_running = bool(broker.snapshot().get("health_check_running"))
        if already_running:
            return {
                "accepted": False,
                "already_running": True,
                "probe_bytes": normalized_probe_bytes,
                "scope_mode": normalized_scope_mode,
                "scope_paths": normalized_scope_paths,
                "scope_label": scope_label,
                "message": "文件巡检已在运行中",
            }
        self._pending_health_check_probe_bytes = normalized_probe_bytes
        self._pending_health_check_scope_mode = normalized_scope_mode
        self._pending_health_check_scope_paths = list(normalized_scope_paths)
        await broker.update_state(
            health_check_probe_bytes=normalized_probe_bytes,
            health_check_scope_mode=normalized_scope_mode,
            health_check_scope_paths=normalized_scope_paths,
            health_check_scope_label=scope_label,
        )
        self.health_trigger_event.set()
        return {
            "accepted": True,
            "already_running": False,
            "probe_bytes": normalized_probe_bytes,
            "scope_mode": normalized_scope_mode,
            "scope_paths": normalized_scope_paths,
            "scope_label": scope_label,
            "message": f"已开始巡检：{scope_label}（每个文件仅读取前 {probe_label}）",
        }


    async def get_health_snapshot(self, limit: int = 50) -> dict[str, Any]:

        return await build_health_snapshot(limit=limit)

    async def refresh_health_state(self):
        summary = await db.get_file_health_summary()
        await broker.update_state(**health_summary_state_payload(summary))

    async def submit_password(self, password: str):
        if not password:
            raise RuntimeError("两步验证密码不能为空")
        if not self.password_future or self.password_future.done():
            raise RuntimeError("当前无需输入两步验证密码")
        self.password_future.set_result(password)
        logger.info("已收到管理员提交的两步验证密码")

    async def _health_check_loop(self, config: RuntimeConfig):
        try:
            while not self.stop_event.is_set() and not self.reload_event.is_set():
                trigger = await self._wait_for_health_signal(
                    timeout=config.health_check_interval_seconds if config.health_check_enabled else None
                )
                if trigger in {"stop", "reload", None}:
                    if trigger in {"stop", "reload"}:
                        break
                    continue
                await self._run_health_check(config, trigger=trigger)
        except asyncio.CancelledError:
            raise

    async def _run_health_check(self, config: RuntimeConfig, *, trigger: str):
        run_id = secrets.token_hex(8)
        started_at = iso_now()
        probe_limit_bytes = (
            self._pending_health_check_probe_bytes
            if trigger == "manual" and self._pending_health_check_probe_bytes
            else HEALTH_CHECK_PROBE_LIMIT_BYTES
        )
        scope_mode = self._pending_health_check_scope_mode if trigger == "manual" else HEALTH_CHECK_SCOPE_MODE_ALL
        scope_paths = list(self._pending_health_check_scope_paths) if trigger == "manual" else []
        self._pending_health_check_probe_bytes = None
        self._pending_health_check_scope_mode = HEALTH_CHECK_SCOPE_MODE_ALL
        self._pending_health_check_scope_paths = []
        probe_label = format_health_check_probe_bytes(probe_limit_bytes)
        scope_label = format_health_check_scope_label(scope_mode, scope_paths)
        full_scan = scope_mode == HEALTH_CHECK_SCOPE_MODE_ALL
        await broker.update_state(
            health_check_running=True,
            health_check_trigger=trigger,
            health_check_last_started_at=started_at,
            health_check_last_finished_at=None,
            health_check_last_error=None,
            health_check_current_file="正在获取 TelDrive 文件清单...",
            health_check_probe_bytes=probe_limit_bytes,
            health_check_scope_mode=scope_mode,
            health_check_scope_paths=scope_paths,
            health_check_scope_label=scope_label,
        )
        logger.info(
            f"开始执行文件巡检（触发方式: {trigger}）| 巡检对象: {scope_label} | 每个文件仅读取前 {probe_label}",
            stream="health",
        )



        try:

            timeout = httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=10.0)
            async with httpx.AsyncClient(
                headers=build_teldrive_auth_headers(config),
                follow_redirects=True,
                timeout=timeout,
            ) as health_client:
                files = await get_teldrive_files_async(config, health_client)
                previous_rows = await db.get_all_file_health_checks()
                previous_by_id = {row.get("file_id"): row for row in previous_rows if row.get("file_id")}
                telegram_part_mapping = load_mapping()
                if config.db_enabled:
                    db_mapping = await asyncio.to_thread(query_db_mapping, config)
                    if db_mapping:
                        telegram_part_mapping.update(db_mapping)
                all_items = sorted(files.items(), key=lambda item: item[1].get("path") or item[1].get("name") or "")
                items = filter_health_check_items(all_items, scope_mode, scope_paths)

                total = len(items)
                checked = 0
                ok_count = 0
                suspect_count = 0
                invalid_count = 0
                error_count = 0

                logger.info(f"本次巡检对象 {scope_label} 共匹配到 {total} 个文件", stream="health")
                await broker.update_state(
                    health_check_total_files=total,
                    health_check_checked_files=0,
                    health_check_ok_count=0,
                    health_check_suspect_count=0,
                    health_check_invalid_count=0,
                    health_check_error_count=0,
                    health_check_probe_bytes=probe_limit_bytes,
                    health_check_current_file=f"正在执行 {scope_label} 的文件头读取探测..." if total else f"{scope_label} 范围内没有可巡检文件",
                )



                for file_id, info in items:
                    if self.stop_event.is_set() or self.reload_event.is_set():
                        raise asyncio.CancelledError()

                    file_name = str(info.get("name") or file_id)
                    file_path = str(info.get("path") or f"/{file_name}")
                    file_size = int(info.get("size") or 0)
                    probe_target_bytes = min(max(file_size, 0), probe_limit_bytes) if file_size > 0 else probe_limit_bytes

                    telegram_part_count = len(telegram_part_mapping.get(file_id) or [])
                    telegram_chunk_mark = f" [Telegram 分片 {telegram_part_count} 块]" if telegram_part_count > 1 else ""
                    await broker.update_state(
                        health_check_current_file=f"{file_path} ({checked + 1}/{max(total, 1)}){telegram_chunk_mark}"
                    )
                    logger.info(
                        f"开始巡检文件 {checked + 1}/{max(total, 1)}: {file_path}{telegram_chunk_mark} | 仅读取前 {probe_target_bytes} 字节",
                        stream="health",
                    )

                    probe_result = await probe_teldrive_file_download_async(
                        config,
                        file_id,
                        file_size,
                        client=health_client,
                        probe_limit_bytes=probe_limit_bytes,
                    )


                    checked_at = probe_result.get("checked_at") or iso_now()
                    previous = previous_by_id.get(file_id) or {}

                    if probe_result.get("success"):
                        status = "ok"
                        consecutive_failures = 0
                        last_ok_at = checked_at
                        last_error = None
                        ok_count += 1
                    else:
                        status_code = probe_result.get("status_code")
                        if status_code in {401, 403}:
                            raise RuntimeError(f"TelDrive 下载鉴权失败：HTTP {status_code}")
                        consecutive_failures = int(previous.get("consecutive_failures") or 0) + 1
                        status = "invalid" if consecutive_failures >= config.confirm_cycles else "suspect"
                        last_ok_at = previous.get("last_ok_at")
                        last_error = probe_result.get("error") or "未知错误"
                        if status == "invalid":
                            invalid_count += 1
                        else:
                            suspect_count += 1
                        logger.warning(
                            f"巡检失败: {file_path}{telegram_chunk_mark} -> {last_error} ({consecutive_failures}/{config.confirm_cycles})",
                            stream="health",
                        )

                    await db.upsert_file_health_check(
                        file_id=file_id,
                        file_name=file_name,
                        file_path=file_path,
                        file_size=file_size,
                        status=status,
                        consecutive_failures=consecutive_failures,
                        last_checked_at=checked_at,
                        last_ok_at=last_ok_at,
                        last_error=last_error,
                        last_run_id=run_id,
                        last_probe_status=probe_result.get("status_code"),
                        last_probe_bytes=int(probe_result.get("bytes_read") or 0),
                        last_probe_duration_ms=int(probe_result.get("duration_ms") or 0),
                        last_probe_content_type=probe_result.get("content_type"),
                    )

                    checked += 1
                    await broker.update_state(
                        health_check_checked_files=checked,
                        health_check_total_files=total,
                        health_check_ok_count=ok_count,
                        health_check_suspect_count=suspect_count,
                        health_check_invalid_count=invalid_count,
                        health_check_error_count=error_count,
                    )
                    if checked == total or checked % 20 == 0:
                        logger.info(
                            f"巡检进度: {checked}/{total} | 正常 {ok_count} | 可疑 {suspect_count} | 失效 {invalid_count}",
                            stream="health",
                        )

            if full_scan:
                await db.delete_stale_file_health_checks(run_id)
            summary = await db.get_file_health_summary()
            finished_at = iso_now()
            final_state = {
                "health_check_running": False,
                "health_check_current_file": None,
                "health_check_last_finished_at": finished_at,
                "health_check_last_checked_at": finished_at,
                "health_check_last_error": None,
                "health_check_scope_mode": scope_mode,
                "health_check_scope_paths": scope_paths,
                "health_check_scope_label": scope_label,
            }
            if full_scan:
                final_state.update(health_summary_state_payload(summary))
            else:
                final_state.update(
                    {
                        "health_check_total_files": total,
                        "health_check_checked_files": checked,
                        "health_check_ok_count": ok_count,
                        "health_check_suspect_count": suspect_count,
                        "health_check_invalid_count": invalid_count,
                        "health_check_error_count": error_count,
                    }
                )
            await broker.update_state(**final_state)

            logger.info(
                f"文件巡检完成: 巡检对象 {scope_label} | 共 {checked} 个文件 | 正常 {ok_count} | "
                f"可疑 {suspect_count} | 失效 {invalid_count}",
                stream="health",
            )


        except asyncio.CancelledError:
            await broker.update_state(
                health_check_running=False,
                health_check_current_file=None,
                health_check_last_finished_at=iso_now(),
            )
            raise
        except Exception as exc:
            logger.error(f"文件巡检异常: {exc}", stream="health")
            await broker.update_state(
                health_check_running=False,
                health_check_current_file=None,
                health_check_last_finished_at=iso_now(),
                health_check_last_error=str(exc),
            )

    async def _wait_for_health_signal(self, timeout: float | None = None) -> str | None:
        stop_task = asyncio.create_task(self.stop_event.wait())
        reload_task = asyncio.create_task(self.reload_event.wait())
        trigger_task = asyncio.create_task(self.health_trigger_event.wait())
        try:
            done, pending = await asyncio.wait(
                {stop_task, reload_task, trigger_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            pass

        if not done:
            for task in (stop_task, reload_task, trigger_task):
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            return "auto" if timeout is not None else None

        for task in pending:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        if stop_task in done and stop_task.result():
            return "stop"
        if reload_task in done and reload_task.result():
            return "reload"
        if trigger_task in done and trigger_task.result():
            self.health_trigger_event.clear()
            return "manual"
        return None

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

        if self.health_task:
            self.health_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.health_task
            self.health_task = None

        self.health_trigger_event.clear()
        self._pending_health_check_probe_bytes = None
        self._pending_health_check_scope_mode = HEALTH_CHECK_SCOPE_MODE_ALL
        self._pending_health_check_scope_paths = []

        if self.client:


            with suppress(Exception):
                if self.client.is_connected():
                    await self.client.disconnect()
            self.client = None


    def register_handlers(self, client: TelegramClient, config: RuntimeConfig):
        @client.on(events.NewMessage(chats=config.telegram_channel_id))
        async def on_new_message(event: Any):
            await self.handle_new_message(client, config, event.message)

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
        for file_id, msg_ids in mapping.items():
            info = td_files.get(file_id)
            file_name = info["name"] if info else ""
            if file_name:
                mapped_names.add(file_name)

        if name in mapped_names:
            logger.warning(f"检测到重复消息，准备删除: {name} (msg_id={msg.id})")
            try:
                await client.delete_messages(config.telegram_channel_id, [msg.id])
                logger.info(f"重复消息已删除: {name} (msg_id={msg.id})")
            except Exception as exc:
                logger.error(f"删除重复消息失败: {exc}")
            return

        existing_name_to_id = {info["name"]: file_id for file_id, info in td_files.items()}
        if name in existing_name_to_id:
            file_id = existing_name_to_id[name]
            mapping[file_id] = [msg.id]
            save_mapping(mapping)
            logger.info(f"TelDrive 已存在该文件，仅补充映射: {name}")
            return

        result = add_file_to_teldrive(
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
