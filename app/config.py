"""统一配置管理 — 加载/保存/热重载 config.toml"""

import os
import sys
import copy
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.toml"
EXAMPLE_PATH = PROJECT_ROOT / "config.example.toml"
FIXED_DOWNLOAD_DIR = str((PROJECT_ROOT / "downloads").resolve())
FIXED_ARIA2_HOME = str((PROJECT_ROOT / "aria2").resolve())

# Python 3.11+ 内置 tomllib
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib

try:
    import tomli_w
except ImportError:
    tomli_w = None

# 内存缓存
_config_cache: dict | None = None

# 默认配置（作为 fallback）
DEFAULTS: dict[str, Any] = {
    "server": {"port": 8888},
    "auth": {"username": "", "password": ""},
    "pikpak": {
        "login_mode": "password", "username": "", "password": "", "session": "", "save_dir": "/",
        "delete_after_download": True, "poll_interval": 3, "max_wait_time": 3600,
        "share_parse_timeout": 45, "share_download_url_timeout": 60, "share_download_url_poll_interval": 3,
    },
    "aria2": {
        "managed": True,
        "installed": False,
        "os_type": "",
        "binary_path": "",
        "rpc_url": "http://127.0.0.1",
        "rpc_port": 6822,
        "rpc_secret": "",
        "allow_remote_access": False,
        "max_concurrent": 3,
        "split": 8,
        "max_connection_per_server": 8,
        "min_split_size_mb": 5,
        "disk_protection_threshold_gb": 5,
        "download_dir": FIXED_DOWNLOAD_DIR,
    },
    "teldrive": {
        "api_host": "", "access_token": "", "channel_id": 0,
        "chunk_size": "500M", "upload_concurrency": 4, "upload_dir": "",
        "random_chunk_name": True, "target_path": "/",
    },
    "upload": {
        "max_retries": 3, "auto_delete": True, "serial_transfer_mode": False,
    },
    "telegram": {
        "api_id": 0, "api_hash": "", "channel_id": 0,
        "session_name": "tel2teldrive_session",
        "sync_interval": 10, "sync_enabled": True,
        "max_scan_messages": 10000, "confirm_cycles": 3,
    },
    "telegram_db": {
        "host": "", "port": 5432, "user": "", "password": "", "name": "postgres",
    },
    "log": {"buffer_size": 400, "file": "runtime.log"},
}


def _deep_merge(base: dict, override: dict) -> dict:
    """深度合并：override 覆盖 base"""
    result = copy.deepcopy(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = copy.deepcopy(val)
    return result


def load_config(force_reload: bool = False) -> dict:
    """加载配置文件（带缓存）"""
    global _config_cache
    if _config_cache is not None and not force_reload:
        return _config_cache

    if not CONFIG_PATH.exists():
        # 如果没有 config.toml，从 example 复制
        if EXAMPLE_PATH.exists():
            import shutil
            shutil.copy2(EXAMPLE_PATH, CONFIG_PATH)
            logger.info(f"已从 {EXAMPLE_PATH} 创建默认配置文件")
        else:
            _config_cache = copy.deepcopy(DEFAULTS)
            return _config_cache

    with open(CONFIG_PATH, "rb") as f:
        raw = tomllib.load(f)

    # 环境变量覆盖（格式: TDM_SECTION_KEY，如 TDM_ARIA2_RPC_SECRET）
    for section, items in DEFAULTS.items():
        if not isinstance(items, dict):
            continue
        for key in items:
            env_key = f"TDM_{section.upper()}_{key.upper()}"
            env_val = os.environ.get(env_key)
            if env_val is not None:
                raw.setdefault(section, {})[key] = _cast_env(env_val, items[key])

    _config_cache = _normalize_config(_deep_merge(DEFAULTS, raw), raw)
    return _config_cache


def _cast_env(value: str, default: Any) -> Any:
    """根据默认值类型转换环境变量"""
    if isinstance(default, bool):
        return value.lower() in ("true", "1", "yes")
    if isinstance(default, int):
        try:
            return int(value)
        except ValueError:
            return default
    if isinstance(default, float):
        try:
            return float(value)
        except ValueError:
            return default
    return value


def _normalize_config(merged: dict, raw: dict | None = None) -> dict:
    raw = raw or {}
    pikpak_cfg = merged.setdefault("pikpak", {})
    aria2_cfg = merged.setdefault("aria2", {})
    upload_cfg = merged.setdefault("upload", {})
    log_cfg = merged.setdefault("log", {})

    raw_aria2 = raw.get("aria2") if isinstance(raw.get("aria2"), dict) else {}
    raw_pikpak = raw.get("pikpak") if isinstance(raw.get("pikpak"), dict) else {}

    pikpak_mode = str(pikpak_cfg.get("login_mode", "password") or "password").strip().lower()
    pikpak_cfg["login_mode"] = "token" if pikpak_mode in ("token", "session") else "password"
    pikpak_cfg["session"] = str(pikpak_cfg.get("session") or "").strip()

    legacy_max = raw_pikpak.get("max_concurrent_downloads", pikpak_cfg.get("max_concurrent_downloads", 3))
    legacy_conn = raw_pikpak.get("connections_per_task", pikpak_cfg.get("connections_per_task", 8))

    if "max_concurrent" not in raw_aria2:
        aria2_cfg["max_concurrent"] = int(legacy_max or aria2_cfg.get("max_concurrent") or 3)
    if "split" not in raw_aria2:
        aria2_cfg["split"] = int(legacy_conn or aria2_cfg.get("split") or 8)
    if "max_connection_per_server" not in raw_aria2:
        aria2_cfg["max_connection_per_server"] = int(legacy_conn or aria2_cfg.get("max_connection_per_server") or 8)

    aria2_cfg["managed"] = bool(aria2_cfg.get("managed", True))
    aria2_cfg["installed"] = bool(aria2_cfg.get("installed", False))
    aria2_cfg["download_dir"] = FIXED_DOWNLOAD_DIR
    aria2_cfg["rpc_url"] = "http://127.0.0.1"
    aria2_cfg["rpc_port"] = int(aria2_cfg.get("rpc_port") or 6822)
    aria2_cfg["rpc_secret"] = str(aria2_cfg.get("rpc_secret") or "").strip()
    aria2_cfg["allow_remote_access"] = bool(aria2_cfg.get("allow_remote_access", False))
    aria2_cfg["max_concurrent"] = max(1, int(aria2_cfg.get("max_concurrent") or 3))
    aria2_cfg["split"] = max(1, int(aria2_cfg.get("split") or 8))
    aria2_cfg["max_connection_per_server"] = max(1, int(aria2_cfg.get("max_connection_per_server") or 8))
    aria2_cfg["min_split_size_mb"] = max(1, int(aria2_cfg.get("min_split_size_mb") or 5))
    aria2_cfg["disk_protection_threshold_gb"] = max(1, int(aria2_cfg.get("disk_protection_threshold_gb") or 5))
    aria2_cfg["binary_path"] = str(aria2_cfg.get("binary_path") or "").strip()
    aria2_cfg["os_type"] = str(aria2_cfg.get("os_type") or "").strip().lower()

    for deprecated_key in ("download_engine", "max_concurrent_downloads", "connections_per_task"):
        pikpak_cfg.pop(deprecated_key, None)

    upload_cfg["max_retries"] = max(1, int(upload_cfg.get("max_retries") or 3))
    upload_cfg["auto_delete"] = bool(upload_cfg.get("auto_delete", True))
    upload_cfg["serial_transfer_mode"] = bool(upload_cfg.get("serial_transfer_mode", False))

    log_cfg["buffer_size"] = max(50, int(log_cfg.get("buffer_size") or 400))
    log_cfg["file"] = str(log_cfg.get("file") or "runtime.log").strip() or "runtime.log"

    return merged


def save_config(data: dict) -> None:
    """保存配置到 config.toml"""
    global _config_cache
    if tomli_w is None:
        raise RuntimeError("tomli_w 未安装，无法保存配置")
    
    # 增量合并：在现有配置的基础上覆盖
    current = load_config()
    sanitized = {
        key: value for key, value in (data or {}).items()
        if not str(key).startswith("_")
    }
    merged = _normalize_config(_deep_merge(current, sanitized), sanitized)

    upload_cfg = merged.get("upload")
    if isinstance(upload_cfg, dict):
        for deprecated_key in ("max_disk_usage", "cpu_limit", "max_disk_usage_gb", "cpu_usage_limit", "check_interval"):
            upload_cfg.pop(deprecated_key, None)

    with open(CONFIG_PATH, "wb") as f:
        tomli_w.dump(merged, f)
    _config_cache = merged
    logger.info("配置已保存到 config.toml")


def reload_config() -> dict:
    """强制重新加载配置"""
    return load_config(force_reload=True)


def get_section(section: str) -> dict:
    """获取某个配置段"""
    cfg = load_config()
    return cfg.get(section, {})


def needs_setup() -> bool:
    """判断是否为首次运行，或缺乏重要凭据"""
    if not CONFIG_PATH.exists():
        return True

    cfg = load_config()
    pikpak_cfg = cfg.get("pikpak", {})
    pikpak_mode = str(pikpak_cfg.get("login_mode", "password") or "password").strip().lower()
    has_pikpak_auth = bool(pikpak_cfg.get("session")) if pikpak_mode == "token" else (
        bool(pikpak_cfg.get("username")) and bool(pikpak_cfg.get("password"))
    )

    aria2_cfg = cfg.get("aria2", {})
    aria2_binary = str(aria2_cfg.get("binary_path", "")).strip()
    # 只检查配置是否填写完整，不检查二进制文件是否存在（那是运行时问题）
    has_aria2 = bool(aria2_cfg.get("installed")) and bool(aria2_binary)

    teldrive_cfg = cfg.get("teldrive", {})
    has_teldrive = (
        bool(str(teldrive_cfg.get("api_host") or "").strip())
        and bool(str(teldrive_cfg.get("access_token") or "").strip())
        and str(teldrive_cfg.get("channel_id", "")).strip() != ""
    )

    telegram_cfg = cfg.get("telegram", {})
    has_telegram = (
        bool(telegram_cfg.get("api_id"))
        and bool(str(telegram_cfg.get("api_hash") or "").strip())
        and str(telegram_cfg.get("channel_id", "")).strip() != ""
    )

    telegram_db_cfg = cfg.get("telegram_db", {})
    has_database = bool(str(telegram_db_cfg.get("host") or "").strip())

    # 如果核心凭证、aria2 或中转所需配置不完整，则需要弹窗引导配置
    if not has_pikpak_auth or not has_aria2 or not has_teldrive or not has_telegram or not has_database:
        return True

    return False
