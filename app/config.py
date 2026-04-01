"""统一配置管理 — 加载/保存/热重载 config.toml"""

import os
import sys
import copy
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
EXAMPLE_PATH = Path(__file__).parent.parent / "config.example.toml"

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
        "username": "", "password": "", "save_dir": "/",
        "delete_after_download": True, "poll_interval": 3, "max_wait_time": 3600,
    },
    "aria2": {
        "rpc_url": "http://localhost", "rpc_port": 6800, "rpc_secret": "",
        "max_concurrent": 3, "download_dir": "/downloads",
    },
    "teldrive": {
        "api_host": "", "access_token": "", "channel_id": 0,
        "chunk_size": "500M", "upload_concurrency": 4, "upload_dir": "",
        "random_chunk_name": True, "target_path": "/",
    },
    "upload": {
        "max_retries": 3, "auto_delete": True,
        "max_disk_usage": 0, "cpu_limit": 85,
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

    _config_cache = _deep_merge(DEFAULTS, raw)
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


def save_config(data: dict) -> None:
    """保存配置到 config.toml"""
    global _config_cache
    if tomli_w is None:
        raise RuntimeError("tomli_w 未安装，无法保存配置")
    
    # 增量合并：在现有配置的基础上覆盖
    current = load_config()
    merged = _deep_merge(current, data)
    
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
    pikpak_user = cfg.get("pikpak", {}).get("username", "")
    teldrive_token = cfg.get("teldrive", {}).get("access_token", "")
    telegram_hash = cfg.get("telegram", {}).get("api_hash", "")
    
    # 如果三个核心凭证全为空或不完整，则需要弹窗引导配置
    if not pikpak_user or not teldrive_token or not telegram_hash:
        return True
        
    return False
