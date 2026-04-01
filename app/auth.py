"""认证模块 — 简单的 Token 会话管理"""

import secrets
from app.config import load_config

# 内存中的活跃 token 集合，重启后需重新登录
_active_tokens: set[str] = set()


def is_auth_enabled() -> bool:
    """检查是否启用了认证"""
    config = load_config()
    auth = config.get("auth", {})
    return bool(auth.get("username")) and bool(auth.get("password"))


def verify_credentials(username: str, password: str) -> bool:
    """验证用户名密码"""
    config = load_config()
    auth = config.get("auth", {})
    return username == auth.get("username") and password == auth.get("password")


def create_token() -> str:
    """生成新的会话 token"""
    token = secrets.token_hex(32)
    _active_tokens.add(token)
    return token


def verify_token(token: str) -> bool:
    """验证 token 是否有效"""
    return token in _active_tokens


def revoke_token(token: str) -> None:
    """撤销 token"""
    _active_tokens.discard(token)
