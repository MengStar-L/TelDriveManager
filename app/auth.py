"""认证模块 — 基于签名 Cookie 的会话管理"""

import base64
import hashlib
import hmac
import json
import secrets
import time

from app.config import load_config

TOKEN_MAX_AGE = 86400 * 7
TOKEN_VERSION = 1

# 兼容当前进程内已登录会话，同时支持手动登出后的撤销。
_active_tokens: set[str] = set()
_revoked_tokens: set[str] = set()


def _get_auth_config() -> dict:
    config = load_config()
    return config.get("auth", {})


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(f"{data}{padding}")


def _get_signing_key() -> bytes:
    auth = _get_auth_config()
    username = str(auth.get("username") or "")
    password = str(auth.get("password") or "")
    seed = f"TelDriveManager|auth|v{TOKEN_VERSION}|{username}\0{password}".encode("utf-8")
    return hashlib.sha256(seed).digest()


def _build_signed_token(username: str) -> str:
    payload = {
        "v": TOKEN_VERSION,
        "u": username,
        "iat": int(time.time()),
        "nonce": secrets.token_hex(8),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    payload_b64 = _b64encode(payload_bytes)
    sig = hmac.new(_get_signing_key(), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64encode(sig)}"


def _verify_signed_token(token: str) -> bool:
    try:
        payload_b64, signature_b64 = token.split(".", 1)
        expected_sig = hmac.new(_get_signing_key(), payload_b64.encode("utf-8"), hashlib.sha256).digest()
        actual_sig = _b64decode(signature_b64)
        if not hmac.compare_digest(actual_sig, expected_sig):
            return False

        payload = json.loads(_b64decode(payload_b64).decode("utf-8"))
        if int(payload.get("v") or 0) != TOKEN_VERSION:
            return False

        issued_at = int(payload.get("iat") or 0)
        if issued_at <= 0 or (time.time() - issued_at) > TOKEN_MAX_AGE:
            return False

        username = str(payload.get("u") or "")
        return username == str(_get_auth_config().get("username") or "")
    except Exception:
        return False



def is_auth_enabled() -> bool:
    """检查是否启用了认证"""
    auth = _get_auth_config()
    return bool(auth.get("username")) and bool(auth.get("password"))


def verify_credentials(username: str, password: str) -> bool:
    """验证用户名密码"""
    auth = _get_auth_config()
    return username == auth.get("username") and password == auth.get("password")


def create_token() -> str:
    """生成新的会话 token"""
    username = str(_get_auth_config().get("username") or "")
    token = _build_signed_token(username)
    _active_tokens.add(token)
    _revoked_tokens.discard(token)
    return token


def verify_token(token: str) -> bool:
    """验证 token 是否有效"""
    if not token or token in _revoked_tokens:
        return False
    return token in _active_tokens or _verify_signed_token(token)


def revoke_token(token: str) -> None:
    """撤销 token"""
    if not token:
        return
    _active_tokens.discard(token)
    _revoked_tokens.add(token)
