"""登录路由 — 统一认证接口"""

from fastapi import APIRouter, Cookie
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from app.auth import (
    is_auth_enabled, verify_credentials,
    create_token, verify_token, revoke_token,
)

router = APIRouter(prefix="/api")


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(req: LoginRequest):
    if not is_auth_enabled():
        return {"success": True, "message": "认证未启用"}
    if not verify_credentials(req.username, req.password):
        return {"success": False, "message": "用户名或密码错误"}
    token = create_token()
    response = JSONResponse({"success": True, "token": token})
    response.set_cookie(
        key="auth_token", value=token,
        httponly=True, samesite="lax", max_age=86400 * 7,
    )
    return response


@router.post("/logout")
async def logout(auth_token: Optional[str] = Cookie(None)):
    if auth_token:
        revoke_token(auth_token)
    response = JSONResponse({"success": True})
    response.delete_cookie("auth_token")
    return response


@router.get("/auth/check")
async def auth_check(auth_token: Optional[str] = Cookie(None)):
    if not is_auth_enabled():
        return {"authenticated": True, "auth_enabled": False}
    if auth_token and verify_token(auth_token):
        return {"authenticated": True, "auth_enabled": True}
    return {"authenticated": False, "auth_enabled": True}
