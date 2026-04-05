"""TelDriveManager — FastAPI 应用入口"""

import os
import sys
from pathlib import Path

# 将项目根目录注入环境变量，方便直接在编辑器环境运行 app/main.py
ROOT_DIR = Path(__file__).parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Cookie, FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from app.config import load_config
from app.auth import is_auth_enabled, verify_token
from app import database as db
from app.aria2_service import aria2_service
from app.modules.aria2teldrive.task_manager import task_manager

# 路由
from app.routes.login import router as login_router
from app.routes.settings import router as settings_router
from app.routes.ws import router as ws_router
from app.modules.pikpak.routes import router as pikpak_router
from app.modules.aria2teldrive.routes import router as a2td_router
from app.modules.tel2teldrive.routes import router as t2td_router

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    logger.info("TelDriveManager 启动中...")

    # 启动本地 aria2 托管服务
    try:
        await aria2_service.start()
    except Exception as e:
        logger.warning(f"本地 aria2 启动失败: {e}")

    # 启动 Aria2TelDrive 任务管理器
    await task_manager.start()

    # 启动 Tel2TelDrive 服务
    t2td_task = None
    try:
        from app.modules.tel2teldrive.service import service as t2td_service
        t2td_task = asyncio.create_task(t2td_service.run_forever())
        logger.info("Tel2TelDrive 服务已启动")
    except Exception as e:
        logger.warning(f"Tel2TelDrive 服务启动失败（可能缺少配置）: {e}")

    logger.info("TelDriveManager 启动完成")
    yield

    # 关闭
    logger.info("TelDriveManager 关闭中...")
    await task_manager.stop()
    await aria2_service.stop()

    if t2td_task:
        try:
            from app.modules.tel2teldrive.service import service as t2td_service
            await t2td_service.stop()
            t2td_task.cancel()
            try:
                await t2td_task
            except asyncio.CancelledError:
                pass
        except Exception:
            pass

    await db.close_db()
    logger.info("TelDriveManager 已关闭")


app = FastAPI(title="TelDriveManager", lifespan=lifespan)

# 认证中间件
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path

    # 放行：静态资源、登录、认证检查
    if (path in ("/login", "/api/login", "/api/auth/check")
            or path.startswith("/static/")
            or path == "/favicon.ico"):
        return await call_next(request)

    # 未启用认证则放行
    if not is_auth_enabled():
        return await call_next(request)

    # 检查 cookie token
    token = request.cookies.get("auth_token", "")
    if token and verify_token(token):
        return await call_next(request)

    # 未认证
    if path.startswith("/api/") or path == "/ws":
        return JSONResponse({"error": "未登录"}, status_code=401)
    return RedirectResponse(url="/login", status_code=303)


# 注册路由
app.include_router(login_router)
app.include_router(settings_router)
app.include_router(ws_router)
app.include_router(pikpak_router)
app.include_router(a2td_router)
app.include_router(t2td_router)


# 静态文件
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/login")
async def login_page(auth_token: str | None = Cookie(None)):
    if not is_auth_enabled():
        return RedirectResponse("/")
    if auth_token and verify_token(auth_token):
        return RedirectResponse("/")
    return FileResponse(STATIC_DIR / "login.html")

if __name__ == "__main__":
    import uvicorn
    # 为了保证相对路径和包引用的正确性，建议使用模块方式运行
    root_dir = Path(__file__).parent.parent
    reload_enabled = os.getenv("TELDRIVE_RELOAD", "0").strip().lower() in {"1", "true", "yes", "on"}
    if reload_enabled:
        uvicorn.run(
            "app.main:app",
            host="0.0.0.0",
            port=8888,
            reload=True,
            reload_dirs=[str(root_dir / "app")],
            reload_excludes=[
                "downloads/*",
                "*.db",
                "*.db-*",
                "*.log",
                "*.session",
                "history_*.md",
            ],
        )
    else:
        uvicorn.run("app.main:app", host="0.0.0.0", port=8888, reload=False)
