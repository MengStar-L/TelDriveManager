"""统一设置路由 — 管理所有模块的配置、aria2 安装与连接测试"""

import uuid
from pathlib import Path

from fastapi import APIRouter, Body, File, Form, UploadFile

from app.aria2_client import Aria2Client
from app.aria2_service import ARIA2_TMP_DIR, aria2_service
from app.config import load_config, needs_setup, reload_config, save_config
from app import database as db
from app.modules.aria2teldrive.task_manager import task_manager
from app.modules.aria2teldrive.teldrive_client import TelDriveClient
from app.modules.pikpak import routes as pikpak_routes

router = APIRouter(prefix="/api/settings")


def _sanitize_payload(payload: dict | None) -> dict:
    payload = payload or {}
    return {
        key: value for key, value in payload.items()
        if not str(key).startswith("_")
    }


def _has_channel_id(value) -> bool:
    return str(value).strip() not in ("", "None")


@router.get("")

async def get_settings():
    config = load_config()
    data = dict(config)
    data["_meta"] = {"needs_setup": needs_setup()}
    return data


@router.put("")
async def update_settings(request_body: dict):
    previous = load_config(force_reload=True)
    save_config(_sanitize_payload(request_body))
    current = reload_config()
    await aria2_service.handle_config_update(previous, current)
    await task_manager.reload_config()
    await pikpak_routes.reset_clients()
    await db.prune_progress_logs(current.get("log", {}).get("buffer_size", 400), stream="pikpak")
    # 通知 tel2teldrive 服务重新加载配置
    try:
        from app.modules.tel2teldrive.service import config_store as t2td_config_store, service as t2td_service
        t2td_config_store.reload()
        await t2td_service.request_reload()
    except Exception:
        pass  # 模块未初始化时静默忽略
    return {"success": True, "message": "设置已保存"}




@router.get("/aria2/runtime")
async def get_aria2_runtime():
    return await aria2_service.get_runtime_status()


@router.get("/aria2/install/status")
async def get_aria2_install_status():
    return await aria2_service.get_runtime_status()


@router.post("/aria2/install/auto")
async def install_aria2_auto(payload: dict = Body(...)):
    os_type = str((payload or {}).get("os_type") or "").strip().lower()
    return await aria2_service.begin_auto_install(os_type)


@router.post("/aria2/install/upload")
async def install_aria2_upload(
    os_type: str = Form(...),
    archive: UploadFile = File(...),
):
    suffixes = "".join(Path(archive.filename or "aria2-package").suffixes)
    temp_name = f"aria2-upload-{uuid.uuid4().hex}{suffixes}"
    ARIA2_TMP_DIR.mkdir(parents=True, exist_ok=True)
    temp_path = ARIA2_TMP_DIR / temp_name
    with open(temp_path, "wb") as fh:
        while True:
            chunk = await archive.read(1024 * 1024)
            if not chunk:
                break
            fh.write(chunk)
    await archive.close()
    return await aria2_service.begin_uploaded_install(temp_path, os_type, archive.filename or temp_name)


async def _test_aria2_connection(payload: dict | None = None):
    cfg = load_config(force_reload=True)
    aria2_cfg = dict(cfg.get("aria2", {}))
    if payload:
        aria2_cfg.update(payload)

    if aria2_cfg.get("installed") and not aria2_service.is_running():
        try:
            await aria2_service.start()
        except Exception:
            pass

    client = Aria2Client(
        rpc_url=aria2_cfg.get("rpc_url", "http://127.0.0.1"),
        rpc_port=aria2_cfg.get("rpc_port", 6800),
        rpc_secret=aria2_cfg.get("rpc_secret", ""),
    )
    try:
        return await client.test_connection()
    finally:
        await client.close()


@router.post("/test/aria2")
async def test_aria2(payload: dict | None = Body(None)):
    return await _test_aria2_connection(payload)



@router.post("/test/teldrive")
async def test_teldrive(payload: dict = Body(None)):
    cfg = payload or load_config()["teldrive"]
    client = TelDriveClient(
        api_host=cfg.get("api_host", ""),
        access_token=cfg.get("access_token", ""),
    )
    return await client.test_connection()


@router.post("/test/pikpak")
async def test_pikpak(payload: dict = Body(None)):
    from app.modules.pikpak.client import PikPakClient

    cfg = payload or load_config()["pikpak"]
    login_mode = str(cfg.get("login_mode", "password") or "password").strip().lower()
    login_mode = "token" if login_mode in ("token", "session") else "password"
    if login_mode == "token":
        session = (cfg.get("session") or "").strip()
        if not session:
            return {"success": False, "message": "PikPak Encoded Token 不能为空"}
        username = ""
        password = ""
    else:
        username = (cfg.get("username") or "").strip()
        password = cfg.get("password", "")
        session = ""
        if not username or not password:
            return {"success": False, "message": "PikPak 账号密码不能为空"}

    client = None
    try:
        client = PikPakClient(
            username=username,
            password=password,
            session=session,
            login_mode=login_mode,
            save_dir=cfg.get("save_dir", "/"),
        )
        await client.login()
        mode_text = "Token" if login_mode == "token" else "账号密码"
        return {"success": True, "message": f"PikPak {mode_text} 登录成功"}

    except Exception as e:
        return {"success": False, "message": f"PikPak 连接失败: {str(e)}"}
    finally:
        if client is not None:
            await client.close()



@router.post("/test/telegram")
async def test_telegram(payload: dict = Body(None)):
    if payload:
        api_id = payload.get("api_id")
        api_hash = payload.get("api_hash")
        if not api_id or not api_hash:
            return {"success": False, "message": "Telegram API ID 和 Hash 不能为空"}
        try:
            from telethon import TelegramClient
            from telethon.sessions import MemorySession

            temp_client = TelegramClient(MemorySession(), api_id=api_id, api_hash=api_hash)
            await temp_client.connect()
            connected = temp_client.is_connected()
            await temp_client.disconnect()
            if connected:
                return {"success": True, "message": "Telegram 握手成功"}
            return {"success": False, "message": "Telegram 连接失败"}
        except Exception as e:
            return {"success": False, "message": f"Telegram 验证失败: {str(e)}"}

    from app.modules.tel2teldrive.service import broker

    state = broker.snapshot()
    if state.get("authorized"):
        return {"success": True, "message": "Telegram 连接正常并已授权"}
    if state.get("phase") in ("awaiting_qr", "awaiting_password"):
        return {"success": True, "message": "正常：等待扫码验证"}
    return {"success": False, "message": f"连接异常: {state.get('last_error', '服务未激活')}"}


@router.post("/test/database")
async def test_database(payload: dict = Body(None)):
    if payload:
        host = payload.get("host", "")
        port = payload.get("port", 5432)
        user = payload.get("user", "")
        password = payload.get("password", "")
        dbname = payload.get("name", "postgres")
    else:
        cfg = load_config()
        db_cfg = cfg.get("telegram_db", {})
        host = db_cfg.get("host", "")
        port = db_cfg.get("port", 5432)
        user = db_cfg.get("user", "")
        password = db_cfg.get("password", "")
        dbname = db_cfg.get("name", "postgres")

    if not host:
        return {"success": False, "message": "未配置数据库主机地址"}

    try:
        import psycopg2

        conn = psycopg2.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=10,
        )
        conn.close()
        return {"success": True, "message": "数据库连接成功"}
    except ImportError:
        return {"success": False, "message": "psycopg2 未安装，无法测试数据库连接"}
    except Exception as e:
        return {"success": False, "message": f"数据库连接失败: {str(e)}"}


@router.get("/health")
async def global_health_check():
    statuses = {
        "pikpak": False,
        "aria2": False,
        "teldrive": False,
        "telegram": False,
        "database": False,
    }
    try:
        cfg = load_config(force_reload=True)
        pikpak_cfg = cfg.get("pikpak", {})
        pikpak_mode = str(pikpak_cfg.get("login_mode", "password") or "password").strip().lower()
        statuses["pikpak"] = bool(pikpak_cfg.get("session")) if pikpak_mode == "token" else (
            bool(pikpak_cfg.get("username")) and bool(pikpak_cfg.get("password"))
        )


        try:
            if cfg.get("aria2", {}).get("installed") and not aria2_service.is_running():
                await aria2_service.start()
        except Exception:
            pass

        aria2_result = await _test_aria2_connection()
        statuses["aria2"] = bool(aria2_result.get("success"))

        teldrive_cfg = cfg.get("teldrive", {})
        statuses["teldrive"] = (
            bool(str(teldrive_cfg.get("api_host") or "").strip())
            and bool(str(teldrive_cfg.get("access_token") or "").strip())
            and _has_channel_id(teldrive_cfg.get("channel_id"))
        )

        tg_cfg = cfg.get("telegram", {})
        telegram_base_ready = (
            bool(tg_cfg.get("api_id"))
            and bool(str(tg_cfg.get("api_hash") or "").strip())
            and _has_channel_id(tg_cfg.get("channel_id"))
        )
        try:
            from app.modules.tel2teldrive.service import broker

            tg_state = broker.snapshot()
            tg_phase = tg_state.get("phase", "")
            # 这些阶段都属于正常运行范围，不应报错
            healthy_phases = ("running", "initializing", "connecting", "reconnecting", "awaiting_qr", "awaiting_password")
            statuses["telegram"] = telegram_base_ready and bool(
                tg_state.get("authorized")
                or tg_phase in healthy_phases
            )
        except Exception:
            statuses["telegram"] = telegram_base_ready

        db_cfg = cfg.get("telegram_db", {})
        statuses["database"] = bool(str(db_cfg.get("host") or "").strip())

    except Exception as e:
        import logging

        logging.getLogger(__name__).warning(f"健康检查异常: {e}")

    is_healthy = all(statuses.values())
    err_modules = [k for k, v in statuses.items() if not v]
    msg = "所有系统服务连接正常" if is_healthy else f"存在异常服务: {', '.join(err_modules)}"
    return {"healthy": is_healthy, "message": msg, "details": statuses}
