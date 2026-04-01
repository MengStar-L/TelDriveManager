"""统一设置路由 — 管理所有模块的配置和连接测试"""

from fastapi import APIRouter, Body
from app.config import load_config, save_config, reload_config
from app.aria2_client import Aria2Client
from app.modules.aria2teldrive.teldrive_client import TelDriveClient
from app.modules.aria2teldrive.task_manager import task_manager
from app.modules.pikpak import routes as pikpak_routes

router = APIRouter(prefix="/api/settings")


@router.get("")
async def get_settings():
    """获取当前所有配置"""
    from app.config import needs_setup
    config = load_config()
    data = dict(config)
    data["_meta"] = {"needs_setup": needs_setup()}
    return data


@router.put("")
async def update_settings(request_body: dict):
    """保存配置"""
    save_config(request_body)
    # 重新加载到各模块
    task_manager.reload_config()
    pikpak_routes.reset_clients()
    return {"success": True, "message": "设置已保存"}


@router.post("/test/aria2")
async def test_aria2(payload: dict = Body(None)):
    if payload:
        cfg = payload
    else:
        cfg = load_config()["aria2"]
    
    client = Aria2Client(
        rpc_url=cfg.get("rpc_url", ""),
        rpc_port=cfg.get("rpc_port", 6800),
        rpc_secret=cfg.get("rpc_secret", "")
    )
    result = await client.test_connection()
    await client.close()
    return result


@router.post("/test/teldrive")
async def test_teldrive(payload: dict = Body(None)):
    if payload:
        cfg = payload
    else:
        cfg = load_config()["teldrive"]
        
    client = TelDriveClient(
        api_host=cfg.get("api_host", ""),
        access_token=cfg.get("access_token", "")
    )
    return await client.test_connection()


@router.post("/test/pikpak")
async def test_pikpak(payload: dict = Body(None)):
    from app.modules.pikpak.client import PikPakClient
    if payload:
        cfg = payload
    else:
        cfg = load_config()["pikpak"]
        
    if not cfg.get("username") or not cfg.get("password"):
         return {"success": False, "message": "PikPak 账号密码不能为空"}

    try:
        client = PikPakClient(
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            save_dir=cfg.get("save_dir", "/"),
        )
        await client.login()
        return {"success": True, "message": "PikPak 登录成功"}
    except Exception as e:
        return {"success": False, "message": f"PikPak 连接失败: {str(e)}"}

@router.post("/test/telegram")
async def test_telegram(payload: dict = Body(None)):
    from app.modules.tel2teldrive.service import service
    if payload:
        # Dynamic check
        api_id = payload.get("api_id")
        api_hash = payload.get("api_hash")
        if not api_id or not api_hash:
            return {"success": False, "message": "Telegram API ID 和 Hash 不能为空"}
        try:
            from telethon import TelegramClient
            import pathlib
            
            # Using memory session since we just want to verify credentials
            from telethon.sessions import MemorySession
            temp_client = TelegramClient(
                MemorySession(),
                api_id=api_id,
                api_hash=api_hash
            )
            # Try connecting
            await temp_client.connect()
            connected = temp_client.is_connected()
            await temp_client.disconnect()
            
            if connected:
                return {"success": True, "message": "Telegram 握手成功"}
            else:
                return {"success": False, "message": "Telegram 连接失败"}
        except Exception as e:
            return {"success": False, "message": f"Telegram 验证失败: {str(e)}"}
    else:
        from app.modules.tel2teldrive.service import broker
        state = broker.snapshot()
        if state.get("authorized"):
            return {"success": True, "message": "Telegram 连接正常并已授权"}
        elif state.get("phase") in ("awaiting_qr", "awaiting_password"):
            return {"success": True, "message": "正常：等待扫码验证"}
        else:
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
        # PikPak: 检查配置是否存在
        cfg = load_config()
        pikpak_cfg = cfg.get("pikpak", {})
        statuses["pikpak"] = bool(pikpak_cfg.get("username")) and bool(pikpak_cfg.get("password"))

        # Aria2
        statuses["aria2"] = task_manager.aria2 is not None

        # TelDrive
        statuses["teldrive"] = task_manager.teldrive is not None

        # Telegram
        try:
            from app.modules.tel2teldrive.service import broker
            tg_state = broker.snapshot()
            statuses["telegram"] = bool(
                tg_state.get("authorized")
                or tg_state.get("phase") in ("awaiting_qr", "awaiting_password")
            )
        except Exception:
            tg_cfg = cfg.get("telegram", {})
            statuses["telegram"] = bool(tg_cfg.get("api_id")) and bool(tg_cfg.get("api_hash"))

        # Database
        db_cfg = cfg.get("telegram_db", {})
        statuses["database"] = bool(db_cfg.get("host"))
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"健康检查异常: {e}")

    is_healthy = all(statuses.values())
    err_modules = [k for k, v in statuses.items() if not v]
    msg = "所有系统服务连接正常" if is_healthy else f"存在异常服务: {', '.join(err_modules)}"
    return {"healthy": is_healthy, "message": msg, "details": statuses}

