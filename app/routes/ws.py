"""WebSocket 路由 — 实时推送（Aria2TelDrive 任务进度 + PikPak 工作流）"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.modules.aria2teldrive.task_manager import task_manager
from app.modules.pikpak import routes as pikpak_routes
from app.auth import is_auth_enabled, verify_token

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """统一 WebSocket — 同时推送 Aria2TelDrive 任务状态和 PikPak 工作流进度"""
    if is_auth_enabled():
        token = ws.query_params.get("token")
        if not token:
            token = ws.cookies.get("auth_token")
        if not token or not verify_token(token):
            await ws.close(code=4001, reason="未认证")
            return

    await ws.accept()
    # 注册到两个模块
    task_manager.register_ws(ws)
    pikpak_routes.register_ws(ws)
    try:
        # 发送 Aria2TelDrive 任务初始状态
        tasks = await task_manager.get_all_tasks()
        global_stat = task_manager.get_global_stat()
        await ws.send_json({
            "type": "init",
            "data": {"tasks": tasks, "global_stat": global_stat}
        })
        # 保持连接
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        task_manager.unregister_ws(ws)
        pikpak_routes.unregister_ws(ws)
