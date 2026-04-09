"""Tel2TelDrive 模块 API 路由 — 从 dashboard_app.py 提取"""

import asyncio
import json
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse

router = APIRouter(prefix="/api/t2td")

# 延迟导入，避免循环依赖
_service = None
_broker = None
_config_store = None
_logger = None


def _get_deps():
    global _service, _broker, _config_store, _logger
    if _service is None:
        from app.modules.tel2teldrive.service import service, broker, config_store, logger
        _service = service
        _broker = broker
        _config_store = config_store
        _logger = logger
    return _service, _broker, _config_store, _logger


@router.get("/bootstrap")
async def bootstrap():
    service, broker, config_store, _ = _get_deps()
    return {
        "state": broker.snapshot(),
        "logs": broker.logs_snapshot(),
        "config": config_store.payload(),
    }


@router.get("/config")
async def get_config():
    _, _, config_store, _ = _get_deps()
    config_store.reload()
    return config_store.payload()


@router.post("/config")
async def save_config(request: Request):
    service, broker, config_store, logger = _get_deps()
    from app.modules.tel2teldrive.service import state_config_payload
    try:
        payload = await request.json()
        config_store.save(payload)
        runtime = config_store.runtime()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"配置保存失败：{exc}")

    await broker.update_state(**state_config_payload(runtime))
    await service.request_reload()
    logger.info("网页配置已保存")

    return {
        "ok": True,
        "config": config_store.payload(),
        "state": broker.snapshot(),
    }


@router.post("/database/test")
async def test_database(request: Request):
    _, _, config_store, _ = _get_deps()
    from app.modules.tel2teldrive.service import test_database_connection
    try:
        payload = await request.json()
        runtime = config_store.runtime_from_payload(payload, strict=True)
        return test_database_connection(runtime)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/login/refresh")
async def refresh_qr():
    service, _, _, _ = _get_deps()
    try:
        await service.request_qr_refresh()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"ok": True}


@router.post("/login/password")
async def submit_password(request: Request):
    service, _, _, _ = _get_deps()
    data = await request.json()
    password = str(data.get("password", ""))
    try:
        await service.submit_password(password)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"ok": True}


@router.get("/deleted-files")
async def get_deleted_files(limit: int = 200):
    _, _, config_store, _ = _get_deps()
    from app import database as db
    from app.modules.tel2teldrive.service import T2TD_ACTION_LOG_STREAM, get_t2td_action_log_limit

    config = config_store.runtime()
    max_limit = get_t2td_action_log_limit(config)
    normalized_limit = max(1, min(int(limit or 200), max_limit))
    logs = await db.get_progress_logs(stream=T2TD_ACTION_LOG_STREAM, limit=normalized_limit)

    items: list[dict] = []
    for log in logs:
        if str(log.get("message_type") or "").strip() != "auto_delete":
            continue
        payload = log.get("payload") if isinstance(log.get("payload"), dict) else {}
        items.append(
            {
                "id": log.get("id"),
                "job_id": log.get("job_id"),
                "created_at": log.get("created_at"),
                "file_id": payload.get("file_id") or log.get("job_id"),
                "file_name": payload.get("file_name") or payload.get("name") or "未命名文件",
                "reason": payload.get("reason") or "telegram_message_missing",
                "message_ids": payload.get("message_ids") or [],
                "missing_message_ids": payload.get("missing_message_ids") or [],
                "file_size": payload.get("file_size"),
                "occurred_at": payload.get("occurred_at") or log.get("created_at"),
                "source": payload.get("source") or "tel2teldrive_auto_sync",
            }
        )

    return {"items": items, "count": len(items)}


@router.get("/stream")
async def stream():
    _, broker, _, _ = _get_deps()
    queue = broker.subscribe()

    async def event_stream():
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            broker.unsubscribe(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
