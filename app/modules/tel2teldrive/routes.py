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
        "logs": broker.logs_snapshot(stream="service"),
        "health_logs": broker.logs_snapshot(stream="health"),
        "config": config_store.payload(),
        "health": await service.get_health_snapshot(limit=20),
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


@router.get("/health")
async def get_health_snapshot(limit: int = 50):
    service, _, _, _ = _get_deps()
    normalized_limit = max(1, min(int(limit or 50), 200))
    return await service.get_health_snapshot(limit=normalized_limit)


@router.post("/health/run")
async def run_health_check(request: Request):
    service, _, _, _ = _get_deps()
    payload: dict[str, object] = {}
    raw_body = await request.body()
    if raw_body:
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="巡检请求参数格式错误") from exc
    try:
        result = await service.request_health_check(
            payload.get("probe_bytes"),
            payload.get("scope_mode"),
            payload.get("scope_paths"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"ok": True, **result}


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
