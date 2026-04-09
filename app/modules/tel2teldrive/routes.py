"""Tel2TelDrive 模块 API 路由 — 从 dashboard_app.py 提取"""

import asyncio
import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

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


def _join_teldrive_path(parent_path: str, name: str) -> str:
    normalized_name = str(name or "").strip().strip("/")
    if not normalized_name:
        return str(parent_path or "/")
    parent = str(parent_path or "/").strip() or "/"
    if parent in {"", "/"}:
        return f"/{normalized_name}"
    return f"{parent.rstrip('/')}/{normalized_name}"


def _build_folder_tree_node(config: Any, path: str, name: str, *, is_root: bool = False) -> dict[str, Any]:
    from app.modules.tel2teldrive.service import list_teldrive_dir

    items = list_teldrive_dir(config, path)
    direct_file_count = 0
    children: list[dict[str, Any]] = []

    for item in items:
        item_type = str(item.get("type") or "").strip().lower()
        if item_type == "folder":
            child_name = str(item.get("name") or "未命名文件夹").strip() or "未命名文件夹"
            child_path = _join_teldrive_path(path, child_name)
            child_node = _build_folder_tree_node(config, child_path, child_name)
            child_node["id"] = str(item.get("id") or child_path)
            children.append(child_node)
        else:
            direct_file_count += 1

    children.sort(key=lambda item: str(item.get("name") or "").lower())
    total_file_count = direct_file_count + sum(int(child.get("total_file_count") or 0) for child in children)
    descendant_folder_count = len(children) + sum(int(child.get("descendant_folder_count") or 0) for child in children)
    has_content = total_file_count > 0
    all_descendants_have_content = has_content and all(bool(child.get("all_descendants_have_content")) for child in children)

    if not has_content:
        status = "empty"
        status_label = "空文件夹"
    elif all_descendants_have_content:
        status = "healthy"
        status_label = "内容完整"
    else:
        status = "partial"
        status_label = "存在空目录"

    return {
        "id": path if is_root else path,
        "name": name,
        "path": path,
        "status": status,
        "status_label": status_label,
        "is_root": is_root,
        "direct_file_count": direct_file_count,
        "total_file_count": total_file_count,
        "descendant_folder_count": descendant_folder_count,
        "has_content": has_content,
        "all_descendants_have_content": all_descendants_have_content,
        "children": children,
    }


def _summarize_folder_tree(root: dict[str, Any]) -> dict[str, int]:
    summary = {"folder_count": 0, "empty_count": 0, "partial_count": 0, "healthy_count": 0, "file_count": 0}
    stack = [root]
    while stack:
        node = stack.pop()
        if not node.get("is_root"):
            summary["folder_count"] += 1
            status = str(node.get("status") or "").strip().lower()
            if status == "empty":
                summary["empty_count"] += 1
            elif status == "partial":
                summary["partial_count"] += 1
            elif status == "healthy":
                summary["healthy_count"] += 1
        summary["file_count"] += int(node.get("direct_file_count") or 0)
        stack.extend(reversed(node.get("children") or []))
    return summary


def _build_folder_tree_snapshot(config: Any) -> dict[str, Any]:
    root = _build_folder_tree_node(config, "/", "根目录 /", is_root=True)
    return {
        "root": root,
        "summary": _summarize_folder_tree(root),
        "scanned_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


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
    logs = await db.get_progress_logs(stream=T2TD_ACTION_LOG_STREAM, limit=max_limit)

    items: list[dict[str, Any]] = []
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

    items = items[-normalized_limit:]
    return {"items": items, "count": len(items)}


@router.delete("/deleted-files")
async def clear_deleted_files():
    from app import database as db
    from app.modules.tel2teldrive.service import T2TD_ACTION_LOG_STREAM

    cleared = await db.clear_progress_logs(stream=T2TD_ACTION_LOG_STREAM, message_type="auto_delete")
    return {"success": True, "count": cleared}


@router.get("/folder-tree")
async def get_folder_tree():
    _, _, config_store, _ = _get_deps()
    config = config_store.runtime()

    if not config.teldrive_url or not config.bearer_token:
        raise HTTPException(status_code=400, detail="TelDrive 配置不完整，无法扫描文件夹")

    try:
        snapshot = await asyncio.to_thread(_build_folder_tree_snapshot, config)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"读取 TelDrive 文件夹失败：{exc}")
    return snapshot


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
