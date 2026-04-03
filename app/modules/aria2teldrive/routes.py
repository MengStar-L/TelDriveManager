"""Aria2TelDrive 模块 API 路由 — 任务管理接口"""

from fastapi import APIRouter, HTTPException
from app.models import TaskAddRequest
from app.modules.aria2teldrive.task_manager import task_manager
from app import database as db

router = APIRouter(prefix="/api/a2td")


@router.post("/task/add")
async def add_task(req: TaskAddRequest):
    """添加下载任务"""
    task = await task_manager.add_task(
        url=req.url, filename=req.filename, teldrive_path=req.teldrive_path
    )
    return {"success": True, "data": task}


@router.get("/tasks")
async def get_all_tasks():
    tasks = await task_manager.get_all_tasks()
    return {"tasks": tasks}


@router.get("/task/{task_id}")
async def get_task(task_id: str):
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    return {"data": task}


@router.post("/task/{task_id}/pause")
async def pause_task(task_id: str):
    result = await task_manager.pause_task(task_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/task/{task_id}/resume")
async def resume_task(task_id: str):
    result = await task_manager.resume_task(task_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/task/{task_id}/cancel")
async def cancel_task(task_id: str):
    result = await task_manager.cancel_task(task_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/task/{task_id}/retry")
async def retry_task(task_id: str):
    result = await task_manager.retry_task(task_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.delete("/task/{task_id}")
async def delete_task(task_id: str):
    result = await task_manager.delete_task(task_id)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/tasks/clear-completed")
async def clear_completed_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] in ("completed", "cancelled"):
            await task_manager.delete_task(t["task_id"])
            count += 1
    return {"success": True, "message": f"已清除 {count} 个任务"}


@router.post("/tasks/clear-failed")
async def clear_failed_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] == "failed":
            await task_manager.delete_task(t["task_id"])
            count += 1
    return {"success": True, "message": f"已清除 {count} 个失败任务"}


@router.post("/tasks/clear-all")
async def clear_all_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] in ("downloading", "uploading", "pending", "paused"):
            try:
                await task_manager.cancel_task(t["task_id"])
            except Exception:
                pass
        await task_manager.delete_task(t["task_id"])
        count += 1
    return {"success": True, "message": f"已清除全部 {count} 个任务"}


@router.post("/tasks/retry-failed")
async def retry_all_failed_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    errors = []
    for t in tasks:
        if t["status"] == "failed":
            result = await task_manager.retry_task(t["task_id"])
            if result["success"]:
                count += 1
            else:
                errors.append(f"{t.get('filename', t['task_id'])}: {result['message']}")
    if errors:
        return {"success": True, "message": f"已重试 {count} 个任务，{len(errors)} 个失败"}
    return {"success": True, "message": f"已重试 {count} 个失败任务"}


@router.post("/tasks/pause-all")
async def pause_all_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] == "downloading":
            result = await task_manager.pause_task(t["task_id"])
            if result["success"]:
                count += 1
        elif t["status"] == "uploading":
            task_id = t["task_id"]
            task_manager._cancel_existing_upload(task_id)
            task_manager.clear_upload_progress(task_id)
            old_gid = t.get("aria2_gid", "")
            if old_gid:
                task_manager._uploading_gids.discard(old_gid)
            await db.update_task(task_id, status="paused", download_speed="", upload_speed="", error=None)
            await task_manager._broadcast_task_update(task_id)
            count += 1
    return {"success": True, "message": f"已暂停 {count} 个任务"}


@router.post("/tasks/resume-all")
async def resume_all_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] == "paused":
            result = await task_manager.resume_task(t["task_id"])
            if result["success"]:
                count += 1
    return {"success": True, "message": f"已恢复 {count} 个任务"}


@router.post("/tasks/pause-uploads")
async def pause_all_uploads():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] == "uploading":
            task_id = t["task_id"]
            task_manager._cancel_existing_upload(task_id)
            task_manager.clear_upload_progress(task_id)
            old_gid = t.get("aria2_gid", "")
            if old_gid:
                task_manager._uploading_gids.discard(old_gid)
            await db.update_task(task_id, status="paused", download_speed="", upload_speed="", error=None)
            await task_manager._broadcast_task_update(task_id)
            count += 1
    return {"success": True, "message": f"已暂停 {count} 个上传任务"}
