"""Aria2TelDrive module API routes."""

from fastapi import APIRouter, HTTPException

from app.models import TaskAddRequest
from app.modules.aria2teldrive.task_manager import task_manager

router = APIRouter(prefix="/api/a2td")


@router.post("/task/add")
async def add_task(req: TaskAddRequest):
    task = await task_manager.add_task(
        url=req.url, filename=req.filename, teldrive_path=req.teldrive_path or "/"
    )
    return {"success": True, "data": task}


@router.get("/tasks")
async def get_all_tasks():
    tasks = await task_manager.get_all_tasks()
    return {"tasks": tasks}


@router.get("/snapshot")
async def get_snapshot():
    tasks = await task_manager.get_all_tasks()
    return {"tasks": tasks, "global_stat": task_manager.get_global_stat()}


@router.get("/task/{task_id}")
async def get_task(task_id: str):
    task = await task_manager.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="task not found")
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


@router.post("/task/{task_id}/cleanup-polluted")
async def cleanup_polluted_task(task_id: str):
    result = await task_manager.cleanup_polluted_upload(task_id, retry_after_cleanup=False)
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/task/{task_id}/cleanup-and-retry")
async def cleanup_and_retry_task(task_id: str):
    result = await task_manager.cleanup_polluted_upload(task_id, retry_after_cleanup=True)
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
    return {"success": True, "message": f"cleared {count} completed tasks"}


@router.post("/tasks/clear-failed")
async def clear_failed_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] == "failed":
            await task_manager.delete_task(t["task_id"])
            count += 1
    return {"success": True, "message": f"cleared {count} failed tasks"}


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
    return {"success": True, "message": f"cleared all {count} tasks"}


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
        return {"success": True, "message": f"retried {count} tasks, {len(errors)} failed"}
    return {"success": True, "message": f"retried {count} failed tasks"}


@router.post("/tasks/pause-all")
async def pause_all_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] in ("downloading", "uploading", "pending"):
            result = await task_manager.pause_task(t["task_id"])
            if result["success"]:
                count += 1
    return {"success": True, "message": f"paused {count} tasks"}


@router.post("/tasks/resume-all")
async def resume_all_tasks():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] == "paused":
            result = await task_manager.resume_task(t["task_id"])
            if result["success"]:
                count += 1
    return {"success": True, "message": f"resumed {count} tasks"}


@router.post("/tasks/pause-uploads")
async def pause_all_uploads():
    tasks = await task_manager.get_all_tasks()
    count = 0
    for t in tasks:
        if t["status"] == "uploading":
            result = await task_manager.pause_task(t["task_id"])
            if result["success"]:
                count += 1
    return {"success": True, "message": f"paused {count} upload tasks"}
