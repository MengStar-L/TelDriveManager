"""应用自更新接口。"""

import hmac
import os

from fastapi import APIRouter, HTTPException, Request

import update_worker as installer
from app.updater import PROJECT_ROOT

from app.updater import update_manager
from app.aria2_service import aria2_service

router = APIRouter(prefix="/api/update", tags=["update"])


def verify_worker(request: Request):
    try:
        state = installer.read_json(PROJECT_ROOT / ".tdm-update-lock")
        token = request.headers.get("X-TDM-Update", "")
        if (request.client and request.client.host in {"127.0.0.1", "::1"}
                and token and hmac.compare_digest(token, state["token"])):
            return state
    except (OSError, ValueError, KeyError):
        pass
    raise HTTPException(status_code=403, detail="Invalid update worker")


@router.get("/health")
async def update_health(request: Request):
    state = verify_worker(request)
    token = os.environ.get("TDM_UPDATE_TOKEN", "")
    backend = await aria2_service.update_health()
    return {"ready": bool(token and token == state["token"] and getattr(request.app.state, "ready", False) and backend["ready"]),
            "token": token, "version": update_manager._startup_version, "pid": os.getpid(), "aria2": backend}


@router.post("/shutdown")
async def update_shutdown(request: Request):
    state = verify_worker(request)
    if state["phase"] not in {"stopping", "validating", "rolling_back"} or not update_manager.shutdown_callback:
        raise HTTPException(status_code=409, detail="Shutdown is not available")
    update_manager.shutdown_callback()
    return {"success": True}


@router.get("/status")
async def update_status():
    return update_manager.snapshot()


@router.post("/check")
async def check_update():
    return await update_manager.check(force=True)


@router.post("/apply")
async def apply_update():
    return await update_manager.apply()
