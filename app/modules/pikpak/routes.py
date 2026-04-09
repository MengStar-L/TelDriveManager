"""PikPak 模块 API 路由 — 从 AutoPikDown web_server.py 迁移到 FastAPI"""

import asyncio
import os
import posixpath
import re
import uuid
import logging
from typing import Dict, List, Optional, Set

import feedparser
import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse

from app.config import load_config
from app import database as db
from app.modules.pikpak.client import PikPakClient
from app.aria2_client import Aria2Client
from app.modules.aria2teldrive.task_manager import task_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pikpak")

# ── 共享状态 ──
_pikpak: Optional[PikPakClient] = None
_aria2: Optional[Aria2Client] = None
_ws_clients: Set[WebSocket] = set()
_active_share_download_jobs: Set[str] = set()
_share_download_jobs_lock = asyncio.Lock()
_parse_job_lock = asyncio.Lock()
_active_parse_job_id: Optional[str] = None
_PARSE_JOB_TYPES = ("magnet", "share", "rss")


def _format_size(size: int) -> str:
    if size >= 1073741824:
        return f"{size / 1073741824:.1f} GB"
    elif size >= 1048576:
        return f"{size / 1048576:.1f} MB"
    elif size >= 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"


_NATURAL_SPLIT_RE = re.compile(r"(\d+)")


def _natural_sort_key(value: str):
    parts = _NATURAL_SPLIT_RE.split(str(value or "").strip().lower())
    return tuple((0, int(part)) if part.isdigit() else (1, part) for part in parts if part != "")


def _sort_file_entries_by_name(files: List[Dict]) -> List[Dict]:
    return sorted(
        files or [],
        key=lambda item: (
            _natural_sort_key(item.get("name") or item.get("title") or ""),
            _natural_sort_key(item.get("path") or item.get("name") or item.get("title") or ""),
            str(
                item.get("id")
                or item.get("file_id")
                or item.get("download_url")
                or item.get("url")
                or item.get("link")
                or ""
            ),
        ),
    )


def _normalize_teldrive_path(path: str) -> str:
    value = str(path or "").strip().replace("\\", "/")
    if not value:
        return "/"
    if not value.startswith("/"):
        value = "/" + value
    while "//" in value:
        value = value.replace("//", "/")
    return value.rstrip("/") or "/"


def _join_teldrive_path(base_path: str, sub_path: str = "") -> str:
    base = _normalize_teldrive_path(base_path)
    child = str(sub_path or "").strip().replace("\\", "/").strip("/")
    if not child:
        return base
    if base == "/":
        return _normalize_teldrive_path(f"/{child}")
    return _normalize_teldrive_path(f"{base}/{child}")


async def _register_aria2_task(gid: str, url: str, filename: str, teldrive_path: str):
    await task_manager.register_external_task(
        gid,
        url,
        filename,
        teldrive_path=_normalize_teldrive_path(teldrive_path),
        status="pending",
    )


def _create_clients():
    cfg = load_config()
    pikpak_cfg = cfg.get("pikpak", {})
    aria2_cfg = cfg.get("aria2", {})
    pikpak = PikPakClient(
        username=pikpak_cfg.get("username", ""),
        password=pikpak_cfg.get("password", ""),
        session=pikpak_cfg.get("session", ""),
        login_mode=pikpak_cfg.get("login_mode", "password"),
        save_dir=pikpak_cfg.get("save_dir", "/"),
    )
    aria2 = Aria2Client(
        rpc_url=aria2_cfg.get("rpc_url", "http://localhost"),
        rpc_port=aria2_cfg.get("rpc_port", 6800),
        rpc_secret=aria2_cfg.get("rpc_secret", ""),
    )
    return pikpak, aria2


async def _ensure_clients():
    global _pikpak, _aria2
    if _pikpak is None:
        _pikpak, _aria2 = _create_clients()
        await _pikpak.login()
    return _pikpak, _aria2


async def reset_clients():
    """配置变更后重置客户端"""
    global _pikpak, _aria2
    old_pikpak, old_aria2 = _pikpak, _aria2
    _pikpak = None
    _aria2 = None
    if old_pikpak is not None:
        await old_pikpak.close()
    if old_aria2 is not None:
        await old_aria2.close()


async def _broadcast(msg: dict, persist: bool = True):
    import json
    dead = set()
    payload = dict(msg or {})
    if persist:
        try:
            log_limit = int(load_config().get("log", {}).get("buffer_size", 400) or 400)
            saved = await db.add_progress_log(
                payload.get("type") or "info",
                payload,
                stream="pikpak",
                job_id=str(payload.get("parse_job_id") or payload.get("job_id") or "").strip() or None,
                limit=log_limit,
            )
            if saved:
                payload["log_id"] = saved.get("id")
                payload["created_at"] = saved.get("created_at")
        except Exception as e:
            logger.warning(f"记录 PikPak 进度日志失败: {e}")
    data = json.dumps(payload, ensure_ascii=False)
    for ws in list(_ws_clients):
        try:
            await ws.send_text(data)
        except Exception as e:
            logging.error(f"WS send error: {e}")
            dead.add(ws)
    if dead:
        _ws_clients.difference_update(dead)


def register_ws(ws: WebSocket):
    _ws_clients.add(ws)


def unregister_ws(ws: WebSocket):
    _ws_clients.discard(ws)


def _normalize_log_path(file_info: Dict[str, str]) -> str:
    return str(file_info.get("path") or file_info.get("name") or "未知文件").replace("\\", "/")


def _get_log_size(file_info: Dict[str, str]) -> str:
    try:
        size = int(file_info.get("size", 0) or 0)
    except (TypeError, ValueError):
        size = 0
    return _format_size(size) if size > 0 else ""


def _get_push_target_label(_engine: str = "aria2") -> str:
    return "Aria2 下载队列"


def _normalize_selected_ids(file_ids: List[str]) -> List[str]:
    normalized: List[str] = []
    seen: Set[str] = set()
    for file_id in file_ids or []:
        value = str(file_id or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _build_file_dedupe_key(file_info: Dict[str, str]) -> tuple:
    file_id = str(file_info.get("file_id") or "").strip()
    url = str(file_info.get("url") or "").strip()
    path = str(file_info.get("path") or "").strip()
    name = str(file_info.get("name") or "").strip()
    size = str(file_info.get("size") or "").strip()
    if file_id:
        return ("file_id", file_id)
    if url:
        return ("url", url)
    if path:
        return ("path", path)
    return ("name", name, size)


def _dedupe_file_entries(files: List[dict]) -> List[dict]:
    deduped: List[dict] = []
    seen: Set[tuple] = set()
    for file_info in files or []:
        if not file_info:
            continue
        key = _build_file_dedupe_key(file_info)
        if key in seen:
            logger.warning(f"检测到重复文件条目，已跳过: {file_info.get('path') or file_info.get('name') or '未知文件'}")
            continue
        seen.add(key)
        deduped.append(file_info)
    return deduped


def _make_share_download_job_key(share_id: str, file_ids: List[str]) -> str:
    normalized_ids = sorted(_normalize_selected_ids(file_ids))
    return f"{share_id}:{'|'.join(normalized_ids)}"


async def _register_share_download_job(job_key: str) -> bool:
    async with _share_download_jobs_lock:
        if job_key in _active_share_download_jobs:
            return False
        _active_share_download_jobs.add(job_key)
        return True


async def _release_share_download_job(job_key: str):
    if not job_key:
        return
    async with _share_download_jobs_lock:
        _active_share_download_jobs.discard(job_key)


async def _broadcast_resolved_files(index: int, files: List[dict], extra: Dict | None = None):
    total = len(files)
    extra_payload = dict(extra or {})
    for sequence, file_info in enumerate(files, 1):
        await _broadcast({
            "type": "file_resolved",
            "index": index,
            "sequence": sequence,
            "total_files": total,
            "file_name": file_info.get("name", "未知文件"),
            "file_path": _normalize_log_path(file_info),
            "file_size": _get_log_size(file_info),
            **extra_payload,
        })


async def _broadcast_link_pushed(index: int, file_info: Dict[str, str], sequence: int,
                                 total_files: int, target: str, extra: Dict | None = None):
    await _broadcast({
        "type": "link_pushed",
        "index": index,
        "sequence": sequence,
        "total_files": total_files,
        "file_name": file_info.get("name", "未知文件"),
        "file_path": _normalize_log_path(file_info),
        "file_size": _get_log_size(file_info),
        "target": target,
        **dict(extra or {}),
    })


def _is_parse_job_active(job: Optional[dict]) -> bool:
    return bool(job and job.get("status") in {"pending", "running"})


def _summarize_parse_request(job_type: str, request_payload: dict | None = None) -> str:
    payload = request_payload or {}
    if job_type == "magnet":
        source = str(payload.get("magnet") or "").strip()
    elif job_type == "share":
        source = str(payload.get("share_link") or "").strip()
    else:
        source = str(payload.get("url") or "").strip()
    return source[:120] + ("..." if len(source) > 120 else "")


def _serialize_parse_job(job: Optional[dict]) -> Optional[dict]:
    if not job:
        return None
    item = dict(job)
    item["request_payload"] = dict(item.get("request_payload") or {})
    result_payload = item.get("result_payload")
    item["input_summary"] = _summarize_parse_request(str(item.get("job_type") or ""), item["request_payload"])
    if isinstance(result_payload, dict):
        if item.get("job_type") in {"magnet", "share"}:
            item["result_count"] = len(result_payload.get("files", []))
        elif item.get("job_type") == "rss":
            item["result_count"] = len(result_payload.get("items", []))
        else:
            item["result_count"] = 0
    else:
        item["result_count"] = 0
    return item


async def init_runtime_state():
    global _active_parse_job_id
    _active_parse_job_id = None
    await db.fail_active_parse_jobs("服务重启，后台解析任务已中断")


async def _get_active_parse_job() -> Optional[dict]:
    global _active_parse_job_id
    if _active_parse_job_id:
        job = await db.get_parse_job(_active_parse_job_id)
        if _is_parse_job_active(job):
            return job
        _active_parse_job_id = None

    job = await db.get_active_parse_job()
    if _is_parse_job_active(job):
        _active_parse_job_id = str(job.get("job_id") or "") or None
        return job
    return None


async def _broadcast_parse_job_state(job: Optional[dict]):
    await _broadcast({"type": "parse_job_state", "job": _serialize_parse_job(job)}, persist=False)


async def _finish_parse_job(job_id: str, status: str, *, result_payload: dict | None = None,
                            error: str | None = None) -> Optional[dict]:
    global _active_parse_job_id
    job = await db.update_parse_job(
        job_id,
        status=status,
        result_payload=result_payload,
        error=(error or None),
    )
    if _active_parse_job_id == job_id:
        _active_parse_job_id = None
    await _broadcast_parse_job_state(job)
    return job


async def _create_parse_job(job_type: str, request_payload: dict) -> tuple[Optional[dict], Optional[dict]]:
    global _active_parse_job_id
    async with _parse_job_lock:
        active_job = await _get_active_parse_job()
        if _is_parse_job_active(active_job):
            return None, active_job
        job_id = uuid.uuid4().hex
        job = await db.create_parse_job(job_id, job_type, request_payload, status="running")
        _active_parse_job_id = job_id
    await _broadcast_parse_job_state(job)
    return job, None


async def _build_parse_snapshot() -> dict:
    log_limit = max(1, int(load_config().get("log", {}).get("buffer_size", 400) or 400))
    latest_jobs = {}
    for job_type in _PARSE_JOB_TYPES:
        latest_jobs[job_type] = _serialize_parse_job(await db.get_latest_parse_job(job_type))
    return {
        "logs": await db.get_progress_logs(stream="pikpak", limit=log_limit),
        "active_job": _serialize_parse_job(await _get_active_parse_job()),
        "latest_jobs": latest_jobs,
    }


async def _run_magnet_parse_job(job_id: str, magnet: str):
    from pikpakapi.enums import DownloadStatus
    import time

    magnet_summary = magnet[:80] + ("..." if len(magnet) > 80 else "")
    cfg = load_config()
    poll_interval = cfg.get("pikpak", {}).get("poll_interval", 3)
    max_wait_time = cfg.get("pikpak", {}).get("max_wait_time", 3600)

    await _broadcast({
        "type": "task_start",
        "index": 1,
        "total": 1,
        "magnet": magnet_summary,
        "parse_job_id": job_id,
        "workflow": "parse",
    })
    try:
        pikpak, _ = await _ensure_clients()
        task_info = await pikpak.add_offline_task(magnet)
        task_id = task_info["task_id"]
        file_id = task_info["file_id"]
        file_name = task_info["file_name"]
        if not task_id:
            raise RuntimeError("添加离线任务失败")

        await _broadcast({
            "type": "task_added",
            "index": 1,
            "file_name": file_name,
            "task_id": task_id,
            "parse_job_id": job_id,
        })
        await _broadcast({
            "type": "task_status",
            "index": 1,
            "status": "PikPak 离线任务已创建，等待云端完成缓存...",
            "parse_job_id": job_id,
        })

        start_time = time.time()
        while True:
            if time.time() - start_time > max_wait_time:
                raise TimeoutError(f"等待超时 ({max_wait_time}s)")
            try:
                status = await pikpak.client.get_task_status(task_id, file_id)
            except Exception:
                await asyncio.sleep(poll_interval)
                continue
            if status == DownloadStatus.done:
                break
            if status in (DownloadStatus.error, DownloadStatus.not_found):
                raise RuntimeError(f"离线失败 ({status.value})")
            await asyncio.sleep(poll_interval)

        file_tree = await pikpak.list_file_tree(file_id)
        for item in file_tree:
            item["size_str"] = _format_size(item.get("size", 0))
        file_tree = _sort_file_entries_by_name(file_tree)
        await _broadcast({
            "type": "files_found",
            "index": 1,
            "files": [item.get("name", "未知文件") for item in file_tree],
            "parse_job_id": job_id,
        })
        await _broadcast_resolved_files(1, file_tree, {"parse_job_id": job_id})
        await _broadcast({
            "type": "task_done",
            "index": 1,
            "file_name": file_name,
            "parse_job_id": job_id,
        })
        await _broadcast({"type": "all_done", "total": 1, "parse_job_id": job_id})
        await _finish_parse_job(
            job_id,
            "completed",
            result_payload={"file_id": file_id, "file_name": file_name, "files": file_tree},
        )
    except Exception as e:
        await _broadcast({"type": "error", "message": f"磁链解析失败: {e}", "parse_job_id": job_id})
        await _finish_parse_job(job_id, "failed", error=str(e))


async def _run_share_parse_job(job_id: str, share_link: str, pass_code: str):
    share_summary = share_link[:80] + ("..." if len(share_link) > 80 else "")
    await _broadcast({
        "type": "task_start",
        "index": 1,
        "total": 1,
        "magnet": share_summary,
        "parse_job_id": job_id,
        "workflow": "parse",
    })
    try:
        await _ensure_clients()
        await _broadcast({
            "type": "task_status",
            "index": 1,
            "status": "正在读取分享内容并递归解析文件列表...",
            "parse_job_id": job_id,
        })
        result = await _pikpak.get_share_file_list(share_link, pass_code)
        for item in result.get("files", []):
            item["size_str"] = _format_size(item.get("size", 0))
        result["files"] = _sort_file_entries_by_name(result.get("files", []))
        await _broadcast({
            "type": "files_found",
            "index": 1,
            "files": [item.get("name", "未知文件") for item in result.get("files", [])],
            "parse_job_id": job_id,
        })
        await _broadcast_resolved_files(1, result.get("files", []), {"parse_job_id": job_id})
        await _broadcast({"type": "task_done", "index": 1, "file_name": "分享解析完成", "parse_job_id": job_id})
        await _broadcast({"type": "all_done", "total": 1, "parse_job_id": job_id})
        await _finish_parse_job(job_id, "completed", result_payload=result)
    except Exception as e:
        await _broadcast({"type": "error", "message": f"分享解析失败: {e}", "parse_job_id": job_id})
        await _finish_parse_job(job_id, "failed", error=str(e))


async def _run_rss_parse_job(job_id: str, rss_url: str):
    await _broadcast({
        "type": "task_start",
        "index": 1,
        "total": 1,
        "magnet": rss_url[:80] + ("..." if len(rss_url) > 80 else ""),
        "parse_job_id": job_id,
        "workflow": "parse",
    })
    try:
        await _broadcast({
            "type": "task_status",
            "index": 1,
            "status": "正在下载 RSS 源并提取可用链接...",
            "parse_job_id": job_id,
        })
        async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
            resp = await client.get(rss_url, headers={"User-Agent": "TelDriveManager/1.0"})
            resp.raise_for_status()
            raw = resp.text
        feed = feedparser.parse(raw)
        if feed.bozo and not feed.entries:
            raise RuntimeError(f"RSS 解析失败: {feed.bozo_exception}")

        items = []
        magnet_re = re.compile(r"magnet:\?xt=urn:[^\s\"'<>]+", re.IGNORECASE)
        for entry in feed.entries:
            title = entry.get("title", "未知")
            published = entry.get("published", entry.get("updated", ""))
            link = entry.get("link", "")
            magnet = ""
            torrent = ""
            for enc in entry.get("enclosures", []):
                href = enc.get("href", "")
                if href.startswith("magnet:"):
                    magnet = href
                    break
                if href.endswith(".torrent"):
                    torrent = href
            if not magnet and link.startswith("magnet:"):
                magnet = link
            elif not torrent and link.endswith(".torrent"):
                torrent = link
            if not magnet:
                for lnk in entry.get("links", []):
                    href = lnk.get("href", "")
                    if href.startswith("magnet:"):
                        magnet = href
                        break
                    if href.endswith(".torrent") and not torrent:
                        torrent = href
            if not magnet:
                content = entry.get("summary", "") + entry.get("description", "")
                match = magnet_re.search(content)
                if match:
                    magnet = match.group(0)
            download_url = magnet or torrent
            if not download_url:
                continue
            items.append({
                "title": title,
                "download_url": download_url,
                "type": "magnet" if magnet else "torrent",
                "published": published,
                "link": link if not link.startswith("magnet:") else "",
            })

        items = _sort_file_entries_by_name(items)
        await _broadcast({
            "type": "files_found",
            "index": 1,
            "files": [item.get("title", "未命名订阅") for item in items],
            "parse_job_id": job_id,
        })
        await _broadcast_resolved_files(
            1,
            [{"name": item.get("title", "未命名订阅"), "path": item.get("link") or item.get("download_url") or "", "size": 0} for item in items],
            {"parse_job_id": job_id},
        )
        result = {"title": feed.feed.get("title", "RSS Feed"), "count": len(items), "items": items}
        await _broadcast({"type": "task_done", "index": 1, "file_name": result["title"], "parse_job_id": job_id})
        await _broadcast({"type": "all_done", "total": 1, "parse_job_id": job_id})
        await _finish_parse_job(job_id, "completed", result_payload=result)
    except Exception as e:
        await _broadcast({"type": "error", "message": f"RSS 解析失败: {e}", "parse_job_id": job_id})
        await _finish_parse_job(job_id, "failed", error=str(e))


@router.get("/progress/snapshot")
async def api_progress_snapshot():
    return await _build_parse_snapshot()


@router.delete("/progress/logs")
async def api_clear_progress_logs():
    cleared = await db.clear_progress_logs(stream="pikpak")
    return {"success": True, "count": cleared}


# ── 磁链 API ──

@router.post("/add")
async def api_add(request: Request):
    data = await request.json()
    magnets_text = data.get("magnets", "")
    magnets = [m.strip() for m in magnets_text.strip().splitlines()
               if m.strip() and not m.strip().startswith("#")]
    if not magnets:
        return JSONResponse({"error": "没有有效的磁力链接"}, status_code=400)
    asyncio.create_task(_process_magnets(magnets))
    return {"message": f"已提交 {len(magnets)} 个磁链，处理中...", "count": len(magnets)}


@router.post("/magnet/parse")
async def api_magnet_parse(request: Request):
    body = await request.json()
    magnet = body.get("magnet", "").strip()
    if not magnet:
        return JSONResponse({"error": "请输入磁力链接"}, status_code=400)

    job, active_job = await _create_parse_job("magnet", {"magnet": magnet})
    if not job:
        return JSONResponse({
            "error": "当前已有解析任务正在执行，请等待完成后再发起新的解析",
            "active_job": _serialize_parse_job(active_job),
        }, status_code=409)

    asyncio.create_task(_run_magnet_parse_job(job["job_id"], magnet))
    return JSONResponse({
        "success": True,
        "message": "磁链解析任务已提交，正在后台执行",
        "job": _serialize_parse_job(job),
    }, status_code=202)


@router.post("/magnet/download")
async def api_magnet_download(request: Request):
    try:
        body = await request.json()
        file_id = body.get("file_id", "")
        selected_ids = body.get("selected_ids", [])
        keep_structure = body.get("keep_structure", True)
        teldrive_path = body.get("teldrive_path", "/")
        if not file_id:
            return JSONResponse({"error": "缺少 file_id"}, status_code=400)
        asyncio.create_task(_process_magnet_selected(file_id, selected_ids, keep_structure, teldrive_path))
        return {"message": f"开始下载 {len(selected_ids)} 个文件"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/status")
async def api_status():
    try:
        pikpak, _ = await _ensure_clients()
        tasks = await pikpak.get_offline_tasks()
        task_list = [{
            "name": t.get("file_name", "未知"),
            "phase": t.get("phase", "未知"),
            "progress": t.get("progress", 0),
            "message": t.get("message", ""),
            "created_time": t.get("created_time", ""),
        } for t in tasks]
        return {"tasks": task_list}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/test")
async def api_test():
    results = {}
    pikpak = None
    aria2 = None
    try:
        pikpak, _ = _create_clients()
        await pikpak.login()
        results["pikpak"] = {"ok": True, "message": "登录成功"}
    except Exception as e:
        results["pikpak"] = {"ok": False, "message": str(e)}
    finally:
        if pikpak is not None:
            await pikpak.close()

    try:
        _, aria2 = _create_clients()
        test_result = await aria2.test_connection()
        results["aria2"] = {"ok": test_result["success"], "message": test_result["message"]}
    except Exception as e:
        results["aria2"] = {"ok": False, "message": str(e)}
    finally:
        if aria2 is not None:
            await aria2.close()
    return results


@router.get("/vip")
async def api_vip_info():
    try:
        await _ensure_clients()
        vip_result, quota_result = await asyncio.gather(
            _pikpak.client.vip_info(),
            _pikpak.client.get_quota_info(),
            return_exceptions=True,
        )
        is_vip = False
        vip_type = "unknown"
        expire = ""
        if isinstance(vip_result, dict):
            data = vip_result.get("data", vip_result)
            vip_type = data.get("type", "") or data.get("vip_type", "")
            status = data.get("status", "")
            expire = data.get("expire", "") or data.get("expire_time", "")
            is_vip = bool(vip_type and vip_type.lower() not in ("novip", "none", ""))
            if not is_vip and status:
                is_vip = status.lower() in ("ok", "active", "valid")
        quota_limit = 0
        quota_usage = 0
        if isinstance(quota_result, dict):
            q = quota_result.get("quota", {})
            quota_limit = int(q.get("limit", 0))
            quota_usage = int(q.get("usage", 0))
        return {"is_vip": is_vip, "type": vip_type, "expire": expire,
                "quota_limit": quota_limit, "quota_usage": quota_usage}
    except Exception as e:
        return {"is_vip": False, "type": "unknown", "error": str(e)}


# ── RSS ──

@router.post("/rss/parse")
async def api_rss_parse(request: Request):
    body = await request.json()
    rss_url = body.get("url", "").strip()
    if not rss_url:
        return JSONResponse({"error": "请输入 RSS 链接"}, status_code=400)

    job, active_job = await _create_parse_job("rss", {"url": rss_url})
    if not job:
        return JSONResponse({
            "error": "当前已有解析任务正在执行，请等待完成后再发起新的解析",
            "active_job": _serialize_parse_job(active_job),
        }, status_code=409)

    asyncio.create_task(_run_rss_parse_job(job["job_id"], rss_url))
    return JSONResponse({
        "success": True,
        "message": "RSS 解析任务已提交，正在后台执行",
        "job": _serialize_parse_job(job),
    }, status_code=202)


@router.post("/rss/download")
async def api_rss_download(request: Request):
    try:
        body = await request.json()
        urls = body.get("urls", [])
        teldrive_path = body.get("teldrive_path", "/")
        if not urls:
            return JSONResponse({"error": "没有选中的链接"}, status_code=400)
        asyncio.create_task(_process_magnets(urls, teldrive_path))
        return {"message": f"已提交 {len(urls)} 个链接，处理中...", "count": len(urls)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Share ──

@router.post("/share/list")
async def api_share_list(request: Request):
    body = await request.json()
    share_link = body.get("share_link", "").strip()
    pass_code = body.get("pass_code", "").strip()
    if not share_link:
        return JSONResponse({"error": "请输入分享链接"}, status_code=400)

    job, active_job = await _create_parse_job("share", {"share_link": share_link, "pass_code": pass_code})
    if not job:
        return JSONResponse({
            "error": "当前已有解析任务正在执行，请等待完成后再发起新的解析",
            "active_job": _serialize_parse_job(active_job),
        }, status_code=409)

    asyncio.create_task(_run_share_parse_job(job["job_id"], share_link, pass_code))
    return JSONResponse({
        "success": True,
        "message": "分享解析任务已提交，正在后台执行",
        "job": _serialize_parse_job(job),
    }, status_code=202)


@router.post("/share/download")
async def api_share_download(request: Request):
    try:
        body = await request.json()
        share_id = str(body.get("share_id", "") or "").strip()
        file_ids = _normalize_selected_ids(body.get("file_ids", []))
        pass_code_token = body.get("pass_code_token", "")
        keep_structure = body.get("keep_structure", True)
        file_paths = body.get("file_paths", {})
        rename_by_folder = body.get("rename_by_folder", False)
        teldrive_path = body.get("teldrive_path", "/")
        if not share_id or not file_ids:
            return JSONResponse({"error": "缺少参数"}, status_code=400)

        job_key = _make_share_download_job_key(share_id, file_ids)
        if not await _register_share_download_job(job_key):
            return JSONResponse({"error": "相同分享文件任务正在处理中，请勿重复提交"}, status_code=409)

        try:
            asyncio.create_task(_process_share_download(
                share_id,
                file_ids,
                pass_code_token,
                keep_structure,
                file_paths,
                rename_by_folder,
                teldrive_path=teldrive_path,
                job_key=job_key,
            ))
        except Exception:
            await _release_share_download_job(job_key)
            raise
        return {"message": f"开始处理 {len(file_ids)} 个文件"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── 后台处理 ──


def _maybe_rename_by_folder(url_info: dict, rename: bool, original_path: str = "") -> str:
    """根据上级目录名重命名分享文件，保持与 AutoPikDown 一致。"""
    name = url_info.get("name", "")
    if not rename:
        return name
    path = original_path or url_info.get("path", "")
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2:
        folder_name = parts[-2]
        dot_idx = name.rfind(".")
        return f"{folder_name}{name[dot_idx:]}" if dot_idx != -1 else folder_name
    return name


async def _aria2_push_only(files: List[dict], index: int, delete_pikpak_ids: List[str] = None,
                           keep_structure: bool = True, teldrive_path: str = "/"):
    """外部接收端模式：仅推送下载链接，不触发 TelDrive 上传"""
    try:
        _, aria2 = await _ensure_clients()
        cfg = load_config()
        aria2_cfg = cfg.get("aria2", {})
        push_target = _get_push_target_label("aria2")
        base_teldrive_path = _normalize_teldrive_path(teldrive_path)

        tasks_to_add = []
        task_contexts = []
        for f in files:
            path = str(f.get("path", f.get("name", "")) or "")
            parent_dir = posixpath.dirname(path).strip("/.")
            aria2_subdir = parent_dir if keep_structure and parent_dir else None
            task_teldrive_path = _join_teldrive_path(base_teldrive_path, parent_dir) if keep_structure and parent_dir else base_teldrive_path
            tasks_to_add.append({
                "url": f["url"],
                "name": f["name"],
                "subdir": aria2_subdir,
            })
            task_contexts.append({
                "url": f["url"],
                "name": f["name"],
                "teldrive_path": task_teldrive_path,
            })

        await _broadcast({
            "type": "task_status",
            "index": index,
            "status": f"共解析出 {len(files)} 个文件，开始推送下载链接到{push_target}...",
        })
        download_dir = aria2_cfg.get("download_dir", "")
        gids = await aria2.add_uris_batch(tasks_to_add, base_dir=download_dir)
        success_count = 0
        for sequence, (gid, file_info, task_ctx) in enumerate(zip(gids, files, task_contexts), 1):
            if not gid:
                continue
            await _register_aria2_task(gid, task_ctx["url"], task_ctx["name"], task_ctx["teldrive_path"])
            success_count += 1
            await _broadcast_link_pushed(index, file_info, sequence, len(files), push_target)
        await _broadcast({"type": "push_done", "index": index,
                          "success_count": success_count, "total_count": len(files),
                          "target": push_target})
    except Exception as e:
        await _broadcast({"type": "task_error", "index": index,
                          "message": f"下载链接推送失败: {e}"})

    if delete_pikpak_ids:
        try:
            pikpak, _ = await _ensure_clients()
            cfg = load_config()
            if cfg.get("pikpak", {}).get("delete_after_download", False):
                await pikpak.delete_files(delete_pikpak_ids)
        except Exception:
            pass


async def _process_magnets(magnets: List[str], teldrive_path: Optional[str] = None):
    from pikpakapi.enums import DownloadStatus
    import time

    cfg = load_config()
    pikpak_cfg = cfg.get("pikpak", {})
    poll_interval = pikpak_cfg.get("poll_interval", 3)
    max_wait_time = pikpak_cfg.get("max_wait_time", 3600)
    delete_after = pikpak_cfg.get("delete_after_download", False)
    resolved_teldrive_path = _normalize_teldrive_path(
        cfg.get("teldrive", {}).get("target_path", "/") if teldrive_path is None else teldrive_path
    )

    try:
        pikpak, _ = await _ensure_clients()
    except Exception as e:
        await _broadcast({"type": "error", "message": f"登录失败: {e}"})
        return

    total = len(magnets)
    for i, magnet in enumerate(magnets, 1):
        await _broadcast({"type": "task_start", "index": i, "total": total,
                          "magnet": magnet[:80] + ("..." if len(magnet) > 80 else "")})
        try:
            task_info = await pikpak.add_offline_task(magnet)
        except Exception as e:
            await _broadcast({"type": "task_error", "index": i, "message": f"添加离线任务失败: {e}"})
            continue

        task_id = task_info["task_id"]
        file_id = task_info["file_id"]
        file_name = task_info["file_name"]
        await _broadcast({"type": "task_added", "index": i, "file_name": file_name, "task_id": task_id})
        await _broadcast({"type": "task_status", "index": i,
                          "status": "PikPak 离线任务已创建，等待云端完成缓存..."})

        if not task_id:
            await _broadcast({"type": "task_error", "index": i, "message": "未获取到 task_id"})
            continue

        # 等待 PikPak 离线完成，并输出状态变化
        start_time = time.time()
        offline_ok = False
        last_status = None
        while True:
            if time.time() - start_time > max_wait_time:
                await _broadcast({"type": "task_error", "index": i, "message": f"等待超时 ({max_wait_time}s)"})
                break
            try:
                status = await pikpak.client.get_task_status(task_id, file_id)
            except Exception:
                await asyncio.sleep(poll_interval)
                continue
            if status != last_status:
                await _broadcast({"type": "task_status", "index": i,
                                  "status": f"PikPak 离线状态: {status.value}"})
                last_status = status
            if status == DownloadStatus.done:
                offline_ok = True
                await _broadcast({"type": "task_status", "index": i,
                                  "status": "PikPak 离线完成，开始解析下载链接..."})
                break
            elif status in (DownloadStatus.error, DownloadStatus.not_found):
                await _broadcast({"type": "task_error", "index": i, "message": f"离线失败 ({status.value})"})
                break
            await asyncio.sleep(poll_interval)

        if not offline_ok:
            continue

        # 获取直链
        try:
            files = await pikpak.get_download_urls(file_id)
        except Exception as e:
            await _broadcast({"type": "task_error", "index": i, "message": f"获取下载链接失败: {e}"})
            continue
        if not files:
            await _broadcast({"type": "task_error", "index": i, "message": "未找到可下载的文件"})
            continue

        await _broadcast({"type": "files_found", "index": i, "files": [f["name"] for f in files]})
        await _broadcast_resolved_files(i, files)

        delete_ids = [file_id] if delete_after else None
        push_target = _get_push_target_label("aria2")
        await _broadcast({"type": "task_status", "index": i,
                          "status": f"解析完成，共 {len(files)} 个文件，开始推送下载链接到{push_target}..."})
        await _aria2_push_only(
            files,
            i,
            delete_pikpak_ids=delete_ids,
            keep_structure=True,
            teldrive_path=resolved_teldrive_path,
        )

        await _broadcast({"type": "task_done", "index": i, "file_name": file_name})

    await _broadcast({"type": "all_done", "total": total})


async def _process_magnet_selected(root_file_id: str, selected_ids: List[str],
                                    keep_structure: bool = True, teldrive_path: Optional[str] = None):
    try:
        pikpak, _ = await _ensure_clients()
        cfg = load_config()
        delete_after = cfg.get("pikpak", {}).get("delete_after_download", False)

        await _broadcast({"type": "task_start", "index": 1, "total": 1,
                          "magnet": f"磁链选择下载 ({len(selected_ids)} 个文件)"})
        await _broadcast({"type": "task_status", "index": 1,
                          "status": "开始解析所选文件的下载链接..."})
        all_files = await pikpak.get_download_urls(root_file_id)
        if not all_files:
            await _broadcast({"type": "task_error", "index": 1, "message": "未找到可下载的文件"})
            return
        if selected_ids:
            selected_set = set(selected_ids)
            files = [f for f in all_files if f["file_id"] in selected_set]
        else:
            files = all_files
        if not files:
            await _broadcast({"type": "task_error", "index": 1, "message": "选中的文件不存在"})
            return
        await _broadcast({"type": "files_found", "index": 1, "files": [f["name"] for f in files]})
        await _broadcast_resolved_files(1, files)
        push_target = _get_push_target_label("aria2")
        await _broadcast({"type": "task_status", "index": 1,
                          "status": f"解析完成，共 {len(files)} 个文件，开始推送下载链接到{push_target}..."})

        delete_ids = [root_file_id] if delete_after else None
        resolved_teldrive_path = _normalize_teldrive_path(
            cfg.get("teldrive", {}).get("target_path", "/") if teldrive_path is None else teldrive_path
        )
        await _aria2_push_only(
            files,
            1,
            delete_pikpak_ids=delete_ids,
            keep_structure=keep_structure,
            teldrive_path=resolved_teldrive_path,
        )

        await _broadcast({"type": "all_done", "total": 1})
    except Exception as e:
        await _broadcast({"type": "error", "message": f"选择下载失败: {e}"})


async def _process_share_download(share_id: str, file_ids: List[str], pass_code_token: str,
                                   keep_structure: bool = True, file_paths: Dict[str, str] = None,
                                   rename_by_folder: bool = False, teldrive_path: Optional[str] = None,
                                   job_key: str = ""):
    saved_ids: List[str] = []
    try:
        await _ensure_clients()
        file_ids = _normalize_selected_ids(file_ids)
        total = len(file_ids)
        cfg = load_config()
        pikpak_cfg = cfg.get("pikpak", {})
        share_url_timeout = float(pikpak_cfg.get("share_download_url_timeout", 60))
        share_poll_interval = float(pikpak_cfg.get("share_download_url_poll_interval", 3))
        base_teldrive_path = _normalize_teldrive_path(
            cfg.get("teldrive", {}).get("target_path", "/") if teldrive_path is None else teldrive_path
        )

        await _broadcast({"type": "task_start", "index": 1, "total": total,
                          "magnet": f"分享文件 ({total} 个)"})
        await _broadcast({"type": "task_status", "index": 1,
                          "status": "正在保存分享内容到 PikPak 网盘..."})
        saved_ids = _normalize_selected_ids(await _pikpak.save_share_files(share_id, file_ids, pass_code_token))
        if not saved_ids:
            await _broadcast({"type": "task_error", "index": 1, "message": "保存失败，未获取到文件"})
            return

        logger.info(f"分享文件已保存, saved_ids={saved_ids}")
        await _broadcast({"type": "task_status", "index": 1,
                          "status": f"转存完成，共 {len(saved_ids)} 项，开始逐个解析下载链接..."})

        orig_paths_by_name: Dict[str, List[str]] = {}
        if rename_by_folder and file_paths:
            for _, path in file_paths.items():
                name = path.rsplit("/", 1)[-1]
                orig_paths_by_name.setdefault(name, []).append(path)
            for paths in orig_paths_by_name.values():
                paths.sort(key=_natural_sort_key)

        all_urls: List[dict] = []
        for i, fid in enumerate(saved_ids, 1):
            await _broadcast({"type": "task_status", "index": i, "status": f"获取下载链接 [{i}/{len(saved_ids)}]"})
            try:
                urls = await _pikpak.wait_for_download_urls(
                    fid,
                    timeout=share_url_timeout,
                    poll_interval=share_poll_interval,
                )
            except Exception as e:
                logger.error(f"分享文件直链获取失败: file_id={fid}, error={e}")
                await _broadcast({"type": "task_error", "index": i, "message": f"获取链接失败: {e}"})
                continue

            if not urls:
                await _broadcast({"type": "task_error", "index": i,
                                  "message": f"获取链接失败 ({int(share_url_timeout)}s 内未就绪)"})
                continue

            await _broadcast({"type": "task_status", "index": i,
                              "status": f"第 {i} 项解析成功，生成 {len(urls)} 条可用下载链接"})
            await _broadcast_resolved_files(i, urls)
            all_urls.extend(urls)

        all_urls = _sort_file_entries_by_name(_dedupe_file_entries(all_urls))
        if not all_urls:
            await _broadcast({"type": "error", "message": "所有文件链接获取失败"})
            return

        push_target = _get_push_target_label("aria2")
        await _broadcast({"type": "task_status", "index": 1,
                          "status": f"全部链接解析完成，共 {len(all_urls)} 个文件，开始推送下载链接到{push_target}..."})
        aria2_cfg = cfg.get("aria2", {})
        success_count = 0
        download_dir = aria2_cfg.get("download_dir", "")
        for sequence, url_info in enumerate(all_urls, 1):
            try:
                opts = {}
                subdir = ""
                if download_dir:
                    if keep_structure:
                        path = url_info.get("path", "")
                        parts = [p for p in path.split("/") if p]
                        subdir = "/".join(parts[1:-1]) if len(parts) > 2 else ""
                        target_dir = os.path.join(download_dir, subdir) if subdir else download_dir
                    else:
                        target_dir = download_dir
                    opts["dir"] = target_dir.replace("\\", "/")

                original_path = ""
                if rename_by_folder:
                    name = url_info.get("name", "")
                    if orig_paths_by_name.get(name):
                        original_path = orig_paths_by_name[name].pop(0)

                output_name = _maybe_rename_by_folder(url_info, rename_by_folder, original_path)
                display_info = dict(url_info)
                if output_name:
                    opts["out"] = output_name
                    display_info["name"] = output_name

                gid = await _aria2.add_uri(url_info["url"], opts)
                if gid:
                    task_teldrive_path = base_teldrive_path
                    if keep_structure:
                        task_teldrive_path = _join_teldrive_path(base_teldrive_path, subdir)
                    await _register_aria2_task(gid, url_info["url"], display_info.get("name") or url_info.get("name") or "", task_teldrive_path)
                    success_count += 1
                    await _broadcast_link_pushed(1, display_info, sequence, len(all_urls), push_target)
            except Exception as e:
                await _broadcast({"type": "task_error", "index": 1,
                                  "message": f"下载链接推送失败: {url_info.get('name', '未知文件')} - {e}"})

        await _broadcast({"type": "push_done", "index": 1,
                          "success_count": success_count, "total_count": len(all_urls),
                          "target": push_target})

        if saved_ids:
            try:
                await _pikpak.delete_files(saved_ids)
            except Exception as e:
                logger.warning(f"清理分享临时文件失败: {e}")

        await _broadcast({"type": "all_done", "total": total})
    except Exception as e:
        await _broadcast({"type": "error", "message": f"分享下载失败: {e}"})
    finally:
        await _release_share_download_job(job_key)
