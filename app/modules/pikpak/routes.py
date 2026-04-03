"""PikPak 模块 API 路由 — 从 AutoPikDown web_server.py 迁移到 FastAPI"""

import asyncio
import os
import posixpath
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

import feedparser
import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse

from app.config import load_config, save_config
from app.modules.pikpak.client import PikPakClient
from app.aria2_client import Aria2Client
from app.downloader import downloader, DownloadTask, TaskStatus, DOWNLOAD_DIR, _format_size

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pikpak")

# ── 共享状态 ──
_pikpak: Optional[PikPakClient] = None
_aria2: Optional[Aria2Client] = None
_ws_clients: Set[WebSocket] = set()


def _format_size(size: int) -> str:
    if size >= 1073741824:
        return f"{size / 1073741824:.1f} GB"
    elif size >= 1048576:
        return f"{size / 1048576:.1f} MB"
    elif size >= 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"


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


def reset_clients():
    """配置变更后重置客户端"""
    global _pikpak, _aria2
    _pikpak = None
    _aria2 = None


async def _broadcast(msg: dict):
    import json
    import logging
    dead = set()
    data = json.dumps(msg, ensure_ascii=False)
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


def _get_push_target_label(engine: str) -> str:
    return "内置下载队列" if engine == "builtin" else "下载链接接收端"


async def _broadcast_resolved_files(index: int, files: List[dict]):
    total = len(files)
    for sequence, file_info in enumerate(files, 1):
        await _broadcast({
            "type": "file_resolved",
            "index": index,
            "sequence": sequence,
            "total_files": total,
            "file_name": file_info.get("name", "未知文件"),
            "file_path": _normalize_log_path(file_info),
            "file_size": _get_log_size(file_info),
        })


async def _broadcast_link_pushed(index: int, file_info: Dict[str, str], sequence: int,
                                 total_files: int, target: str):
    await _broadcast({
        "type": "link_pushed",
        "index": index,
        "sequence": sequence,
        "total_files": total_files,
        "file_name": file_info.get("name", "未知文件"),
        "file_path": _normalize_log_path(file_info),
        "file_size": _get_log_size(file_info),
        "target": target,
    })


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
    try:
        body = await request.json()
        magnet = body.get("magnet", "").strip()
        if not magnet:
            return JSONResponse({"error": "请输入磁力链接"}, status_code=400)
        pikpak, _ = await _ensure_clients()

        task_info = await pikpak.add_offline_task(magnet)
        task_id = task_info["task_id"]
        file_id = task_info["file_id"]
        file_name = task_info["file_name"]
        if not task_id:
            return JSONResponse({"error": "添加离线任务失败"}, status_code=500)

        from pikpakapi.enums import DownloadStatus
        import time
        cfg = load_config()
        poll_interval = cfg.get("pikpak", {}).get("poll_interval", 3)
        max_wait_time = cfg.get("pikpak", {}).get("max_wait_time", 3600)
        start_time = time.time()

        while True:
            if time.time() - start_time > max_wait_time:
                return JSONResponse({"error": f"等待超时 ({max_wait_time}s)"}, status_code=408)
            try:
                status = await pikpak.client.get_task_status(task_id, file_id)
            except Exception:
                await asyncio.sleep(poll_interval)
                continue
            if status == DownloadStatus.done:
                break
            elif status in (DownloadStatus.error, DownloadStatus.not_found):
                return JSONResponse({"error": f"离线失败 ({status.value})"}, status_code=500)
            await asyncio.sleep(poll_interval)

        file_tree = await pikpak.list_file_tree(file_id)
        for f in file_tree:
            f["size_str"] = _format_size(f.get("size", 0))
        return {"file_id": file_id, "file_name": file_name, "files": file_tree}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/magnet/download")
async def api_magnet_download(request: Request):
    try:
        body = await request.json()
        file_id = body.get("file_id", "")
        selected_ids = body.get("selected_ids", [])
        keep_structure = body.get("keep_structure", True)
        if not file_id:
            return JSONResponse({"error": "缺少 file_id"}, status_code=400)
        asyncio.create_task(_process_magnet_selected(file_id, selected_ids, keep_structure))
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
    try:
        pikpak, aria2 = _create_clients()
        await pikpak.login()
        results["pikpak"] = {"ok": True, "message": "登录成功"}
    except Exception as e:
        results["pikpak"] = {"ok": False, "message": str(e)}

    try:
        _, aria2 = _create_clients()
        test_result = await aria2.test_connection()
        results["aria2"] = {"ok": test_result["success"], "message": test_result["message"]}
    except Exception as e:
        results["aria2"] = {"ok": False, "message": str(e)}
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
    try:
        body = await request.json()
        rss_url = body.get("url", "").strip()
        if not rss_url:
            return JSONResponse({"error": "请输入 RSS 链接"}, status_code=400)
        async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
            resp = await client.get(rss_url, headers={"User-Agent": "TelDriveManager/1.0"})
            resp.raise_for_status()
            raw = resp.text
        feed = feedparser.parse(raw)
        if feed.bozo and not feed.entries:
            return JSONResponse({"error": f"RSS 解析失败: {feed.bozo_exception}"}, status_code=400)
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
                    magnet = href; break
                elif href.endswith(".torrent"):
                    torrent = href
            if not magnet and link.startswith("magnet:"):
                magnet = link
            elif not torrent and link.endswith(".torrent"):
                torrent = link
            if not magnet:
                for lnk in entry.get("links", []):
                    href = lnk.get("href", "")
                    if href.startswith("magnet:"):
                        magnet = href; break
                    elif href.endswith(".torrent") and not torrent:
                        torrent = href
            if not magnet:
                content = entry.get("summary", "") + entry.get("description", "")
                m = magnet_re.search(content)
                if m:
                    magnet = m.group(0)
            download_url = magnet or torrent
            if not download_url:
                continue
            items.append({"title": title, "download_url": download_url,
                          "type": "magnet" if magnet else "torrent",
                          "published": published,
                          "link": link if not link.startswith("magnet:") else ""})
        return {"title": feed.feed.get("title", "RSS Feed"), "count": len(items), "items": items}
    except httpx.HTTPError as e:
        return JSONResponse({"error": f"下载 RSS 失败: {e}"}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/rss/download")
async def api_rss_download(request: Request):
    try:
        body = await request.json()
        urls = body.get("urls", [])
        if not urls:
            return JSONResponse({"error": "没有选中的链接"}, status_code=400)
        asyncio.create_task(_process_magnets(urls))
        return {"message": f"已提交 {len(urls)} 个链接，处理中...", "count": len(urls)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Share ──

@router.post("/share/list")
async def api_share_list(request: Request):
    try:
        body = await request.json()
        share_link = body.get("share_link", "").strip()
        pass_code = body.get("pass_code", "").strip()
        if not share_link:
            return JSONResponse({"error": "请输入分享链接"}, status_code=400)
        await _ensure_clients()
        result = await _pikpak.get_share_file_list(share_link, pass_code)
        for f in result.get("files", []):
            f["size_str"] = _format_size(f.get("size", 0))
        return result
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/share/download")
async def api_share_download(request: Request):
    try:
        body = await request.json()
        share_id = body.get("share_id", "")
        file_ids = body.get("file_ids", [])
        pass_code_token = body.get("pass_code_token", "")
        keep_structure = body.get("keep_structure", True)
        file_paths = body.get("file_paths", {})
        rename_by_folder = body.get("rename_by_folder", False)
        if not share_id or not file_ids:
            return JSONResponse({"error": "缺少参数"}, status_code=400)
        asyncio.create_task(_process_share_download(
            share_id, file_ids, pass_code_token, keep_structure, file_paths, rename_by_folder))
        return {"message": f"开始处理 {len(file_ids)} 个文件"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── 后台处理 ──


def _get_engine() -> str:
    """获取用户配置的下载引擎"""
    cfg = load_config()
    return cfg.get("pikpak", {}).get("download_engine", "builtin")


def _get_teldrive_client():
    """按需创建 TelDrive 客户端（内置引擎上传用）"""
    from app.modules.aria2teldrive.teldrive_client import TelDriveClient
    cfg = load_config()
    td_cfg = cfg.get("teldrive", {})
    return TelDriveClient(
        api_host=td_cfg.get("api_host", ""),
        access_token=td_cfg.get("access_token", ""),
        channel_id=td_cfg.get("channel_id", 0),
        chunk_size=td_cfg.get("chunk_size", "500M"),
        upload_concurrency=td_cfg.get("upload_concurrency", 4),
        random_chunk_name=td_cfg.get("random_chunk_name", True),
        max_retries=cfg.get("upload", {}).get("max_retries", 3),
    )


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


async def _builtin_download_and_upload(files: List[dict], index: int, delete_pikpak_ids: List[str] = None):
    """内置引擎：下载文件列表 → 逐个上传 TelDrive → 清理
    
    每个文件会注册到 tasks 数据库，并通过 WS 广播更新到任务页面。
    """
    from app import database as db
    from app.modules.aria2teldrive.task_manager import task_manager

    cfg = load_config()
    td_cfg = cfg.get("teldrive", {})
    target_path = td_cfg.get("target_path", "/")
    auto_delete = cfg.get("upload", {}).get("auto_delete", True)

    # 更新下载器配置
    pikpak_cfg = cfg.get("pikpak", {})
    downloader.update_config(
        max_concurrent=pikpak_cfg.get("max_concurrent_downloads", 3),
        connections_per_task=pikpak_cfg.get("connections_per_task", 8),
    )

    total_files = len(files)
    push_target = _get_push_target_label("builtin")
    await _broadcast({
        "type": "task_status",
        "index": index,
        "status": f"共解析出 {total_files} 个文件，开始推送下载链接到{push_target}...",
    })
    for fi, f in enumerate(files, 1):
        url = f["url"]
        name = f["name"]
        file_index = index
        await _broadcast_link_pushed(file_index, f, fi, total_files, push_target)

        # === 注册到数据库 ===
        import uuid
        task_db_id = f"bd-{uuid.uuid4().hex[:10]}"
        task_download_dir = DOWNLOAD_DIR / task_db_id
        task_local_path = task_download_dir / name
        await db.add_task(task_db_id, url, name, target_path)
        await db.update_task(task_db_id, status="downloading",
                             local_path=str(task_local_path))
        await task_manager.broadcast({"type": "task_update",
                                       "data": await db.get_task(task_db_id)})

        # === 下载阶段 ===
        dl_done_event = asyncio.Event()

        async def on_progress(task: DownloadTask, _tid=task_db_id):
            progress = round(task.downloaded / task.total_size * 100, 1) if task.total_size > 0 else 0
            # 更新进度条
            await _broadcast({
                "type": "download_progress",
                "task_id": _tid,
                "index": file_index,
                "file_index": fi,
                "total_files": total_files,
                "filename": task.filename,
                "progress": progress,
                "speed": task.to_dict()["speed_str"],
                "downloaded": task.to_dict()["downloaded_str"],
                "downloaded_bytes": task.downloaded,
                "total": task.to_dict()["total_str"],
                "total_bytes": task.total_size,
                "eta": task.to_dict()["eta_str"],
                "connections": task.connections,
                "max_connections": task.max_connections,
                "status": task.status.value,
            })
            # 更新数据库（节流：只在整数百分比变化时写库）
            int_pct = int(progress)
            if int_pct % 5 == 0:
                await db.update_task(_tid, download_progress=progress,
                                     download_speed=task.to_dict()["speed_str"],
                                     file_size=task.to_dict()["total_str"])

        async def on_complete(task: DownloadTask):
            dl_done_event.set()

        dl_task = await downloader.add_task(
            url=url, filename=name, task_id=task_db_id,
            on_progress=on_progress, on_complete=on_complete,
            base_dir=str(task_download_dir),
        )

        # 等待下载完成
        await dl_done_event.wait()
        downloader.forget_task(task_db_id)

        if dl_task.status == TaskStatus.FAILED:
            await db.update_task(task_db_id, status="failed",
                                 error=dl_task.error)
            await task_manager.broadcast({"type": "task_update",
                                           "data": await db.get_task(task_db_id)})
            await _broadcast({"type": "task_error", "index": file_index,
                              "message": f"下载失败: {name} - {dl_task.error}"})
            continue
        if dl_task.status == TaskStatus.CANCELLED:
            task_manager.clear_upload_progress(task_db_id)
            if await db.get_task(task_db_id):
                await db.delete_task(task_db_id)
                await task_manager.broadcast({"type": "task_deleted",
                                               "data": {"task_id": task_db_id}})
            continue
        if not await db.get_task(task_db_id):
            task_manager.clear_upload_progress(task_db_id)
            continue

        # === 上传阶段 ===
        await _broadcast({
            "type": "task_status",
            "index": file_index,
            "status": f"下载完成，开始上传到 TelDrive: {name}",
        })
        await db.update_task(task_db_id, status="uploading",
                             download_progress=100.0,
                             download_speed="",
                             upload_speed="",
                             file_size=dl_task.to_dict()["total_str"])
        task_manager.track_upload_progress(task_db_id, 0)
        await task_manager.broadcast({"type": "task_update",
                                       "data": await db.get_task(task_db_id)})

        try:
            teldrive = _get_teldrive_client()
            local_path = dl_task.dest_path

            async def upload_progress_cb(uploaded: int, total: int, _tid=task_db_id, _name=name):
                raw_pct = round(uploaded / total * 100, 1) if total > 0 else 0
                pct = min(raw_pct, 99.9) if total > 0 else 0
                task_manager.track_upload_progress(_tid, uploaded)
                await _broadcast({
                    "type": "upload_progress",
                    "task_id": _tid,
                    "index": file_index,
                    "filename": _name,
                    "progress": pct,
                    "speed": "",
                    "uploaded": _format_size(uploaded),
                    "uploaded_bytes": uploaded,
                    "total": _format_size(total),
                    "total_bytes": total,
                })
                # 更新数据库
                int_pct = int(pct)
                if int_pct % 5 == 0:
                    await db.update_task(_tid, upload_progress=pct)

            if os.path.isdir(local_path):
                for root, _dirs, fnames in os.walk(local_path):
                    for fname in fnames:
                        fpath = os.path.join(root, fname)
                        rel = os.path.relpath(fpath, local_path)
                        sub_dir = os.path.dirname(rel).replace("\\", "/")
                        td_path = target_path.rstrip("/") + ("/" + sub_dir if sub_dir and sub_dir != "." else "")
                        await teldrive.upload_file_chunked(
                            file_path=fpath, teldrive_path=td_path,
                            progress_callback=upload_progress_cb,
                        )
            else:
                await teldrive.upload_file_chunked(
                    file_path=local_path, teldrive_path=target_path,
                    progress_callback=upload_progress_cb,
                )

            # 上传完成
            task_manager.clear_upload_progress(task_db_id)
            await db.update_task(task_db_id, status="completed",
                                 download_progress=100.0,
                                 upload_progress=100.0,
                                 download_speed="",
                                 upload_speed="")
            await task_manager.broadcast({"type": "task_update",
                                           "data": await db.get_task(task_db_id)})
            await _broadcast({"type": "upload_done", "task_id": task_db_id, "index": file_index,
                              "filename": name})

            # 清理本地文件
            if auto_delete:
                try:
                    if os.path.isdir(local_path):
                        import shutil
                        shutil.rmtree(local_path)
                    elif os.path.exists(local_path):
                        os.remove(local_path)
                    task_dir = Path(local_path).parent
                    if task_dir.exists() and task_dir != DOWNLOAD_DIR:
                        task_dir.rmdir()
                    logger.info(f"已清理本地文件: {local_path}")
                except Exception as e:
                    logger.warning(f"清理文件失败: {e}")

        except Exception as e:
            task_manager.clear_upload_progress(task_db_id)
            await db.update_task(task_db_id, status="failed",
                                 error=str(e))
            await task_manager.broadcast({"type": "task_update",
                                           "data": await db.get_task(task_db_id)})
            await _broadcast({"type": "task_error", "index": file_index,
                              "message": f"上传 TelDrive 失败: {name} - {e}"})
            continue

    # PikPak 网盘文件清理
    if delete_pikpak_ids:
        try:
            pikpak, _ = await _ensure_clients()
            await pikpak.delete_files(delete_pikpak_ids)
        except Exception:
            pass


async def _aria2_push_only(files: List[dict], index: int, delete_pikpak_ids: List[str] = None):
    """外部接收端模式：仅推送下载链接，不触发 TelDrive 上传"""
    try:
        _, aria2 = await _ensure_clients()
        cfg = load_config()
        aria2_cfg = cfg.get("aria2", {})
        push_target = _get_push_target_label("aria2")

        tasks_to_add = []
        for f in files:
            path = f.get("path", f["name"])
            parent_dir = posixpath.dirname(path)
            tasks_to_add.append({"url": f["url"], "name": f["name"],
                                 "subdir": parent_dir if parent_dir else None})

        await _broadcast({
            "type": "task_status",
            "index": index,
            "status": f"共解析出 {len(files)} 个文件，开始推送下载链接到{push_target}...",
        })
        download_dir = aria2_cfg.get("download_dir", "")
        gids = await aria2.add_uris_batch(tasks_to_add, base_dir=download_dir)
        for sequence, file_info in enumerate(files, 1):
            await _broadcast_link_pushed(index, file_info, sequence, len(files), push_target)
        await _broadcast({"type": "push_done", "index": index,
                          "success_count": len(gids), "total_count": len(files),
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


async def _process_magnets(magnets: List[str]):
    from pikpakapi.enums import DownloadStatus
    import time

    cfg = load_config()
    pikpak_cfg = cfg.get("pikpak", {})
    poll_interval = pikpak_cfg.get("poll_interval", 3)
    max_wait_time = pikpak_cfg.get("max_wait_time", 3600)
    delete_after = pikpak_cfg.get("delete_after_download", False)
    engine = _get_engine()

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

        # 按引擎分发
        delete_ids = [file_id] if delete_after else None
        engine_label = _get_push_target_label(engine)
        await _broadcast({"type": "task_status", "index": i,
                          "status": f"解析完成，共 {len(files)} 个文件，开始推送下载链接到{engine_label}..."})
        if engine == "builtin":
            await _builtin_download_and_upload(files, i, delete_pikpak_ids=delete_ids)
        else:
            await _aria2_push_only(files, i, delete_pikpak_ids=delete_ids)

        await _broadcast({"type": "task_done", "index": i, "file_name": file_name})

    await _broadcast({"type": "all_done", "total": total})


async def _process_magnet_selected(root_file_id: str, selected_ids: List[str],
                                    keep_structure: bool = True):
    try:
        pikpak, _ = await _ensure_clients()
        cfg = load_config()
        delete_after = cfg.get("pikpak", {}).get("delete_after_download", False)
        engine = _get_engine()

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
        await _broadcast({"type": "task_status", "index": 1,
                          "status": f"解析完成，共 {len(files)} 个文件，开始推送下载链接到{_get_push_target_label(engine)}..."})

        delete_ids = [root_file_id] if delete_after else None
        if engine == "builtin":
            await _builtin_download_and_upload(files, 1, delete_pikpak_ids=delete_ids)
        else:
            await _aria2_push_only(files, 1, delete_pikpak_ids=delete_ids)

        await _broadcast({"type": "all_done", "total": 1})
    except Exception as e:
        await _broadcast({"type": "error", "message": f"选择下载失败: {e}"})


async def _process_share_download(share_id: str, file_ids: List[str], pass_code_token: str,
                                   keep_structure: bool = True, file_paths: Dict[str, str] = None,
                                   rename_by_folder: bool = False):
    saved_ids: List[str] = []
    try:
        await _ensure_clients()
        total = len(file_ids)
        cfg = load_config()
        engine = _get_engine()
        pikpak_cfg = cfg.get("pikpak", {})
        share_url_timeout = float(pikpak_cfg.get("share_download_url_timeout", 60))
        share_poll_interval = float(pikpak_cfg.get("share_download_url_poll_interval", 3))

        await _broadcast({"type": "task_start", "index": 1, "total": total,
                          "magnet": f"分享文件 ({total} 个)"})
        await _broadcast({"type": "task_status", "index": 1,
                          "status": "正在保存分享内容到 PikPak 网盘..."})
        saved_ids = await _pikpak.save_share_files(share_id, file_ids, pass_code_token)
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

        if not all_urls:
            await _broadcast({"type": "error", "message": "所有文件链接获取失败"})
            return

        await _broadcast({"type": "task_status", "index": 1,
                          "status": f"全部链接解析完成，共 {len(all_urls)} 个文件，开始推送下载链接到{_get_push_target_label(engine)}..."})
        if engine == "builtin":
            await _builtin_download_and_upload(all_urls, 1, delete_pikpak_ids=saved_ids)
            saved_ids = []
        else:
            aria2_cfg = cfg.get("aria2", {})
            success_count = 0
            download_dir = aria2_cfg.get("download_dir", "")
            push_target = _get_push_target_label("aria2")
            for sequence, url_info in enumerate(all_urls, 1):
                try:
                    opts = {}
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
