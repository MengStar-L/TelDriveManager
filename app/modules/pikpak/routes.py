"""PikPak 模块 API 路由 — 从 AutoPikDown web_server.py 迁移到 FastAPI"""

import asyncio
import posixpath
import re
import logging
from typing import Dict, List, Optional, Set

import feedparser
import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse

from app.config import load_config, save_config
from app.modules.pikpak.client import PikPakClient
from app.aria2_client import Aria2Client

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
    dead = set()
    data = json.dumps(msg, ensure_ascii=False)
    for ws in _ws_clients:
        try:
            await ws.send_text(data)
        except Exception:
            dead.add(ws)
    _ws_clients -= dead


def register_ws(ws: WebSocket):
    _ws_clients.add(ws)


def unregister_ws(ws: WebSocket):
    _ws_clients.discard(ws)


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

async def _process_magnets(magnets: List[str]):
    from pikpakapi.enums import DownloadStatus
    import time

    cfg = load_config()
    pikpak_cfg = cfg.get("pikpak", {})
    aria2_cfg = cfg.get("aria2", {})
    poll_interval = pikpak_cfg.get("poll_interval", 3)
    max_wait_time = pikpak_cfg.get("max_wait_time", 3600)
    delete_after = pikpak_cfg.get("delete_after_download", False)

    try:
        pikpak, aria2 = await _ensure_clients()
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

        if not task_id:
            await _broadcast({"type": "task_error", "index": i, "message": "未获取到 task_id"})
            continue

        start_time = time.time()
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
                await _broadcast({"type": "task_status", "index": i, "status": status.value})
                last_status = status
            if status == DownloadStatus.done:
                break
            elif status in (DownloadStatus.error, DownloadStatus.not_found):
                await _broadcast({"type": "task_error", "index": i, "message": f"离线失败 ({status.value})"})
                break
            await asyncio.sleep(poll_interval)
        else:
            continue

        if last_status != DownloadStatus.done:
            continue

        try:
            files = await pikpak.get_download_urls(file_id)
        except Exception as e:
            await _broadcast({"type": "task_error", "index": i, "message": f"获取下载链接失败: {e}"})
            continue

        if not files:
            await _broadcast({"type": "task_error", "index": i, "message": "未找到可下载的文件"})
            continue

        await _broadcast({"type": "files_found", "index": i, "files": [f["name"] for f in files]})

        try:
            tasks_to_add = []
            for f in files:
                path = f.get("path", f["name"])
                parent_dir = posixpath.dirname(path)
                tasks_to_add.append({"url": f["url"], "name": f["name"],
                                     "subdir": parent_dir if parent_dir else None})
            download_dir = aria2_cfg.get("download_dir", "")
            gids = await aria2.add_uris_batch(tasks_to_add, base_dir=download_dir)
            await _broadcast({"type": "aria2_done", "index": i,
                              "success_count": len(gids), "total_count": len(files)})
        except Exception as e:
            await _broadcast({"type": "task_error", "index": i, "message": f"推送 Aria2 失败: {e}"})
            continue

        if delete_after:
            try:
                await pikpak.delete_files([file_id])
            except Exception:
                pass

        await _broadcast({"type": "task_done", "index": i, "file_name": file_name})

    await _broadcast({"type": "all_done", "total": total})


async def _process_magnet_selected(root_file_id: str, selected_ids: List[str],
                                    keep_structure: bool = True):
    try:
        pikpak, aria2 = await _ensure_clients()
        cfg = load_config()
        delete_after = cfg.get("pikpak", {}).get("delete_after_download", False)
        aria2_cfg = cfg.get("aria2", {})

        await _broadcast({"type": "task_start", "index": 1, "total": 1,
                          "magnet": f"磁链选择下载 ({len(selected_ids)} 个文件)"})
        await _broadcast({"type": "task_status", "index": 1, "status": "获取下载链接..."})
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

        tasks_to_add = []
        for f in files:
            path = f.get("path", f["name"])
            parent_dir = posixpath.dirname(path) if keep_structure else None
            tasks_to_add.append({"url": f["url"], "name": f["name"],
                                 "subdir": parent_dir if parent_dir else None})
        download_dir = aria2_cfg.get("download_dir", "")
        gids = await aria2.add_uris_batch(tasks_to_add, base_dir=download_dir)
        await _broadcast({"type": "aria2_done", "index": 1,
                          "success_count": len(gids), "total_count": len(files)})
        if delete_after:
            try:
                await pikpak.delete_files([root_file_id])
            except Exception:
                pass
        await _broadcast({"type": "all_done", "total": 1})
    except Exception as e:
        await _broadcast({"type": "error", "message": f"选择下载失败: {e}"})


async def _process_share_download(share_id: str, file_ids: List[str], pass_code_token: str,
                                   keep_structure: bool = True, file_paths: Dict[str, str] = None,
                                   rename_by_folder: bool = False):
    try:
        await _ensure_clients()
        total = len(file_ids)
        cfg = load_config()
        aria2_cfg = cfg.get("aria2", {})

        await _broadcast({"type": "task_start", "index": 1, "total": total,
                          "magnet": f"分享文件 ({total} 个)"})
        await _broadcast({"type": "task_status", "index": 1, "status": "正在保存到网盘..."})
        saved_ids = await _pikpak.save_share_files(share_id, file_ids, pass_code_token)
        if not saved_ids:
            await _broadcast({"type": "task_error", "index": 1, "message": "保存失败"})
            return

        success_count = 0
        for i, fid in enumerate(saved_ids, 1):
            await _broadcast({"type": "task_status", "index": i, "status": f"获取下载链接 [{i}/{len(saved_ids)}]"})
            urls = []
            for attempt in range(3):
                try:
                    urls = await asyncio.wait_for(_pikpak.get_download_urls(fid), timeout=30.0)
                    if urls:
                        break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(2)
            if not urls:
                await _broadcast({"type": "task_error", "index": i, "message": "获取链接失败"})
                continue
            try:
                for url_info in urls:
                    subdir = None
                    if keep_structure:
                        path = url_info.get("path", "")
                        if path:
                            parts = path.split("/")
                            if len(parts) > 2:
                                subdir = "/".join(parts[1:-1]) or None
                    opts = {}
                    download_dir = aria2_cfg.get("download_dir", "")
                    if download_dir:
                        if subdir:
                            import os
                            opts["dir"] = os.path.join(download_dir, subdir).replace("\\", "/")
                        else:
                            opts["dir"] = download_dir
                    name = url_info.get("name", "")
                    if name:
                        opts["out"] = name
                    gid = await _aria2.add_uri(url_info["url"], opts)
                    if gid:
                        success_count += 1
                        await _broadcast({"type": "task_added", "index": i, "file_name": name})
            except Exception as e:
                await _broadcast({"type": "task_error", "index": i, "message": str(e)})

        await _broadcast({"type": "aria2_done", "index": 1,
                          "success_count": success_count, "total_count": len(saved_ids)})
        if saved_ids:
            await _broadcast({"type": "task_status", "index": 1, "status": "正在删除网盘文件..."})
            await _pikpak.delete_files(saved_ids)
        await _broadcast({"type": "all_done", "total": total})
    except Exception as e:
        await _broadcast({"type": "error", "message": f"分享下载失败: {e}"})
