"""Conservative, journaled recovery of stalled HTTP download caches."""

import asyncio
import functools
import json
import os
import re
import stat
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp

from app import database as db
from app.aria2_client import Aria2RPCError
from app.disk_budget import disk_budget


@asynccontextmanager
async def download_changes(manager):
    current = asyncio.current_task()
    if getattr(manager, "_download_mutation_owner", None) is current:
        yield
        return
    async with manager._download_mutation_lock:
        manager._download_mutation_owner = current
        try:
            yield
        finally:
            manager._download_mutation_owner = None


def download_creation(method):
    @functools.wraps(method)
    async def guarded(self, *args, **kwargs):
        async with download_changes(self):
            return await method(self, *args, **kwargs)
    return guarded


def download_mutation(method):
    @functools.wraps(method)
    async def guarded(self, task_id, *args, **kwargs):
        async with download_changes(self):
            task = await db.get_task(task_id)
            if task and task.get("disk_recovery_json") not in (None, "", "{}"):
                if method.__name__ in ("cancel_task", "delete_task"):
                    try:
                        await self._disk_recovery.prepare_manual_cleanup(task)
                    except Exception as exc:
                        return {"success": False, "message": str(exc)}
                    return await method(self, task_id, *args, **kwargs)
                return {"success": False, "message": "磁盘缓存回收尚未完成，请稍后重试"}
            return await method(self, task_id, *args, **kwargs)
    return guarded


async def http_file_size(url, options):
    """Inspect response headers without downloading a body or creating a file."""
    if urlsplit(url).scheme not in ("http", "https"):
        return None
    headers = {}
    raw = options.get("header") or []
    for line in raw.splitlines() if isinstance(raw, str) else raw:
        name, separator, value = str(line).partition(":")
        if separator:
            headers[name.strip()] = value.strip()
    headers = {k: v for k, v in headers.items() if k.lower() not in ("range", "accept-encoding")}
    headers["Accept-Encoding"] = "identity"
    for option, header in (("user-agent", "User-Agent"), ("referer", "Referer")):
        if options.get(option):
            headers[header] = options[option]
    auth = None
    if options.get("http-user"):
        auth = aiohttp.BasicAuth(options["http-user"], options.get("http-passwd", ""))
    timeout = aiohttp.ClientTimeout(total=8, connect=3)
    async with aiohttp.ClientSession(timeout=timeout, auto_decompress=False) as session:
        for method in ("HEAD", "GET"):
            request_headers = dict(headers)
            if method == "GET":
                request_headers["Range"] = "bytes=0-0"
            async with session.request(method, url, headers=request_headers, auth=auth,
                                       proxy=options.get("all-proxy") or None,
                                       allow_redirects=True, max_redirects=5) as response:
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    return None
                if response.status == 206:
                    match = re.fullmatch(r"bytes 0-0/(\d+)", response.headers.get("Content-Range", ""))
                    return int(match[1]) if match else None
                if response.status == 200:
                    length = response.headers.get("Content-Length", "")
                    if length.isdecimal():
                        return int(length)
                if response.status in (401, 403, 404, 410):
                    return None
    return None


def file_identity(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise RuntimeError("Cache is not an exclusively owned regular file")
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns]


def allocated_bytes(path):
    info = path.stat()
    if hasattr(info, "st_blocks"):
        return info.st_blocks * 512
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        get_size = kernel.GetCompressedFileSizeW
        get_size.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
        get_size.restype = wintypes.DWORD
        high = wintypes.DWORD()
        ctypes.set_last_error(0)
        low = get_size(str(path), ctypes.byref(high))
        if low == 0xFFFFFFFF and ctypes.get_last_error():
            raise ctypes.WinError(ctypes.get_last_error())
        return (high.value << 32) | low
    return 0


class DiskRecovery:
    def __init__(self, manager):
        self.manager = manager
        self.next_attempt = 0.0
        self.next_journal_attempt = 0.0
        self.source_retry_at = {}
        self.message = ""

    def safe_path(self, value):
        path = Path(os.path.abspath(value))
        root = Path(self.manager.config["aria2"]["download_dir"])
        if not root.is_absolute():
            root = Path(__file__).resolve().parents[3] / root
        root = root.resolve()
        if path == root or root not in path.parents or path != path.resolve():
            raise RuntimeError("Cache path is outside the download directory")
        for entry in (path, *path.parents):
            if entry.is_symlink() or (hasattr(entry, "is_junction") and entry.is_junction()):
                raise RuntimeError("Cache path contains a filesystem link")
            if entry == root:
                break
        if path.exists():
            file_identity(path)
        return path

    async def cache_claims(self, items):
        claims = []
        tasks = await db.get_all_tasks()
        owners = {task.get("aria2_gid"): task["task_id"] for task in tasks}
        for task in tasks:
            paths = self.manager._queued_local_candidates(task)
            if task.get("local_path"):
                paths.append(self.manager._get_upload_path(task["local_path"]))
            claims.extend((task["task_id"], path) for path in paths if path)
        for path in await db.get_telegram_relay_cache_paths():
            if path:
                claims.extend([(None, path), (None, path + ".part")])
        for item in items:
            for entry in item.get("files", []):
                if entry.get("path"):
                    owner = owners.get(item.get("gid"))
                    claims.extend([(owner, entry["path"]), (owner, entry["path"] + ".aria2")])
        return claims

    async def assert_exclusive(self, task_id, paths, items, claims=None):
        if claims is None:
            claims = await self.cache_claims(items)
        for owner, value in claims:
            if owner == task_id:
                continue
            other = Path(value).resolve()
            if any(path == other or path in other.parents or other in path.parents for path in paths):
                raise RuntimeError("Cache is shared with another transfer")

    async def status(self, gid):
        try:
            return await self.manager._require_aria2().tell_status(gid)
        except Aria2RPCError as exc:
            if exc.code == 1 and gid in exc.message and "not found" in exc.message.lower():
                return None
            raise

    async def source_options(self, task):
        # Preserve authentication and the actual destination when rebuilding a task.
        options = await self.manager._require_aria2().get_option(task["aria2_gid"])
        options.update(self.manager._prepare_aria2_options(task))
        return options

    async def prepare_manual_cleanup(self, task):
        gid = json.loads(task["disk_recovery_json"])["gid"]
        status = await self.status(gid)
        if status and status.get("status") == "paused":
            await self.manager._require_aria2().remove_for_recovery(gid)
            status = await self.status(gid)
        if status and status.get("status") not in ("removed", "error"):
            raise RuntimeError("Cannot confirm download stopped; cache retained")
        await self.manager._require_aria2().save_session()
        await db.update_task(task["task_id"], disk_recovery_json="{}", aria2_gid=None)
        disk_budget.release("aria2:" + gid)
        self.manager._discard_disk_gate_gid(gid)
        self.manager._terminal_gids.add(gid)

    async def candidate(self, item, tasks, items, claims):
        gid = item["gid"]
        task = tasks.get(gid)
        if (not task or task.get("status") not in ("pending", "downloading")
                or task.get("disk_recovery_json") not in (None, "", "{}")
                or self.manager._is_upload_stage_task(task)
                or not self.manager._is_disk_gate_held(gid)
                or urlsplit(task.get("url") or "").scheme not in ("http", "https")
                or item.get("bittorrent") or len(item.get("files", [])) != 1):
            return None
        fresh = item
        if not fresh or fresh.get("status") != "paused" or fresh.get("bittorrent"):
            return None
        files = fresh.get("files", [])
        if len(files) != 1:
            return None
        total = max(int(fresh.get("totalLength") or 0), int(task.get("source_size_bytes") or 0))
        done = int(fresh.get("completedLength") or 0)
        if not 0 <= done < total:
            return None
        path = self.safe_path(files[0]["path"])
        if task.get("local_path") and Path(task["local_path"]).resolve() != path:
            return None
        control = self.safe_path(str(path) + ".aria2")
        await self.assert_exclusive(task["task_id"], [path, control], items, claims)
        # A donor must have a real checkpoint. Zero-byte queued tasks can only receive space.
        reclaimable = allocated_bytes(path) if path.is_file() and control.is_file() else 0
        return dict(task=task, item=fresh, path=path, control=control, total=total,
                    remaining=self.manager._aria2_remaining_bytes(fresh, task), reclaimable=reclaimable,
                    volume=disk_budget.filesystem(path.parent)[0])

    async def finish_journal(self, task, items):
        manager = self.manager
        journal = json.loads(task["disk_recovery_json"])
        gid = journal["gid"]
        path = self.safe_path(journal["path"])
        control = self.safe_path(str(path) + ".aria2")
        if manager._is_upload_stage_task(task) or task.get("aria2_gid") != gid:
            raise RuntimeError("Task changed while recovering its download cache")
        status = await self.status(gid)
        if status and status.get("status") not in ("paused", "removed", "error"):
            raise RuntimeError("Cannot confirm download is stopped; cache retained")
        if (status and int(status.get("totalLength") or 0) > 0
                and int(status.get("completedLength") or 0) >= int(status["totalLength"])):
            raise RuntimeError("Download may now be complete; cache retained")
        if path.exists():
            if await http_file_size(task["url"], journal["options"]) != journal["total"]:
                raise RuntimeError("Source unavailable or changed; cache retained")
        if journal["phase"] == "prepared":
            if path.exists() and file_identity(path) != journal["identity"]:
                raise RuntimeError("Partial file changed; cache retained")
        aria2 = manager._require_aria2()
        if status and status.get("status") == "paused":
            await aria2.remove_for_recovery(gid)
        # Flush removal before unlinking so an old session cannot resurrect a writer on restart.
        await aria2.save_session()
        if journal["phase"] == "prepared":
            if path.exists() and file_identity(path) != journal["identity"]:
                raise RuntimeError("Partial file changed while stopping; cache retained")
            journal["phase"] = "stopped"
            journal["files"] = {str(p): file_identity(p) if p.exists() else None for p in (path, control)}
            await db.update_task(task["task_id"], disk_recovery_json=json.dumps(journal))
        fresh_items = (await aria2.tell_active() or []) + (await aria2.tell_waiting_all() or [])
        if any(item.get("gid") == gid for item in fresh_items):
            raise RuntimeError("aria2 still holds the download; cache retained")
        await self.assert_exclusive(task["task_id"], [path, control], fresh_items)
        # No awaits between identity verification and deletion of these exact regular files.
        for value, identity in journal["files"].items():
            target = self.safe_path(value)
            if target not in (path, control):
                raise RuntimeError("Invalid recovery journal path")
            if target.exists() and (identity is None or file_identity(target) != identity):
                raise RuntimeError("Cache was replaced; replacement retained")
        for value in journal["files"]:
            if journal["files"][value] is not None:
                Path(value).unlink(missing_ok=True)
        await db.update_task(task["task_id"], status="pending", aria2_gid=None, local_path=None,
                             download_progress=0, download_speed="", source_size_bytes=journal["total"],
                             aria2_options_json=manager._serialize_aria2_options(journal["options"]),
                             disk_recovery_json="{}", error="磁盘空间不足，已回收未完成缓存，等待重新下载")
        disk_budget.release("aria2:" + gid)
        manager._discard_disk_gate_gid(gid)
        manager._serial_gate_paused_gids.discard(gid)
        manager._known_gids.discard(gid)
        manager._terminal_gids.add(gid)
        for item in items:
            if item.get("gid") == gid:
                item["status"] = "removed"
                item["_disk_removed"] = True
        await manager._broadcast_task_update(task["task_id"])

    async def resume_interrupted(self):
        if time.monotonic() < self.next_journal_attempt:
            return
        self.next_journal_attempt = time.monotonic() + 30
        async with download_changes(self.manager):
            for task in await db.get_all_tasks():
                if task.get("disk_recovery_json") in (None, "", "{}"):
                    continue
                try:
                    await self.finish_journal(task, [])
                except Exception as exc:
                    self.message = f"缓存回收等待重试: {exc}"
                    await db.update_task(task["task_id"], error=self.message)

    async def run(self, items):
        if time.monotonic() < self.next_attempt or not any(item.get("files") for item in items):
            return None
        self.next_attempt = time.monotonic() + 30
        self.message = ""
        manager = self.manager
        self.source_retry_at = {gid: after for gid, after in self.source_retry_at.items() if after > time.monotonic()}
        async with download_changes(manager):
            # Let complete-file uploads release their cache before discarding partial work.
            if any(not task.done() for task in manager._upload_tasks.values()):
                return None
            tasks = {task.get("aria2_gid"): task for task in await db.get_all_tasks()}
            claims = await self.cache_claims(items)
            candidates = []
            for item in items:
                try:
                    candidate = await self.candidate(item, tasks, items, claims)
                    if candidate:
                        candidates.append(candidate)
                except Exception:
                    continue
            threshold = manager._get_disk_protection_threshold_bytes()
            attempts = 0
            for target in sorted(candidates, key=lambda item: item["remaining"]):
                gid = target["item"]["gid"]
                if time.monotonic() < self.source_retry_at.get(gid, 0):
                    continue
                owner = "aria2:" + gid
                peers = [item for item in candidates if item is not target
                         and item["volume"] == target["volume"] and item["reclaimable"] > 0
                         and time.monotonic() >= self.source_retry_at.get(item["item"]["gid"], 0)]
                available = disk_budget.available(owner, target["path"].parent, threshold)
                if available + sum(item["reclaimable"] for item in peers) < target["remaining"]:
                    continue
                selected = []
                for donor in sorted(peers, key=lambda item: -item["reclaimable"]):
                    if available >= target["remaining"]:
                        break
                    selected.append(donor)
                    available += donor["reclaimable"]
                if not selected:
                    continue
                attempts += 1
                if attempts > 3:
                    break
                try:
                    for entry in [target, *selected]:
                        try:
                            entry["options"] = await self.source_options(entry["task"])
                            if await http_file_size(entry["task"]["url"], entry["options"]) != entry["total"]:
                                raise RuntimeError("Source unavailable or size changed")
                        except Exception:
                            self.source_retry_at[entry["item"]["gid"]] = time.monotonic() + 60
                            raise
                except Exception:
                    continue
                # Claim freed bytes before deletion; relay/new downloads cannot take this space.
                fresh_target = await self.status(gid)
                if not fresh_target or fresh_target.get("status") != "paused":
                    continue
                disk_budget.track(owner, target["path"].parent, target["remaining"], threshold)
                resumed = False
                try:
                    for donor in selected:
                        task = donor["task"]
                        options = dict(donor["options"], dir=str(donor["path"].parent), out=donor["path"].name)
                        journal = dict(phase="prepared", gid=donor["item"]["gid"], path=str(donor["path"]),
                                       total=donor["total"], options=options, identity=file_identity(donor["path"]))
                        await db.update_task(task["task_id"], status="paused", disk_recovery_json=json.dumps(journal))
                        await self.finish_journal(await db.get_task(task["task_id"]), items)
                        if disk_budget.available(owner, target["path"].parent, threshold) >= target["remaining"]:
                            break
                    fresh = await self.status(gid)
                    if not fresh or fresh.get("status") != "paused":
                        raise RuntimeError("Recovery target changed")
                    await manager._reserve_aria2_item(fresh)
                    resumed = await manager._release_from_disk_gate(gid)
                    if resumed:
                        for item in items:
                            if item["gid"] == gid:
                                item["status"] = "waiting"
                        self.message = "已回收未完成缓存，优先完成一个下载；被回收任务将重新排队"
                        manager._clear_runtime_task_fields(target["task"]["task_id"], "download_note")
                        return gid
                except Exception as exc:
                    self.message = f"缓存回收等待重试: {exc}"
                finally:
                    if not resumed:
                        disk_budget.release(owner)
                return None
            self.message = "没有足够的可安全回收缓存；等待上传释放空间或增加磁盘容量"
        return None
