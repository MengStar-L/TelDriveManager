"""PikPak API 客户端封装"""

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from pikpakapi import PikPakApi
from pikpakapi.enums import DownloadStatus

logger = logging.getLogger(__name__)

TOKEN_FILE = Path(__file__).resolve().parent.parent.parent.parent / "pikpak_token.json"


class PikPakClient:
    """封装 PikPakApi，提供离线下载 → 获取直链的完整流程"""

    def __init__(self, username: str, password: str, save_dir: str = "/"):
        self.username = username
        self.password = password
        self.save_dir = save_dir
        self._save_dir_id: Optional[str] = None

        saved_token = self._load_token()
        if saved_token:
            self.client = PikPakApi(
                username=username,
                password=password,
                encoded_token=saved_token,
                token_refresh_callback=PikPakClient._on_token_refresh,
            )
        else:
            self.client = PikPakApi(
                username=username,
                password=password,
                token_refresh_callback=PikPakClient._on_token_refresh,
            )

    def _load_token(self) -> Optional[str]:
        try:
            if TOKEN_FILE.exists():
                data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
                if data.get("username") == self.username:
                    return data.get("encoded_token")
        except Exception:
            pass
        return None

    def _save_token(self):
        try:
            if self.client.encoded_token:
                TOKEN_FILE.write_text(
                    json.dumps({
                        "username": self.username,
                        "encoded_token": self.client.encoded_token,
                    }, ensure_ascii=False),
                    encoding="utf-8",
                )
        except Exception:
            pass

    @staticmethod
    async def _on_token_refresh(client: PikPakApi, **kwargs):
        try:
            if client.encoded_token:
                TOKEN_FILE.write_text(
                    json.dumps({
                        "username": client.username,
                        "encoded_token": client.encoded_token,
                    }, ensure_ascii=False),
                    encoding="utf-8",
                )
        except Exception:
            pass

    async def login(self):
        if self.client.refresh_token:
            try:
                await self.client.refresh_access_token()
                self._save_token()
                return
            except Exception:
                pass
        await self.client.login()
        self._save_token()

    async def _get_save_dir_id(self) -> Optional[str]:
        if self._save_dir_id is not None:
            return self._save_dir_id
        if self.save_dir in ("/", ""):
            return None
        result = await self.client.path_to_id(self.save_dir, create=True)
        if result:
            self._save_dir_id = result[-1]["id"]
            return self._save_dir_id
        return None

    async def add_offline_task(self, magnet_url: str, name: Optional[str] = None) -> Dict[str, Any]:
        parent_id = await self._get_save_dir_id()
        result = await self.client.offline_download(
            file_url=magnet_url, parent_id=parent_id, name=name,
        )
        task = result.get("task", {})
        return {
            "task_id": task.get("id", ""),
            "file_id": task.get("file_id", ""),
            "file_name": task.get("file_name", "未知"),
            "raw": result,
        }

    async def wait_for_task(self, task_id: str, file_id: str,
                            poll_interval: float = 3.0, max_wait_time: float = 3600.0) -> DownloadStatus:
        start_time = time.time()
        last_status = None
        poll_count = 0
        while True:
            elapsed = int(time.time() - start_time)
            if elapsed > max_wait_time:
                logger.warning(f"转存等待超时 ({elapsed}s)，放弃")
                return DownloadStatus.error
            try:
                status = await self.client.get_task_status(task_id, file_id)
            except Exception as e:
                poll_count += 1
                if poll_count % 3 == 0:
                    logger.info(f"转存轮询中... 已等待 {elapsed}s (查询异常: {e})")
                await asyncio.sleep(poll_interval)
                continue
            poll_count += 1
            if status != last_status:
                logger.info(f"转存状态变更: {last_status} -> {status} (已等待 {elapsed}s)")
                last_status = status
            elif poll_count % 5 == 0:
                logger.info(f"转存进行中... 状态={status}, 已等待 {elapsed}s")
            if status == DownloadStatus.done:
                logger.info(f"转存完成！耗时 {elapsed}s")
                return status
            elif status in (DownloadStatus.error, DownloadStatus.not_found):
                logger.warning(f"转存异常终止: {status}, 耗时 {elapsed}s")
                return status
            await asyncio.sleep(poll_interval)

    async def get_download_urls(self, file_id: str) -> List[Dict[str, str]]:
        info = await self.client.get_download_url(file_id)
        kind = info.get("kind", "")
        if kind == "drive#folder":
            folder_name = info.get("name", "")
            return await self._list_folder_files(file_id, prefix=folder_name)
        url = info.get("web_content_link", "")
        name = info.get("name", "未知文件")
        if not url:
            medias = info.get("medias", [])
            if medias:
                url = medias[0].get("link", {}).get("url", "")
        if url:
            return [{"name": name, "url": url, "file_id": file_id, "path": name}]
        return []

    async def _list_folder_files(self, folder_id: str, prefix: str = "") -> List[Dict[str, str]]:
        results = []
        next_page_token = None
        while True:
            resp = await self.client.file_list(parent_id=folder_id, next_page_token=next_page_token)
            for f in resp.get("files", []):
                kind = f.get("kind", "")
                fid = f.get("id", "")
                name = f.get("name", "")
                full_path = f"{prefix}/{name}" if prefix else name
                if kind == "drive#folder":
                    sub_files = await self._list_folder_files(fid, prefix=full_path)
                    results.extend(sub_files)
                else:
                    url = f.get("web_content_link", "")
                    if url:
                        results.append({"name": name, "url": url, "file_id": fid, "path": full_path})
                    else:
                        try:
                            info = await self.client.get_download_url(fid)
                            dl_url = info.get("web_content_link", "")
                            if not dl_url:
                                medias = info.get("medias", [])
                                if medias:
                                    dl_url = medias[0].get("link", {}).get("url", "")
                            if dl_url:
                                results.append({"name": name, "url": dl_url, "file_id": fid, "path": full_path})
                        except Exception:
                            pass
            next_page_token = resp.get("next_page_token")
            if not next_page_token:
                break
        return results

    async def list_file_tree(self, file_id: str) -> List[Dict[str, Any]]:
        info = await self.client.get_download_url(file_id)
        kind = info.get("kind", "")
        name = info.get("name", "未知")
        if kind != "drive#folder":
            return [{
                "id": file_id, "name": name, "path": name,
                "size": int(info.get("size", 0)), "kind": kind,
                "file_type": info.get("mime_type", ""),
            }]
        results: List[Dict[str, Any]] = []
        await self._collect_file_tree(file_id, results, prefix=name)
        return results

    async def _collect_file_tree(self, folder_id: str, results: List[Dict], prefix: str = ""):
        next_page_token = None
        while True:
            resp = await self.client.file_list(parent_id=folder_id, next_page_token=next_page_token)
            for f in resp.get("files", []):
                fid = f.get("id", "")
                name = f.get("name", "")
                kind = f.get("kind", "")
                full_path = f"{prefix}/{name}" if prefix else name
                results.append({
                    "id": fid, "name": name, "path": full_path,
                    "size": int(f.get("size", 0)), "kind": kind,
                    "file_type": f.get("mime_type", ""),
                })
                if kind == "drive#folder":
                    await self._collect_file_tree(fid, results, prefix=full_path)
            next_page_token = resp.get("next_page_token")
            if not next_page_token:
                break

    async def delete_files(self, file_ids: List[str]):
        if file_ids:
            await self.client.delete_forever(file_ids)

    async def get_offline_tasks(self) -> List[Dict[str, Any]]:
        result = await self.client.offline_list()
        return result.get("tasks", [])

    # ── 分享链接相关 ──

    async def get_share_file_list(self, share_link: str, pass_code: str = "") -> Dict[str, Any]:
        match = re.search(r"/s/([^/?#]+)", share_link)
        if not match:
            raise ValueError("无效的分享链接格式")
        share_id = match.group(1)
        result = await self.client.get_share_info(share_link, pass_code or None)
        if isinstance(result, ValueError):
            raise result
        pass_code_token = result.get("pass_code_token", "")
        files: List[Dict] = []
        for item in result.get("files", []):
            await self._collect_share_files(share_id, pass_code_token, item, files)
        return {"share_id": share_id, "pass_code_token": pass_code_token, "files": files}

    async def _collect_share_files(self, share_id: str, pass_code_token: str,
                                    file_info: Dict, files: List[Dict], prefix: str = ""):
        kind = file_info.get("kind", "")
        file_id = file_info.get("id", "")
        name = file_info.get("name", "")
        full_path = f"{prefix}/{name}" if prefix else name
        if kind == "drive#folder":
            resp = await self.client.get_share_folder(share_id, pass_code_token, parent_id=file_id)
            for f in resp.get("files", []):
                await self._collect_share_files(share_id, pass_code_token, f, files, full_path)
        elif kind == "drive#file":
            files.append({
                "id": file_id, "name": name, "path": full_path,
                "size": int(file_info.get("size", 0)),
                "file_type": file_info.get("mime_type", ""),
                "icon_link": file_info.get("icon_link", ""),
            })

    async def save_share_files(self, share_id: str, file_ids: List[str],
                                pass_code_token: str) -> tuple:
        """
        转存分享文件到网盘，返回 (saved_ids, restore_task_id)。
        对于异步转存（大文件），会轮询任务状态直到完成，然后从任务详情中提取真实文件 ID。
        """
        result = await self.client.restore(share_id, pass_code_token, file_ids)
        logger.info(f"restore 响应: {json.dumps(result, ensure_ascii=False)}")
        
        restore_task_id = result.get("restore_task_id", "")
        restore_status = result.get("restore_status", "")
        
        saved_ids = []
        # 优先从 task_info 中提取（某些场景下 API 会直接返回）
        for task_info in result.get("task_info", []):
            fid = task_info.get("file_id", "")
            if fid:
                saved_ids.append(fid)
        
        # 如果有 task_info 就直接返回（同步完成的场景）
        if saved_ids:
            return saved_ids, restore_task_id
        
        # 异步转存场景：需要轮询任务状态获取真实文件 ID
        if restore_status == "RESTORE_START" and restore_task_id:
            logger.info(f"转存为异步模式，开始轮询任务 {restore_task_id}")
            actual_ids = await self._poll_restore_task(restore_task_id)
            if actual_ids:
                return actual_ids, restore_task_id
        
        # 最终 fallback：用 restore 返回的 file_id（可能是根目录，但聊胜于无）
        root_id = result.get("file_id", "")
        if root_id:
            saved_ids.append(root_id)
        return saved_ids, restore_task_id

    async def _poll_restore_task(self, task_id: str, 
                                  poll_interval: float = 3.0, 
                                  max_wait: float = 300.0) -> List[str]:
        """轮询离线任务状态，直到完成后从任务详情中提取真实目标文件 ID"""
        import httpx
        start = time.time()
        
        while True:
            elapsed = int(time.time() - start)
            if elapsed > max_wait:
                logger.warning(f"转存任务轮询超时 ({elapsed}s)")
                return []
            
            try:
                url = f"https://api-drive.mypikpak.com/drive/v1/tasks/{task_id}"
                headers = {"Authorization": f"Bearer {self.client.access_token}"}
                async with httpx.AsyncClient() as hc:
                    resp = await hc.get(url, headers=headers)
                    task = resp.json()
                
                phase = task.get("phase", "")
                file_id = task.get("file_id", "")
                file_name = task.get("file_name", task.get("name", ""))
                
                logger.info(f"转存轮询: phase={phase}, file_id={file_id}, name={file_name}, 已等待 {elapsed}s")
                
                if phase == "PHASE_TYPE_COMPLETE" and file_id:
                    logger.info(f"转存完成！目标文件 ID={file_id}, 名称={file_name}, 耗时 {elapsed}s")
                    return [file_id]
                elif phase in ("PHASE_TYPE_ERROR", "PHASE_TYPE_FAILED"):
                    logger.error(f"转存任务失败: phase={phase}")
                    return []
                    
            except Exception as e:
                logger.warning(f"转存轮询异常: {e}, 已等待 {elapsed}s")
            
            await asyncio.sleep(poll_interval)

