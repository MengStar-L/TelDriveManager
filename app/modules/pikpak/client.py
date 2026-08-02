"""PikPak API 客户端封装"""

import asyncio
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from pikpakapi import PikPakApi
from pikpakapi.enums import DownloadStatus

logger = logging.getLogger(__name__)

TOKEN_FILE = Path(__file__).resolve().parent.parent.parent.parent / "pikpak_token.json"


@dataclass(frozen=True)
class ShareRestoreReceipt:
    scope_id: str
    task_id: str
    selected_ids: tuple[str, ...]


class _ShareRestoreValidationError(RuntimeError):
    pass


def _parse_pikpak_share_location(share_link: str) -> tuple[str, Optional[str]]:
    value = str(share_link or "").strip()
    parsed = urlsplit(value if "://" in value else f"https://{value}")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) not in {2, 3} or parts[0].lower() != "s":
        raise ValueError("无效的 PikPak 分享链接格式")
    share_id = parts[1].strip()
    target_id = parts[2].strip() if len(parts) == 3 else None
    if not share_id or (len(parts) == 3 and not target_id):
        raise ValueError("无效的 PikPak 分享链接格式")
    return share_id, target_id


class PikPakClient:
    """封装 PikPakApi，提供离线下载 → 获取直链的完整流程"""

    def __init__(self, username: str = "", password: str = "", save_dir: str = "/",
                 session: str = "", login_mode: str = "password"):
        self.username = username
        self.password = password
        self.save_dir = save_dir
        self.session = session.strip()
        normalized_login_mode = str(login_mode or "password").strip().lower()
        self.login_mode = "token" if normalized_login_mode in {"token", "session"} else "password"
        self._save_dir_id: Optional[str] = None

        encoded_token = self.session or self._load_token()
        if encoded_token:
            self.client = PikPakApi(
                username=username or None,
                password=password or None,
                encoded_token=encoded_token,
                token_refresh_callback=PikPakClient._on_token_refresh,
            )
        else:
            self.client = PikPakApi(
                username=username,
                password=password,
                token_refresh_callback=PikPakClient._on_token_refresh,
            )

    def _load_token(self) -> Optional[str]:
        if not self.username:
            return None
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

    async def close(self):
        httpx_client = getattr(self.client, "httpx_client", None)
        if httpx_client is not None:
            try:
                await httpx_client.aclose()
            except Exception:
                pass

    async def login(self):
        if self.client.refresh_token:
            try:
                await self.client.refresh_access_token()
                self._save_token()
                return
            except Exception as e:
                if self.login_mode == "token" and not (self.username and self.password):
                    raise ValueError(f"PikPak Token 已失效，请更新 encoded_token：{e}") from e
        if not (self.username and self.password):
            raise ValueError("PikPak 账号密码未配置，无法使用密码登录")
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

    @staticmethod
    def _extract_download_url(info: Dict[str, Any]) -> str:
        url = info.get("web_content_link", "")
        if url:
            return url
        for media in info.get("medias", []) or []:
            if not isinstance(media, dict):
                continue
            link = media.get("link", {})
            if isinstance(link, dict) and link.get("url"):
                return link["url"]
        for link in info.get("links", []) or []:
            if not isinstance(link, dict):
                continue
            if link.get("url"):
                return link["url"]
            nested = link.get("link", {})
            if isinstance(nested, dict) and nested.get("url"):
                return nested["url"]
        return ""

    async def get_download_urls(self, file_id: str) -> List[Dict[str, str]]:
        info = await self.client.get_download_url(file_id)
        kind = info.get("kind", "")
        if kind == "drive#folder":
            folder_name = info.get("name", "")
            return await self._list_folder_files(file_id, prefix=folder_name)
        url = self._extract_download_url(info)
        name = info.get("name", "未知文件")
        if url:
            return [{
                "name": name,
                "url": url,
                "file_id": file_id,
                "path": name,
                "size": int(info.get("size", 0)),
            }]
        logger.warning(f"文件暂无直链: file_id={file_id}, name={name}")
        return []

    async def wait_for_download_urls(self, file_id: str, timeout: float = 60.0,
                                     poll_interval: float = 3.0) -> List[Dict[str, str]]:
        timeout = max(float(timeout or 0.0), 0.0)
        poll_interval = max(float(poll_interval or 0.0), 0.1)
        request_timeout = max(8.0, min(timeout if timeout > 0 else 15.0, 15.0))
        deadline = time.time() + timeout
        last_error: Optional[Exception] = None
        attempt = 0
        while True:
            attempt += 1
            try:
                urls = await asyncio.wait_for(self.get_download_urls(file_id), timeout=request_timeout)
                if urls:
                    return urls
            except asyncio.TimeoutError:
                last_error = RuntimeError(f"获取直链请求超时（>{int(request_timeout)}s）")
                logger.warning(f"获取直链请求超时: file_id={file_id}, attempt={attempt}, request_timeout={request_timeout}")
            except Exception as e:
                last_error = e
                logger.warning(f"获取直链失败，等待重试: file_id={file_id}, attempt={attempt}, error={e}")
            if time.time() >= deadline:
                break
            await asyncio.sleep(poll_interval)
        if last_error is not None:
            detail = str(last_error).strip()
            raise RuntimeError(f"等待 PikPak 直链超时，可能触发风控或分享暂不可用{f'：{detail}' if detail else ''}")
        return []

    async def wait_for_isolated_share_urls(
        self, receipt: ShareRestoreReceipt,
        timeout: float = 60.0, poll_interval: float = 3.0,
    ) -> List[Dict[str, Any]]:
        if not isinstance(receipt, ShareRestoreReceipt):
            raise TypeError("分享恢复回执无效")
        if not receipt.scope_id or not receipt.task_id or not receipt.selected_ids:
            raise ValueError("分享恢复回执字段不完整")

        timeout = max(float(timeout or 0.0), 0.0)
        poll_interval = max(float(poll_interval or 0.0), 0.01)
        request_timeout = max(1.0, min(timeout if timeout > 0 else 15.0, 15.0))
        deadline = time.monotonic() + timeout
        expected_count = len(receipt.selected_ids)
        trace_map: Optional[Dict[str, str]] = None
        last_count = 0
        last_error: Optional[Exception] = None

        while True:
            try:
                if trace_map is None:
                    task = await asyncio.wait_for(
                        self.client._request_get(
                            f"https://{self.client.PIKPAK_API_HOST}/drive/v1/tasks/{receipt.task_id}"
                        ),
                        timeout=request_timeout,
                    )
                    phase = str(task.get("phase") or "").strip()
                    params = task.get("params") if isinstance(task.get("params"), dict) else {}
                    if phase in {"PHASE_TYPE_ERROR", "PHASE_TYPE_FAILED"}:
                        detail = str(
                            params.get("error_detail")
                            or task.get("message")
                            or phase
                        ).strip()
                        raise _ShareRestoreValidationError(
                            f"PikPak 恢复任务失败: {detail}"
                        )
                    if phase == "PHASE_TYPE_COMPLETE":
                        trace_map = self._parse_restore_trace_map(
                            params.get("trace_file_ids"), receipt.selected_ids
                        )

                if trace_map is not None:
                    children = await asyncio.wait_for(
                        self._list_direct_folder_files(receipt.scope_id),
                        timeout=request_timeout,
                    )
                    last_count = len(children)
                    destination_ids = set(trace_map.values())
                    child_ids = {
                        str(child.get("id") or "").strip()
                        for child in children
                    }
                    extra_ids = child_ids - destination_ids
                    if extra_ids:
                        raise _ShareRestoreValidationError(
                            "PikPak 隔离目录出现未映射文件，本次未推送任何下载链接"
                        )
                    for child in children:
                        if str(child.get("parent_id") or "").strip() != receipt.scope_id:
                            raise _ShareRestoreValidationError(
                                "PikPak 恢复文件不属于本次隔离目录，本次未推送任何下载链接"
                            )
                        if str(child.get("kind") or "").strip() != "drive#file":
                            raise _ShareRestoreValidationError(
                                "PikPak 恢复结果不是直属文件，本次未推送任何下载链接"
                            )

                    if child_ids == destination_ids:
                        children_by_id = {
                            str(child.get("id") or "").strip(): child
                            for child in children
                        }
                        resolved = []
                        missing_url = False
                        for selected_id in receipt.selected_ids:
                            destination_id = trace_map[selected_id]
                            child = children_by_id[destination_id]
                            url = self._extract_download_url(child)
                            if not url:
                                info = await asyncio.wait_for(
                                    self.client.get_download_url(destination_id),
                                    timeout=request_timeout,
                                )
                                url = self._extract_download_url(info)
                            if not url:
                                missing_url = True
                                break
                            name = str(child.get("name") or "")
                            resolved.append({
                                "name": name,
                                "url": url,
                                "file_id": destination_id,
                                "path": name,
                                "size": int(child.get("size") or 0),
                            })
                        if not missing_url:
                            return resolved
            except _ShareRestoreValidationError:
                raise
            except Exception as error:
                last_error = error

            if time.monotonic() >= deadline:
                detail = f"，最后错误：{last_error}" if last_error else ""
                if trace_map is None:
                    raise RuntimeError(f"PikPak 恢复任务等待超时{detail}")
                if last_count < expected_count:
                    raise RuntimeError(
                        f"PikPak 隔离目录直属文件等待超时：应有 {expected_count} 个，"
                        f"实际 {last_count} 个{detail}"
                    )
                raise RuntimeError(
                    f"PikPak 隔离目录直链等待超时：应有 {expected_count} 个文件{detail}"
                )
            await asyncio.sleep(poll_interval)

    @staticmethod
    def _parse_restore_trace_map(
        value: Any, selected_ids: tuple[str, ...]
    ) -> Dict[str, str]:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError) as error:
                raise _ShareRestoreValidationError(
                    "PikPak 恢复任务映射格式无效"
                ) from error
        if not isinstance(value, dict):
            raise _ShareRestoreValidationError("PikPak 恢复任务映射格式无效")

        trace_map = {
            str(source_id).strip(): str(destination_id).strip()
            for source_id, destination_id in value.items()
        }
        if set(trace_map) != set(selected_ids):
            raise _ShareRestoreValidationError(
                "PikPak 恢复任务源文件映射不一致"
            )
        destination_ids = list(trace_map.values())
        if (
            any(not destination_id for destination_id in destination_ids)
            or len(set(destination_ids)) != len(destination_ids)
        ):
            raise _ShareRestoreValidationError(
                "PikPak 恢复任务目标文件映射无效"
            )
        return trace_map

    async def _list_direct_folder_files(self, folder_id: str) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        next_page_token = None
        while True:
            resp = await self.client.file_list(
                parent_id=folder_id,
                next_page_token=next_page_token,
            )
            for item in resp.get("files", []) or []:
                results.append({
                    "id": str(item.get("id") or "").strip(),
                    "name": str(item.get("name") or ""),
                    "kind": str(item.get("kind") or ""),
                    "parent_id": str(item.get("parent_id") or "").strip(),
                    "size": int(item.get("size") or 0),
                    "web_content_link": item.get("web_content_link", ""),
                    "medias": item.get("medias", []),
                    "links": item.get("links", []),
                })
            next_page_token = resp.get("next_page_token")
            if not next_page_token:
                break
        return results

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
                    url = self._extract_download_url(f)
                    if url:
                        results.append({
                            "name": name,
                            "url": url,
                            "file_id": fid,
                            "path": full_path,
                            "size": int(f.get("size", 0)),
                        })
                    else:
                        try:
                            info = await self.client.get_download_url(fid)
                            dl_url = self._extract_download_url(info)
                            if dl_url:
                                results.append({
                                    "name": name,
                                    "url": dl_url,
                                    "file_id": fid,
                                    "path": full_path,
                                    "size": int(f.get("size", 0)),
                                })
                        except Exception as e:
                            logger.warning(f"子文件直链获取失败: path={full_path}, error={e}")
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

    async def get_task_progress(self, task_id: str) -> Optional[int]:
        """返回指定离线任务的下载进度（0-100）；任务不在进行中列表里则返回 None。

        包含 PENDING 阶段，以便在磁链尚未解析出元数据时也能观察是否有进展。
        """
        try:
            result = await self.client.offline_list(
                phase=["PHASE_TYPE_PENDING", "PHASE_TYPE_RUNNING", "PHASE_TYPE_ERROR"]
            )
        except Exception:
            return None
        for task in result.get("tasks", []) or []:
            if task.get("id") == task_id:
                try:
                    return int(task.get("progress", 0) or 0)
                except (TypeError, ValueError):
                    return 0
        return None

    # ── 分享链接相关 ──

    async def get_share_file_list(self, share_link: str, pass_code: str = "") -> Dict[str, Any]:
        share_id, target_id = _parse_pikpak_share_location(share_link)
        result = await self.client._request_get(
            url=f"https://{self.client.PIKPAK_API_HOST}/drive/v1/share",
            params={
                "limit": "100",
                "thumbnail_size": "SIZE_LARGE",
                "order": "3",
                "share_id": share_id,
                "parent_id": target_id,
                "pass_code": pass_code or None,
            },
        )
        if not isinstance(result, dict):
            raise RuntimeError("PikPak 分享接口响应格式无效")

        roots = list(result.get("files", []) or [])
        if target_id:
            target_is_returned = (
                len(roots) == 1
                and str(roots[0].get("id") or "").strip() == target_id
            )
            target_is_parent = bool(roots) and all(
                str(item.get("parent_id") or "").strip() == target_id
                for item in roots
            )
            if not (target_is_returned or target_is_parent):
                raise RuntimeError("PikPak 返回的分享内容与链接目标节点不一致")

        pass_code_token = result.get("pass_code_token", "")
        files: List[Dict] = []
        for item in roots:
            await self._collect_share_files(share_id, pass_code_token, item, files)
        return {
            "share_id": share_id,
            "target_id": target_id,
            "pass_code_token": pass_code_token,
            "files": files,
        }

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
                "id": file_id,
                "source_file_id": file_id,
                "name": name,
                "source_name": name,
                "path": full_path,
                "source_path": full_path,
                "size": int(file_info.get("size", 0)),
                "file_type": file_info.get("mime_type", ""),
                "icon_link": file_info.get("icon_link", ""),
            })

    async def start_isolated_share_restore(
        self, share_id: str, file_ids: List[str], pass_code_token: str
    ) -> ShareRestoreReceipt:
        selected_ids = tuple(
            str(file_id).strip()
            for file_id in file_ids
            if str(file_id or "").strip()
        )
        if not selected_ids:
            raise ValueError("分享恢复文件 ID 不能为空")
        if len(set(selected_ids)) != len(selected_ids):
            raise ValueError("分享恢复文件 ID 不能重复")

        parent_id = await self._get_save_dir_id()
        folder_name = f".teldrive-share-{uuid.uuid4().hex}"
        folder = await self.client.create_folder(name=folder_name, parent_id=parent_id)
        scope_id = str(
            folder.get("file", {}).get("id") or folder.get("id") or ""
        ).strip()
        if not scope_id:
            raise RuntimeError("PikPak 未返回隔离目录 ID，本次未转存任何文件")

        payload = {
            "parent_id": scope_id,
            "share_id": share_id,
            "pass_code_token": pass_code_token,
            "file_ids": list(selected_ids),
            "ancestor_ids": [],
            "specify_parent_id": True,
            "params": {"trace_file_ids": ",".join(selected_ids)},
        }
        try:
            result = await self.client._request_post(
                url=f"https://{self.client.PIKPAK_API_HOST}/drive/v1/share/restore",
                data=payload,
            )
            if not isinstance(result, dict):
                raise RuntimeError("PikPak 转存响应格式无效")
            if result.get("error"):
                error = result["error"]
                detail = error.get("message") if isinstance(error, dict) else str(error)
                raise RuntimeError(f"PikPak 隔离转存失败: {detail or error}")
            returned_scope_id = str(result.get("file_id") or "").strip()
            if returned_scope_id != scope_id:
                raise RuntimeError(
                    "PikPak 恢复目标目录不一致，本次未解析或推送任何文件"
                )
            task_id = str(result.get("restore_task_id") or "").strip()
            if not task_id:
                raise RuntimeError(
                    "PikPak 未返回恢复任务 ID，本次未解析或推送任何文件"
                )
        except BaseException:
            try:
                await self.delete_files([scope_id])
            except Exception as cleanup_error:
                logger.warning(
                    f"清理未启用的分享隔离目录失败: {scope_id}, {cleanup_error}"
                )
            raise

        logger.info(
            "分享转存已提交到隔离目录: scope=%s, task=%s, selected=%s, status=%s",
            scope_id,
            task_id,
            len(selected_ids),
            result.get("restore_status", ""),
        )
        return ShareRestoreReceipt(scope_id, task_id, selected_ids)
