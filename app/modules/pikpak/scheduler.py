"""Concurrent PikPak magnet parse scheduler."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from app import database as db
from app.config import load_config
from app.modules.pikpak.account_pool import PikPakAccountContext, PikPakAccountPool


ParseOne = Callable[..., Awaitable[dict[str, Any]]]
Broadcast = Callable[[dict[str, Any]], Awaitable[None]]
FinishJob = Callable[..., Awaitable[dict[str, Any] | None]]
SortFiles = Callable[[list[dict[str, Any]]], list[dict[str, Any]]]


class MagnetParseScheduler:
    def __init__(self, account_pool: PikPakAccountPool):
        self.account_pool = account_pool

    async def run(
        self,
        job_id: str,
        magnets: list[str],
        *,
        parse_one: ParseOne,
        broadcast: Broadcast,
        finish_job: FinishJob,
        sort_files: SortFiles,
    ) -> None:
        cfg = load_config()
        pikpak_cfg = cfg.get("pikpak", {})
        poll_interval = pikpak_cfg.get("poll_interval", 3)
        max_wait_time = pikpak_cfg.get("max_wait_time", 3600)
        parse_timeout = pikpak_cfg.get("magnet_parse_timeout", 300)
        concurrency = min(16, max(1, int(pikpak_cfg.get("parse_concurrency") or 1)))

        total = len(magnets)
        semaphore = asyncio.Semaphore(concurrency)
        results: list[tuple[int, dict[str, Any]]] = []
        errors: list[dict[str, Any]] = []
        result_lock = asyncio.Lock()

        async def worker(index: int, magnet: str) -> None:
            account: PikPakAccountContext | None = None
            async with semaphore:
                try:
                    account, client = await self.account_pool.next_client()
                    result = await parse_one(
                        client,
                        magnet,
                        index,
                        total,
                        job_id,
                        poll_interval,
                        max_wait_time,
                        parse_timeout,
                        account=account,
                    )
                    async with result_lock:
                        results.append((index, result))
                except Exception as exc:
                    message = str(exc)
                    error_item = {
                        "index": index,
                        "link": magnet,
                        "stage": "magnet_parse",
                        "message": message,
                        "account_id": account.id if account else "",
                        "account_name": account.name if account else "",
                    }
                    async with result_lock:
                        errors.append(error_item)
                    if account is not None:
                        await db.add_pikpak_account_error(account.id, job_id, magnet, "magnet_parse", message)
                    await broadcast({
                        "type": "task_error",
                        "index": index,
                        "message": f"第 {index} 个磁链解析失败: {message}",
                        "parse_job_id": job_id,
                        "account_id": account.id if account else "",
                        "account_name": account.name if account else "",
                    })

        try:
            await asyncio.gather(*(worker(index, magnet) for index, magnet in enumerate(magnets, 1)))

            ordered = [result for _index, result in sorted(results, key=lambda item: item[0])]
            roots: list[str] = []
            root_accounts: dict[str, str] = {}
            merged_files: list[dict[str, Any]] = []
            names: list[str] = []
            for result in ordered:
                if result.get("file_id"):
                    roots.append(result["file_id"])
                    if result.get("account_id"):
                        root_accounts[str(result["file_id"])] = str(result["account_id"])
                if result.get("file_name"):
                    names.append(result["file_name"])
                merged_files.extend(result.get("files") or [])

            if not merged_files:
                detail = "；".join(item["message"] for item in errors) if errors else "未解析到任何文件"
                await broadcast({"type": "error", "message": f"磁链解析失败: {detail}", "parse_job_id": job_id})
                await finish_job(job_id, "failed", error=detail)
                return

            merged_files = sort_files(merged_files)
            file_name = names[0] if len(names) == 1 else f"{len(names)} 个磁链（共 {len(merged_files)} 个文件）"

            await broadcast({"type": "all_done", "total": total, "parse_job_id": job_id})
            await finish_job(
                job_id,
                "completed",
                result_payload={
                    "file_id": roots[0] if roots else "",
                    "roots": roots,
                    "root_accounts": root_accounts,
                    "file_name": file_name,
                    "files": merged_files,
                    "errors": errors,
                },
                error=(f"{len(errors)} 个链接解析失败" if errors else None),
            )
        except Exception as exc:
            await broadcast({"type": "error", "message": f"磁链解析失败: {exc}", "parse_job_id": job_id})
            await finish_job(job_id, "failed", error=str(exc))
