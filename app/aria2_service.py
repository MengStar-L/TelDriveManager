"""本地托管 aria2 服务与安装器"""

from __future__ import annotations

import asyncio
import os
import platform
import secrets
import shutil
import stat
import subprocess
import tarfile
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import httpx

from app.aria2_client import Aria2Client
from app.config import FIXED_ARIA2_HOME, FIXED_DOWNLOAD_DIR, load_config, save_config

import logging

logger = logging.getLogger(__name__)

ARIA2_HOME = Path(FIXED_ARIA2_HOME)
ARIA2_BIN_DIR = ARIA2_HOME / "bin"
ARIA2_TMP_DIR = ARIA2_HOME / "tmp"
ARIA2_SESSION_FILE = ARIA2_HOME / "aria2.session"
ARIA2_LOG_FILE = ARIA2_HOME / "aria2.log"


@dataclass
class InstallState:
    status: str = "idle"
    progress: float = 0.0
    message: str = "尚未开始"
    mode: str = ""
    os_type: str = ""
    file_name: str = ""
    downloaded_bytes: int = 0
    total_bytes: int = 0
    installed: bool = False
    running: bool = False
    version: str = ""
    binary_path: str = ""
    error: str = ""


class Aria2Service:
    def __init__(self):
        self._process: Optional[subprocess.Popen] = None
        self._log_handle = None
        self._install_task: Optional[asyncio.Task] = None
        self._install_lock = asyncio.Lock()
        self._state = InstallState()

    @staticmethod
    def detect_host_os() -> str:
        system = platform.system().lower()
        if system.startswith("win"):
            return "win"
        if system == "linux":
            return "linux"
        raise RuntimeError(f"当前系统暂不支持自动托管 aria2: {platform.system()}")

    @staticmethod
    def detect_host_arch() -> str:
        machine = platform.machine().lower()
        if machine in {"x86_64", "amd64"}:
            return "amd64"
        if machine in {"aarch64", "arm64"}:
            return "arm64"
        if machine.startswith("armv7") or machine.startswith("armhf"):
            return "armhf"
        if machine in {"i386", "i686", "x86"}:
            return "i386"
        raise RuntimeError(f"当前 CPU 架构暂不支持自动托管 aria2: {platform.machine()}")

    def get_binary_path(self, cfg: Optional[dict] = None) -> Path:
        cfg = cfg or load_config()
        configured = str(cfg.get("aria2", {}).get("binary_path") or "").strip()
        if configured:
            return Path(configured)
        suffix = ".exe" if self.detect_host_os() == "win" else ""
        return ARIA2_BIN_DIR / f"aria2c{suffix}"

    def is_installed(self, cfg: Optional[dict] = None) -> bool:
        return self.get_binary_path(cfg).exists()

    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    async def get_runtime_status(self) -> dict:
        cfg = load_config()
        binary_path = self.get_binary_path(cfg)
        snapshot = asdict(self._state)
        snapshot.update({
            "host_os": self.detect_host_os(),
            "host_arch": self.detect_host_arch(),
            "configured_os": str(cfg.get("aria2", {}).get("os_type") or ""),
            "installed": self.is_installed(cfg),
            "running": self.is_running(),
            "binary_path": str(binary_path) if binary_path else "",
            "download_dir": FIXED_DOWNLOAD_DIR,
            "rpc_url": cfg.get("aria2", {}).get("rpc_url", "http://127.0.0.1"),
            "rpc_port": int(cfg.get("aria2", {}).get("rpc_port") or 6800),
        })
        if snapshot["running"]:
            try:
                client = self._build_client(cfg)
                version = await client.get_version()
                snapshot["version"] = str(version.get("version") or "")
                await client.close()
            except Exception:
                snapshot["running"] = False
        return snapshot

    def _build_client(self, cfg: Optional[dict] = None) -> Aria2Client:
        cfg = cfg or load_config()
        aria2_cfg = cfg.get("aria2", {})
        return Aria2Client(
            rpc_url=aria2_cfg.get("rpc_url", "http://127.0.0.1"),
            rpc_port=int(aria2_cfg.get("rpc_port") or 6800),
            rpc_secret=aria2_cfg.get("rpc_secret", ""),
        )

    def _set_state(self, **kwargs):
        data = asdict(self._state)
        data.update(kwargs)
        self._state = InstallState(**data)

    async def handle_config_update(self, previous: Optional[dict], current: dict):
        prev_aria2 = (previous or {}).get("aria2", {})
        curr_aria2 = current.get("aria2", {})
        keys = {
            "binary_path",
            "rpc_port",
            "rpc_secret",
            "max_concurrent",
            "split",
            "max_connection_per_server",
            "min_split_size_mb",
        }
        changed = any(prev_aria2.get(key) != curr_aria2.get(key) for key in keys)
        if not self.is_installed(current):
            return
        if self.is_running() and changed:
            await self.restart()
        elif not self.is_running():
            await self.start()

    async def start(self):
        cfg = load_config(force_reload=True)
        if not self.is_installed(cfg):
            logger.info("aria2 未安装，跳过本地服务启动")
            return
        if self.is_running():
            return

        binary_path = self.get_binary_path(cfg)
        binary_path.parent.mkdir(parents=True, exist_ok=True)
        Path(FIXED_DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
        ARIA2_HOME.mkdir(parents=True, exist_ok=True)
        ARIA2_TMP_DIR.mkdir(parents=True, exist_ok=True)
        ARIA2_SESSION_FILE.touch(exist_ok=True)

        aria2_cfg = cfg.get("aria2", {})
        command = [
            str(binary_path),
            "--enable-rpc=true",
            "--rpc-listen-all=false",
            "--rpc-allow-origin-all=true",
            f"--rpc-listen-port={int(aria2_cfg.get('rpc_port') or 6800)}",
            f"--dir={FIXED_DOWNLOAD_DIR}",
            f"--max-concurrent-downloads={int(aria2_cfg.get('max_concurrent') or 3)}",
            f"--split={int(aria2_cfg.get('split') or 8)}",
            f"--max-connection-per-server={int(aria2_cfg.get('max_connection_per_server') or 8)}",
            f"--min-split-size={int(aria2_cfg.get('min_split_size_mb') or 5)}M",
            "--continue=true",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--max-tries=0",
            "--retry-wait=5",
            f"--input-file={ARIA2_SESSION_FILE}",
            f"--save-session={ARIA2_SESSION_FILE}",
            "--save-session-interval=30",
        ]
        if aria2_cfg.get("rpc_secret"):
            command.append(f"--rpc-secret={aria2_cfg['rpc_secret']}")

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
        self._log_handle = open(ARIA2_LOG_FILE, "ab")
        self._process = subprocess.Popen(
            command,
            cwd=str(ARIA2_HOME),
            stdout=self._log_handle,
            stderr=self._log_handle,
            creationflags=creationflags,
        )
        await self._wait_until_ready(cfg)
        logger.info("本地 aria2 服务已启动")

    async def stop(self):
        process = self._process
        self._process = None
        if process and process.poll() is None:
            process.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(process.wait, 8), timeout=10)
            except Exception:
                process.kill()
        if self._log_handle:
            try:
                self._log_handle.close()
            except Exception:
                pass
            self._log_handle = None

    async def restart(self):
        await self.stop()
        await self.start()

    async def _wait_until_ready(self, cfg: Optional[dict] = None, timeout: float = 20.0):
        deadline = asyncio.get_running_loop().time() + timeout
        last_error = None
        while asyncio.get_running_loop().time() < deadline:
            if self._process and self._process.poll() is not None:
                raise RuntimeError("aria2 进程启动后立即退出，请检查安装包或日志")
            try:
                client = self._build_client(cfg)
                version = await client.get_version()
                await client.close()
                self._set_state(
                    status="completed",
                    progress=100.0,
                    message="aria2 已启动并可用",
                    installed=True,
                    running=True,
                    version=str(version.get("version") or ""),
                    binary_path=str(self.get_binary_path(cfg)),
                    error="",
                )
                return
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.5)
        raise RuntimeError(f"aria2 启动超时: {last_error}")

    async def begin_auto_install(self, os_type: str) -> dict:
        async with self._install_lock:
            self._ensure_install_slot()
            self._install_task = asyncio.create_task(self._run_auto_install(os_type))
        return {"success": True, "message": "已开始自动安装 aria2"}

    async def begin_uploaded_install(self, archive_path: Path, os_type: str, file_name: str) -> dict:
        async with self._install_lock:
            self._ensure_install_slot()
            self._install_task = asyncio.create_task(self._run_uploaded_install(archive_path, os_type, file_name))
        return {"success": True, "message": "已接收安装包，开始解压部署"}

    def _ensure_install_slot(self):
        if self._install_task and not self._install_task.done():
            raise RuntimeError("已有 aria2 安装任务正在进行，请稍候")

    async def _run_auto_install(self, os_type: str):
        archive_path: Optional[Path] = None
        try:
            self._validate_requested_os(os_type)
            archive_path = await self._download_release_archive(os_type)
            await self._install_from_archive(archive_path, os_type=os_type, mode="auto")
        except Exception as exc:
            logger.exception("自动安装 aria2 失败")
            self._set_state(status="failed", message="aria2 自动安装失败", error=str(exc), running=False)
        finally:
            if archive_path and archive_path.exists():
                archive_path.unlink(missing_ok=True)

    async def _run_uploaded_install(self, archive_path: Path, os_type: str, file_name: str):
        try:
            self._validate_requested_os(os_type)
            self._set_state(
                status="extracting",
                progress=72.0,
                message=f"安装包 {file_name} 上传完成，正在解压...",
                mode="upload",
                os_type=os_type,
                file_name=file_name,
                error="",
                running=False,
            )
            await self._install_from_archive(archive_path, os_type=os_type, mode="upload")
        except Exception as exc:
            logger.exception("上传安装 aria2 失败")
            self._set_state(status="failed", message="aria2 上传安装失败", error=str(exc), running=False)
        finally:
            archive_path.unlink(missing_ok=True)

    async def _download_release_archive(self, os_type: str) -> Path:
        headers = {"User-Agent": "TelDriveManager/1.0", "Accept": "application/vnd.github+json"}
        self._set_state(
            status="downloading",
            progress=0.0,
            message="正在获取 aria2 发布信息...",
            mode="auto",
            os_type=os_type,
            file_name="",
            downloaded_bytes=0,
            total_bytes=0,
            error="",
            running=False,
        )
        async with httpx.AsyncClient(follow_redirects=True, timeout=None, headers=headers) as client:
            meta_url = (
                "https://api.github.com/repos/aria2/aria2/releases/latest"
                if os_type == "win"
                else "https://api.github.com/repos/P3TERX/Aria2-Pro-Core/releases/latest"
            )
            release = (await client.get(meta_url)).raise_for_status().json()
            asset = self._select_release_asset(release, os_type)
            download_url = asset["browser_download_url"]
            file_name = asset["name"]
            total_bytes = int(asset.get("size") or 0)
            archive_path = ARIA2_TMP_DIR / file_name
            ARIA2_TMP_DIR.mkdir(parents=True, exist_ok=True)
            self._set_state(file_name=file_name, total_bytes=total_bytes, message=f"正在下载 {file_name} ...")

            downloaded = 0
            async with client.stream("GET", download_url) as response:
                response.raise_for_status()
                with open(archive_path, "wb") as fh:
                    async for chunk in response.aiter_bytes(1024 * 256):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        downloaded += len(chunk)
                        progress = 5.0 + (downloaded / total_bytes * 65.0) if total_bytes > 0 else 35.0
                        self._set_state(
                            progress=min(progress, 70.0),
                            downloaded_bytes=downloaded,
                            total_bytes=total_bytes,
                            message=f"正在下载 {file_name} ...",
                        )
            return archive_path

    def _select_release_asset(self, release: dict, os_type: str) -> dict:
        assets = release.get("assets", []) or []
        if os_type == "win":
            for asset in assets:
                name = str(asset.get("name") or "")
                if "win-64bit" in name and name.endswith(".zip"):
                    return asset
            raise RuntimeError("未找到 Windows 64 位 aria2 安装包")

        arch = self.detect_host_arch()
        mapping = {
            "amd64": "linux-amd64.tar.gz",
            "arm64": "linux-arm64.tar.gz",
            "armhf": "linux-armhf.tar.gz",
            "i386": "linux-i386.tar.gz",
        }
        target = mapping.get(arch)
        for asset in assets:
            name = str(asset.get("name") or "")
            if target and name.endswith(target):
                return asset
        raise RuntimeError(f"未找到适用于 Linux/{arch} 的 aria2 安装包")

    async def _install_from_archive(self, archive_path: Path, os_type: str, mode: str):
        extract_root = ARIA2_TMP_DIR / "extract"
        if extract_root.exists():
            shutil.rmtree(extract_root, ignore_errors=True)
        extract_root.mkdir(parents=True, exist_ok=True)
        self._set_state(status="extracting", progress=max(self._state.progress, 72.0), message="正在解压 aria2 安装包...")
        await asyncio.to_thread(self._extract_archive_sync, archive_path, extract_root)

        binary_name = "aria2c.exe" if os_type == "win" else "aria2c"
        binary_path = self._find_file(extract_root, binary_name)
        if binary_path is None:
            raise RuntimeError(f"安装包中未找到 {binary_name}")

        await asyncio.to_thread(self._deploy_binary_dir_sync, binary_path.parent, os_type)
        target_binary = ARIA2_BIN_DIR / binary_name
        self._persist_install_config(target_binary, os_type)
        self._set_state(status="starting", progress=92.0, message="安装完成，正在启动本地 aria2 服务...", mode=mode)

        from app.modules.aria2teldrive.task_manager import task_manager

        task_manager.reload_config()
        await self.restart()
        self._set_state(
            status="completed",
            progress=100.0,
            message="aria2 安装完成并已启动",
            installed=True,
            running=True,
            binary_path=str(target_binary),
            error="",
        )

    def _persist_install_config(self, target_binary: Path, os_type: str):
        cfg = load_config(force_reload=True)
        aria2_cfg = cfg.get("aria2", {})
        secret = str(aria2_cfg.get("rpc_secret") or "").strip() or secrets.token_hex(16)
        save_config({
            "aria2": {
                "managed": True,
                "installed": True,
                "os_type": os_type,
                "binary_path": str(target_binary.resolve()),
                "rpc_url": "http://127.0.0.1",
                "rpc_port": int(aria2_cfg.get("rpc_port") or 6800),
                "rpc_secret": secret,
                "download_dir": FIXED_DOWNLOAD_DIR,
                "max_concurrent": int(aria2_cfg.get("max_concurrent") or 3),
                "split": int(aria2_cfg.get("split") or 8),
                "max_connection_per_server": int(aria2_cfg.get("max_connection_per_server") or 8),
                "min_split_size_mb": int(aria2_cfg.get("min_split_size_mb") or 5),
            }
        })

    @staticmethod
    def _extract_archive_sync(archive_path: Path, extract_root: Path):
        suffix = archive_path.name.lower()
        if suffix.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(extract_root)
            return
        if suffix.endswith(".tar.gz") or suffix.endswith(".tgz") or suffix.endswith(".tar.xz") or suffix.endswith(".tar"):
            with tarfile.open(archive_path, "r:*") as tf:
                tf.extractall(extract_root)
            return
        raise RuntimeError("仅支持 zip / tar.gz / tgz / tar.xz / tar 安装包")

    @staticmethod
    def _find_file(root: Path, target_name: str) -> Optional[Path]:
        for file_path in root.rglob(target_name):
            if file_path.is_file():
                return file_path
        return None

    @staticmethod
    def _deploy_binary_dir_sync(source_dir: Path, os_type: str):
        if ARIA2_BIN_DIR.exists():
            shutil.rmtree(ARIA2_BIN_DIR, ignore_errors=True)
        ARIA2_BIN_DIR.mkdir(parents=True, exist_ok=True)
        for item in source_dir.iterdir():
            target = ARIA2_BIN_DIR / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)
        binary_name = "aria2c.exe" if os_type == "win" else "aria2c"
        binary_path = ARIA2_BIN_DIR / binary_name
        if not binary_path.exists():
            raise RuntimeError("安装目录中缺少 aria2 主程序")
        if os_type != "win":
            current_mode = os.stat(binary_path).st_mode
            os.chmod(binary_path, current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        Path(FIXED_DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
        ARIA2_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        ARIA2_SESSION_FILE.touch(exist_ok=True)

    def _validate_requested_os(self, os_type: str):
        requested = str(os_type or "").strip().lower()
        host = self.detect_host_os()
        if requested not in {"win", "linux"}:
            raise RuntimeError("请选择有效的操作系统类型")
        if requested != host:
            raise RuntimeError(f"当前服务运行在 {host}，不能安装 {requested} 安装包")


aria2_service = Aria2Service()
