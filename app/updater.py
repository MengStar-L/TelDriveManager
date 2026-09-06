"""GitHub Release based self-updater for source deployments."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

import httpx
import update_worker as installer

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GITHUB_REPOSITORY = os.getenv("TDM_GITHUB_REPOSITORY", "MengStar-L/TelDriveManager")
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/releases/latest"
MAX_ARCHIVE_BYTES = 512 * 1024 * 1024
MAX_EXTRACTED_BYTES = 1024 * 1024 * 1024
UPDATE_MIN_FREE_BYTES = 64 * 1024 * 1024
UPDATE_RESERVATION_BYTES = MAX_ARCHIVE_BYTES + MAX_EXTRACTED_BYTES + installer.RUNTIME_LIMIT_BYTES + 128 * 1024 * 1024
UPDATE_CHECK_INTERVAL = 30 * 60


def _version(value: str) -> tuple[int, int, int, int]:
    text = str(value or "").strip().lstrip("vV")
    match = re.match(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:[-+].*)?$", text)
    if not match:
        return (0, 0, 0, 0)
    # Stable releases sort after prereleases with the same numeric version.
    prerelease = 0 if "-" in text else 1
    return tuple(int(match.group(i) or 0) for i in range(1, 4)) + (prerelease,)


def _protected(relative: str) -> bool:
    return installer.protected(relative)


def _read_version() -> str:
    marker = PROJECT_ROOT / ".tdm-version"
    try:
        value = marker.read_text(encoding="utf-8").strip()
        if value:
            return value
    except OSError:
        pass
    env_value = os.getenv("TDM_VERSION", "").strip()
    if env_value:
        return env_value
    try:
        value = (PROJECT_ROOT / "VERSION").read_text(encoding="utf-8").strip()
        if value:
            return value
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=2, check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "0.0.0-dev"


def _headers() -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": "TelDriveManager-updater",
        "X-GitHub-Api-Version": "2022-11-28",
    }


class UpdateManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._poll_task: asyncio.Task | None = None
        self._initial_task: asyncio.Task | None = None
        self._apply_task: asyncio.Task | None = None
        self._stage: Path | None = None
        self._checked_at = 0.0
        self.shutdown_callback = None
        self._handed_off = False
        self._startup_version = _read_version()
        self._state: dict[str, Any] = {
            "state": "idle", "current_version": _read_version(), "latest_version": None,
            "update_available": False, "release_url": None, "published_at": None,
            "release_notes": "", "progress": 0.0, "error": None,
        }

    def snapshot(self) -> dict[str, Any]:
        result = dict(self._state)
        try:
            transaction = installer.read_json(PROJECT_ROOT / ".tdm-update-lock")
            result.update(state="restarting" if transaction["phase"] != "downloading" else result["state"],
                          error=transaction.get("error"), update_available=False)
        except FileNotFoundError:
            try:
                saved = installer.read_json(PROJECT_ROOT / ".tdm-update-result")
                if saved.get("error"):
                    result.update(state="error", error=saved["error"])
            except FileNotFoundError:
                pass
            except (OSError, ValueError) as exc:
                result.update(state="error", error=f"无法读取更新结果: {exc}")
        except (OSError, ValueError, KeyError) as exc:
            result.update(state="error", error=f"无法读取更新恢复记录: {exc}", update_available=False)
        return result

    async def start(self):
        if self._poll_task and not self._poll_task.done():
            return
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._initial_task = asyncio.create_task(self.check(force=True))

    async def stop(self):
        tasks = (self._poll_task, self._initial_task, self._apply_task)
        for task in tasks:
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in tasks if task), return_exceptions=True)
        if self._stage and not self._handed_off:
            shutil.rmtree(self._stage, ignore_errors=True)
        self._stage = None
        self._poll_task = None
        self._initial_task = None
        self._apply_task = None

    async def _poll_loop(self):
        while True:
            try:
                await asyncio.sleep(UPDATE_CHECK_INTERVAL)
                await self.check(force=True)
            except asyncio.CancelledError:
                return
            except Exception:
                continue

    async def check(self, force: bool = False) -> dict[str, Any]:
        async with self._lock:
            if (self._apply_task and not self._apply_task.done()) or (PROJECT_ROOT / ".tdm-update-lock").exists():
                return self.snapshot()
            now = time.monotonic()
            if not force and now - self._checked_at < 60:
                return self.snapshot()
            self._checked_at = now
            return await self._check_locked()

    async def _check_locked(self) -> dict[str, Any]:
        current = _read_version()
        self._state.update(state="checking", current_version=current, error=None)
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=4.0), follow_redirects=True) as client:
                response = await client.get(GITHUB_API_URL, headers=_headers())
            if response.status_code == 404:
                self._state.update(
                    state="no_release", latest_version=None, update_available=False,
                    release_url=None, published_at=None, release_notes="",
                )
                return self.snapshot()
            response.raise_for_status()
            data = response.json()
            tag = str(data.get("tag_name") or "").strip()
            if not tag or _version(tag) == (0, 0, 0, 0):
                raise RuntimeError("GitHub Release 的版本号无效")
            available = _version(tag) > _version(current)
            self._state.update(
                state="update_available" if available else "up_to_date",
                current_version=current, latest_version=tag, update_available=available,
                release_url=data.get("html_url"), published_at=data.get("published_at"),
                release_notes=str(data.get("body") or "")[:10000],
            )
        except Exception as exc:
            self._state.update(state="error", error=f"检查更新失败: {exc}")
        return self.snapshot()

    async def apply(self) -> dict[str, Any]:
        async with self._lock:
            if (PROJECT_ROOT / ".tdm-update-lock").exists():
                return {"success": False, "message": "已有更新或恢复操作尚未完成"}
            if not self.shutdown_callback:
                return {"success": False, "message": "请使用 python main.py 启动以支持自动更新"}
            if self._apply_task and not self._apply_task.done():
                return {"success": False, "message": "更新已经在进行中"}
            needs_check = not self._state.get("update_available")
        if needs_check:
            checked = await self.check(force=True)
            if not checked.get("update_available"):
                return {"success": False, "message": "当前没有可用更新"}
        async with self._lock:
            if self._apply_task and not self._apply_task.done():
                return {"success": False, "message": "更新已经在进行中"}
            if not self._state.get("update_available"):
                return {"success": False, "message": "当前没有可用更新"}
            self._state.update(state="preparing", progress=0.0, error=None)
            self._handed_off = False
            self._apply_task = asyncio.create_task(self._download_and_schedule())
            return {"success": True, "message": "更新已开始，程序将在准备完成后自动重启"}

    async def _download_and_schedule(self):
        stage = None
        lock_handed_off = False
        lock_acquired = False
        token = uuid.uuid4().hex
        reservation_owner = f"update:{os.getpid()}"
        reservation_held = False
        try:
            latest = str(self._state["latest_version"])
            service = await _run_blocking(_systemd_service)
            stage = Path(tempfile.mkdtemp(prefix=".tdm-update-stage-", dir=PROJECT_ROOT))
            self._stage = stage
            from app.config import load_config
            state = {
                "token": token, "phase": "downloading", "version": latest,
                "parent_pid": os.getpid(), "parent_identity": installer.process_identity(os.getpid()), "stage": str(stage),
                "backup": str(PROJECT_ROOT / f".tdm-update-backup-{token}"),
                "worker": str(stage / "update_worker.py"), "service": service,
                "port": int(load_config().get("server", {}).get("port", 8888)),
                "restart_args": [sys.executable, str(PROJECT_ROOT / "main.py"), *sys.argv[1:]],
            }
            _ensure_update_space(PROJECT_ROOT, UPDATE_MIN_FREE_BYTES)
            installer.copy_atomic(PROJECT_ROOT / "update_worker.py", Path(state["worker"]))
            installer.acquire_update(PROJECT_ROOT, state)
            lock_acquired = True
            (PROJECT_ROOT / ".tdm-update-result").unlink(missing_ok=True)
            from app.disk_budget import disk_budget
            disk_budget.reserve(
                reservation_owner,
                PROJECT_ROOT,
                UPDATE_RESERVATION_BYTES,
                _disk_safety_floor(),
            )
            reservation_held = True
            _ensure_update_space(PROJECT_ROOT, 128 * 1024 * 1024)
            archive = stage / "release.zip"
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=8.0), follow_redirects=True) as client:
                response = await client.get(GITHUB_API_URL, headers=_headers())
                if response.status_code == 404:
                    raise RuntimeError("GitHub 当前没有可用发布版本")
                response.raise_for_status()
                release = response.json()
                if str(release.get("tag_name") or "") != latest:
                    raise RuntimeError("GitHub Release 在下载期间发生变化，请重新检查")
                url = str(release.get("zipball_url") or "").strip()
                if not url.startswith("https://api.github.com/"):
                    raise RuntimeError("GitHub Release 下载地址不受信任")
                downloaded = 0
                async with client.stream("GET", url, headers=_headers()) as stream:
                    stream.raise_for_status()
                    total = int(stream.headers.get("content-length") or 0)
                    if total > MAX_ARCHIVE_BYTES:
                        raise RuntimeError("更新包超过大小限制")
                    _ensure_update_space(
                        PROJECT_ROOT,
                        min(MAX_ARCHIVE_BYTES, max(UPDATE_MIN_FREE_BYTES, total * 2)),
                    )
                    with archive.open("wb") as output:
                        async for chunk in stream.aiter_bytes(1024 * 1024):
                            downloaded += len(chunk)
                            if downloaded > MAX_ARCHIVE_BYTES:
                                raise RuntimeError("更新包超过大小限制")
                            _ensure_update_space(PROJECT_ROOT, len(chunk) + UPDATE_MIN_FREE_BYTES)
                            output.write(chunk)
                            self._state["progress"] = round(
                                min(45, downloaded / max(total, downloaded) * 45), 1,
                            )
            self._state.update(state="validating", progress=50.0)
            extract = stage / "extract"
            extract.mkdir()
            await _run_blocking(_safe_extract_zip, archive, extract, PROJECT_ROOT, archive.stat().st_size)
            source = _find_source_root(extract)
            files = [
                str(path.relative_to(source).as_posix())
                for path in source.rglob("*")
                if path.is_file() and not path.is_symlink()
                and not _protected(path.relative_to(source).as_posix())
            ]
            if "main.py" not in files or "app/main.py" not in files:
                raise RuntimeError("更新包不是有效的 TelDriveManager 源码")
            manifest = {
                "source_root": str(source), "files": files, "version": latest,
                "sha256": await _run_blocking(_sha256, archive), "release_url": self._state.get("release_url"),
            }
            installer.write_json(stage / "manifest.json", manifest)
            installer.save_state(PROJECT_ROOT, state, phase="preparing")
            def handoff():
                _spawn_detached([sys.executable, state["worker"], "--project", str(PROJECT_ROOT)], service)
                self._handed_off = True
            await _run_blocking(handoff)
            lock_handed_off = True
            self._handed_off = True
            self._state.update(state="restarting", progress=90.0)
            while (PROJECT_ROOT / ".tdm-update-lock").exists():
                await asyncio.sleep(1)
            self._state.update(state="idle", progress=0.0)
        except asyncio.CancelledError:
            lock_handed_off = lock_handed_off or self._handed_off
            if reservation_held:
                from app.disk_budget import disk_budget
                disk_budget.release(reservation_owner)
            if lock_acquired and not lock_handed_off:
                installer.release_update(PROJECT_ROOT, token)
            if stage and not lock_handed_off:
                shutil.rmtree(stage, ignore_errors=True)
            self._stage = None
            raise
        except Exception as exc:
            lock_handed_off = lock_handed_off or self._handed_off
            if reservation_held:
                from app.disk_budget import disk_budget
                disk_budget.release(reservation_owner)
            if lock_acquired and not lock_handed_off:
                installer.release_update(PROJECT_ROOT, token)
            self._state.update(state="error", error=f"更新失败: {exc}", progress=0.0)
            if stage and not lock_handed_off:
                shutil.rmtree(stage, ignore_errors=True)
            self._stage = None
        finally:
            if reservation_held:
                from app.disk_budget import disk_budget
                disk_budget.release(reservation_owner)


def _disk_safety_floor() -> int:
    try:
        from app.config import load_config
        value = float(load_config().get("aria2", {}).get("disk_protection_threshold_gb", 5) or 5)
        return max(UPDATE_MIN_FREE_BYTES, int(value * 1024 ** 3))
    except Exception:
        return 5 * 1024 ** 3


def _ensure_update_space(path: Path, required_bytes: int):
    free = shutil.disk_usage(path).free
    floor = _disk_safety_floor()
    if free < floor + max(0, int(required_bytes)):
        raise RuntimeError(f"磁盘空间不足，更新需要至少保留 {floor} 字节安全空间")


def _safe_extract_zip(
    archive: Path,
    destination: Path,
    volume_path: Path | None = None,
    reserved_bytes: int = 0,
):
    extracted = 0
    with zipfile.ZipFile(archive) as source:
        for member in source.infolist():
            name = PurePosixPath(member.filename)
            if name.is_absolute() or any(part in ("", ".", "..") for part in name.parts):
                raise RuntimeError("更新包包含非法路径")
            mode = (member.external_attr >> 16) & 0o170000
            if mode == 0o120000:
                raise RuntimeError("更新包包含符号链接")
            extracted += max(0, int(member.file_size))
            if extracted > MAX_EXTRACTED_BYTES:
                raise RuntimeError("更新包解压内容超过大小限制")
            if volume_path is not None:
                _ensure_update_space(
                    volume_path,
                    min(MAX_EXTRACTED_BYTES, int(reserved_bytes) + extracted + UPDATE_MIN_FREE_BYTES),
                )
            target = (destination / Path(*name.parts)).resolve()
            if destination.resolve() not in target.parents and target != destination.resolve():
                raise RuntimeError("更新包路径越界")
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.open(member) as input_file, target.open("wb") as output_file:
                    while chunk := input_file.read(1024 * 1024):
                        if volume_path is not None:
                            _ensure_update_space(volume_path, len(chunk) + UPDATE_MIN_FREE_BYTES)
                        output_file.write(chunk)


def _find_source_root(extract: Path) -> Path:
    candidates = [extract, *[item for item in extract.iterdir() if item.is_dir()]]
    for candidate in candidates:
        if (candidate / "main.py").is_file() and (candidate / "app" / "main.py").is_file():
            return candidate
    raise RuntimeError("更新包缺少主程序文件")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def _run_blocking(callback, *args):
    task = asyncio.create_task(asyncio.to_thread(callback, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _systemd_service():
    if not (os.getenv("INVOCATION_ID") or os.getenv("SYSTEMD_EXEC_PID")):
        return None
    if not shutil.which("systemd-run") or os.geteuid() != 0:
        raise RuntimeError("systemd 自动更新需要 root 服务及 systemd-run；当前服务继续运行")
    candidates = re.findall(r"/([^/\n]+\.service)(?:/|$)", Path("/proc/self/cgroup").read_text(), re.MULTILINE)
    for unit in candidates:
        if re.fullmatch(r"[A-Za-z0-9_.@:-]+\.service", unit):
            result = subprocess.run(["systemctl", "show", unit, "--property=MainPID", "--value"],
                                    capture_output=True, text=True, timeout=10, check=True)
            if result.stdout.strip() == str(os.getpid()):
                return unit
    raise RuntimeError("无法确认当前 systemd 服务，已取消自动更新")


def _spawn_detached(command: list[str], service=None):
    if service:
        subprocess.run([
            "systemd-run", "--quiet", "--collect", "--unit=tdm-update-" + uuid.uuid4().hex,
            "--property=Type=exec", "--property=Restart=on-failure", "--property=RestartSec=15",
            "--property=WorkingDirectory=" + str(PROJECT_ROOT), *command,
        ], check=True, timeout=30, stdin=subprocess.DEVNULL)
        return
    if os.name == "nt":
        flags = (
            getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
        )
        subprocess.Popen(
            command, cwd=PROJECT_ROOT, close_fds=True, creationflags=flags,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        subprocess.Popen(
            command, cwd=PROJECT_ROOT, close_fds=True, start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )


update_manager = UpdateManager()
