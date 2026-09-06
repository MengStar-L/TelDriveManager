"""Durable update transaction and recovery. This module uses only the stdlib."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from urllib.request import ProxyHandler, Request, build_opener

UPDATE_PROTOCOL = 2
STARTUP_TIMEOUT_SECONDS = 120
MIN_FREE_BYTES = 64 * 1024 ** 2
RUNTIME_LIMIT_BYTES = 2 * 1024 ** 3
PROTECTED_TOP_LEVEL = {".git", ".venv", "venv", "downloads", "aria2", "history", "telegram_relay", ".tdm-runtimes"}
PROTECTED_NAMES = {"config.toml", "config.bak.toml", ".env", "tasks.db", "tasks.db-shm", "tasks.db-wal",
                   ".tdm-version", ".tdm-runtime", "pikpak_token.json", "db_backup_pending_deletion.json", "file_msg_map.json"}
MARKERS = (".tdm-version", ".tdm-runtime")


class CommandFailure(RuntimeError):
    pass


def protected(relative: str) -> bool:
    path = PurePosixPath(relative)
    return (not path.parts or path.is_absolute() or "\\" in relative or ":" in relative
            or any(part in ("", ".", "..") for part in relative.split("/"))
            or path.parts[0].lower() in PROTECTED_TOP_LEVEL
            or path.name.lower() in PROTECTED_NAMES
            or path.name.lower().endswith((".db", ".db-shm", ".db-wal", ".session", ".session-journal", ".log"))
            or any(part.startswith(".tdm-update-") or part == "__pycache__" for part in path.parts))


def fsync_directory(path: Path):
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def durable_mkdir(path: Path):
    if not path.exists():
        durable_mkdir(path.parent)
        path.mkdir(exist_ok=True)
        fsync_directory(path.parent)


def write_json(path: Path, value):
    durable_mkdir(path.parent)
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, ensure_ascii=False)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


@contextmanager
def transaction_guard(project: Path):
    """The OS releases this mutex after a crash; its inode is never unlinked."""
    with (project / ".tdm-update-guard").open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            handle.seek(0)
            if not handle.read(1):
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


def acquire_update(project: Path, state: dict):
    with transaction_guard(project):
        path = project / ".tdm-update-lock"
        if path.exists():
            raise RuntimeError("An update or recovery is already pending")
        write_json(path, state)


def release_update(project: Path, token: str):
    path = project / ".tdm-update-lock"
    if path.exists() and read_json(path).get("token") == token:
        path.unlink()
        fsync_directory(project)


def save_state(project: Path, state: dict, **changes):
    state.update(changes)
    path = project / ".tdm-update-lock"
    if read_json(path).get("token") != state["token"]:
        raise RuntimeError("Update ownership changed")
    write_json(path, state)


def checked_path(root: Path, relative: str) -> Path:
    path = root.joinpath(*PurePosixPath(relative).parts)
    if root not in path.resolve().parents:
        raise RuntimeError("Update path escapes its root")
    for item in (path, *path.parents):
        if item == root:
            break
        if item.is_symlink() or (hasattr(item, "is_junction") and item.is_junction()):
            raise RuntimeError("Update path contains a filesystem link")
    if path.exists() and not path.is_file():
        raise RuntimeError("Update target is not a regular file")
    return path


def copy_atomic(source: Path, target: Path):
    durable_mkdir(target.parent)
    temporary = target.with_name(f".{target.name}.tdm-new-{uuid.uuid4().hex}")
    try:
        shutil.copyfile(source, temporary)
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        shutil.copystat(source, temporary)
        os.replace(temporary, target)
        fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def safety_floor(project: Path) -> int:
    try:
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib
        config = tomllib.loads((project / "config.toml").read_text(encoding="utf-8"))
        return max(MIN_FREE_BYTES, int(float(config.get("aria2", {}).get("disk_protection_threshold_gb", 5)) * 1024 ** 3))
    except Exception:
        return 5 * 1024 ** 3


def ensure_install_space(project: Path, source: Path, files: list[str]):
    sizes = [checked_path(source, name).stat().st_size for name in files]
    old_sizes = [path.stat().st_size for name in files if (path := checked_path(project, name)).exists()]
    required = safety_floor(project) + sum(sizes) + sum(old_sizes) + max(sizes, default=0)
    if shutil.disk_usage(project).free < required:
        raise RuntimeError("Insufficient disk space for update and rollback")


def backup_and_install(project: Path, stage: Path, backup: Path, manifest: dict, state=None):
    project, stage = project.resolve(), stage.resolve()
    source = Path(manifest["source_root"]).resolve()
    files = list(dict.fromkeys(manifest.get("files", [])))
    if stage not in source.parents or not files or any(protected(name) for name in files):
        raise RuntimeError("Invalid update manifest")
    if not {"main.py", "app/main.py"}.issubset(files):
        raise RuntimeError("Release is missing application entry points")
    ensure_install_space(project, source, files)
    if backup.exists():
        raise RuntimeError("Backup already exists; recovery is required")
    durable_mkdir(backup)
    records = []
    # Back up everything and persist the full undo log before replacing any file.
    for name in [*files, *MARKERS]:
        target = checked_path(project, name)
        if target.exists():
            copy_atomic(target, backup / name)
        records.append({"path": name, "old": target.exists()})
    saved = {"files": records, "marker_existed": (project / ".tdm-version").exists(), "phase": "installing"}
    write_json(backup / "manifest.json", saved)
    if state is not None:
        save_state(project, state, undo_ready=True)
    for name in files:
        copy_atomic(checked_path(source, name), checked_path(project, name))
    marker_source = stage / "version.txt"
    marker_source.write_text(str(manifest["version"]) + "\n", encoding="utf-8")
    copy_atomic(marker_source, project / ".tdm-version")
    if manifest.get("runtime"):
        write_json(project / ".tdm-runtime", {"python": manifest["runtime"]})


def rollback(project: Path, backup: Path, records: list[dict], marker_existed=None):
    for record in reversed(records):
        name = record["path"]
        if protected(name) and name not in MARKERS:
            raise RuntimeError("Rollback contains a protected path")
        target = checked_path(project.resolve(), name)
        if record["old"]:
            old = checked_path(backup.resolve(), name)
            if not old.is_file():
                raise RuntimeError(f"Rollback backup is missing: {name}")
            copy_atomic(old, target)
        else:
            target.unlink(missing_ok=True)
            if target.parent.exists():
                fsync_directory(target.parent)


def run_checked(command: list[str], cwd: Path, project: Path, timeout=900, runtime: Path | None = None):
    if shutil.disk_usage(project).free < safety_floor(project) + MIN_FREE_BYTES:
        raise RuntimeError("Insufficient disk reserve for dependency preparation")
    tail = deque(maxlen=40)
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PIP_NO_CACHE_DIR="1", TMPDIR=str(cwd), TEMP=str(cwd), TMP=str(cwd))
    process = subprocess.Popen(command, cwd=cwd, env=environment, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    def drain():
        while block := process.stdout.read(4096):
            tail.append(block.decode("utf-8", errors="replace"))
    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    deadline = time.monotonic() + timeout
    try:
        while process.poll() is None:
            if time.monotonic() > deadline:
                raise RuntimeError("Dependency preparation timed out")
            if shutil.disk_usage(project).free < safety_floor(project) + MIN_FREE_BYTES:
                raise RuntimeError("Disk safety reserve reached during dependency preparation")
            if runtime:
                try:
                    size = sum(p.stat().st_size for root in (runtime, cwd) for p in root.rglob("*") if p.is_file())
                except FileNotFoundError:
                    size = 0
                if size > RUNTIME_LIMIT_BYTES:
                    raise RuntimeError("Update runtime exceeds its disk budget")
            time.sleep(0.25)
        reader.join(timeout=5)
        if process.returncode:
            raise CommandFailure("Update preflight failed: " + "".join(tail)[-4000:])
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        reader.join(timeout=5)
        process.stdout.close()


def prepare_runtime(project: Path, stage: Path, state: dict, manifest: dict):
    source = Path(manifest["source_root"])
    tree = ast.parse((source / "main.py").read_text(encoding="utf-8"))
    if not any(isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "UPDATE_PROTOCOL" for t in node.targets)
               and isinstance(node.value, ast.Constant) and node.value.value == UPDATE_PROTOCOL for node in tree.body):
        raise RuntimeError("Release requires a manual launcher upgrade (unsupported update protocol)")
    for name in manifest["files"]:
        if name.endswith(".py"):
            path = checked_path(source.resolve(), name)
            compile(path.read_bytes(), str(path), "exec")
    requirements = source / "requirements.txt"
    if not requirements.is_file():
        raise RuntimeError("Release is missing requirements.txt")
    interpreter = state["restart_args"][0]
    try:
        report = stage / "requirements-report.json"
        run_checked([interpreter, "-m", "pip", "install", "--dry-run", "--no-index", "--report", str(report), "-r", str(requirements)], stage, project, timeout=60)
        if read_json(report).get("install"):
            raise CommandFailure("The release requires new dependencies")
        run_checked([interpreter, "-m", "pip", "check"], stage, project, timeout=60)
    except CommandFailure:
        runtime = project / ".tdm-runtimes" / state["token"]
        save_state(project, state, new_runtime=str(runtime))
        durable_mkdir(runtime)
        write_json(runtime / ".tdm-owner", {"project": str(project.resolve())})
        run_checked([interpreter, "-m", "venv", "--copies", str(runtime)], stage, project, runtime=runtime)
        interpreter = str(runtime / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
        run_checked([interpreter, "-m", "pip", "install", "--no-cache-dir", "--only-binary=:all:", "-r", str(requirements)], stage, project, runtime=runtime)
        run_checked([interpreter, "-m", "pip", "check"], stage, project, runtime=runtime)
        manifest["runtime"] = interpreter
    run_checked([interpreter, "-B", "-c", "import app.main"], source, project, timeout=90)
    write_json(stage / "manifest.json", manifest)


def systemctl(unit: str, action: str):
    if not re.fullmatch(r"[A-Za-z0-9_.@:-]+\.service", unit):
        raise RuntimeError("Invalid systemd service name")
    subprocess.run(["systemctl", action, unit], check=True, timeout=180, stdin=subprocess.DEVNULL)


def local_request(state: dict, path: str, method="GET"):
    request = Request(f"http://127.0.0.1:{int(state['port'])}/api/update/{path}", method=method,
                      headers={"X-TDM-Update": state["token"]})
    with build_opener(ProxyHandler({})).open(request, timeout=3) as response:
        return json.load(response)


def process_identity(pid: int):
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.GetProcessTimes.argtypes = [wintypes.HANDLE, *[ctypes.POINTER(wintypes.FILETIME)] * 4]
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            if ctypes.get_last_error() == 87:
                return ""
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            values = [wintypes.FILETIME() for _ in range(4)]
            if not kernel.GetProcessTimes(handle, *[ctypes.byref(value) for value in values]):
                raise ctypes.WinError(ctypes.get_last_error())
            if values[1].dwHighDateTime or values[1].dwLowDateTime:
                return ""
            return f"{pid}:{values[0].dwHighDateTime}:{values[0].dwLowDateTime}"
        finally:
            kernel.CloseHandle(handle)
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        if fields[0] == "Z":
            return ""
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        return f"{boot}:{pid}:{fields[19]}"
    except FileNotFoundError:
        return ""


def pid_exists(pid: int):
    return bool(process_identity(pid))


def same_process(state: dict, prefix: str):
    pid = state.get(prefix + "_pid")
    if not pid:
        return False
    current = process_identity(pid)
    return bool(current and current == state.get(prefix + "_identity", current))


def wait_parent(pid: int, timeout=180):
    deadline = time.monotonic() + timeout
    while pid_exists(pid) and time.monotonic() < deadline:
        time.sleep(0.25)
    if pid_exists(pid):
        raise RuntimeError("Application did not shut down; installation refused")


def start_process(args: list[str]):
    kwargs = dict(cwd=str(Path(args[1]).parent), stdin=subprocess.DEVNULL)
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x200)
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(args, **kwargs)


def start_application(state: dict):
    if state.get("service"):
        systemctl(state["service"], "start")
        return None
    args = list(state["restart_args"])
    marker = Path(args[1]).parent / ".tdm-runtime"
    if marker.exists():
        args[0] = read_json(marker)["python"]
    return start_process(args)


def wait_ready(state: dict, process=None):
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"New application exited: {process.returncode}")
        try:
            data = local_request(state, "health")
            if data.get("token") == state["token"] and data.get("version") == state["version"] and data.get("ready"):
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("New application did not become ready")


def stop_candidate(state: dict, process=None):
    if state.get("service"):
        systemctl(state["service"], "stop")
    elif process is not None:
        if process.poll() is None:
            try:
                local_request(state, "shutdown", "POST")
            except Exception:
                process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    elif same_process(state, "candidate"):
        local_request(state, "shutdown", "POST")
        wait_parent(state["candidate_pid"])


def cleanup_runtimes(project: Path, state: dict):
    root = project / ".tdm-runtimes"
    keep = {Path(sys.executable).absolute(), Path(state["restart_args"][0]).absolute()}
    marker = project / ".tdm-runtime"
    if marker.exists():
        keep.add(Path(read_json(marker)["python"]).absolute())
    if not root.exists() or root.is_symlink():
        return
    for runtime in root.iterdir():
        if (not re.fullmatch(r"[0-9a-f]{32}", runtime.name) or not runtime.is_dir()
                or runtime.is_symlink() or (hasattr(runtime, "is_junction") and runtime.is_junction())
                or any(runtime in interpreter.parents for interpreter in keep)):
            continue
        try:
            if read_json(runtime / ".tdm-owner").get("project") == str(project.resolve()):
                shutil.rmtree(runtime)
        except OSError:
            pass


def finish(project: Path, state: dict, outcome: str, error=""):
    write_json(project / ".tdm-update-result", {"token": state["token"], "version": state["version"], "outcome": outcome, "error": error})
    save_state(project, state, phase=outcome)
    release_update(project, state["token"])
    # Failed recovery never reaches here; keep its backups and lock for a retry.
    shutil.rmtree(state["backup"], ignore_errors=True)
    shutil.rmtree(state["stage"], ignore_errors=True)
    if outcome != "committed" and state.get("new_runtime"):
        shutil.rmtree(state["new_runtime"], ignore_errors=True)
    if outcome == "committed":
        try:
            cleanup_runtimes(project, state)
        except (OSError, ValueError, KeyError):
            pass


def recover(project: Path, state: dict, process=None, startup=False):
    phase = state["phase"]
    if phase in ("committed", "rolled_back", "aborted"):
        finish(project, state, phase, state.get("error", ""))
        return
    if phase in ("downloading", "preparing"):
        finish(project, state, "aborted", state.get("error", "Update preparation interrupted"))
        return
    if startup and same_process(state, "candidate"):
        raise RuntimeError("A candidate instance may still be running; use the saved worker to recover before starting another instance")
    if not startup:
        stop_candidate(state, process)
        if phase == "stopping" and not state.get("service"):
            wait_parent(state["parent_pid"])
    save_state(project, state, phase="rolling_back")
    backup = Path(state["backup"])
    manifest_path = backup / "manifest.json"
    if state.get("undo_ready") and not manifest_path.is_file():
        raise RuntimeError("Durable undo log is missing; refusing to accept an incomplete rollback")
    if manifest_path.exists():
        records = read_json(manifest_path)["files"]
        for record in records:
            target = checked_path(project.resolve(), record["path"])
            for partial in target.parent.glob(f".{target.name}.tdm-new-*"):
                if not partial.is_symlink() and partial.is_file():
                    partial.unlink()
        rollback(project, backup, records)
    # Without an undo log replacement has not begun; all backups precede the log.
    finish(project, state, "rolled_back", state.get("error", "Update interrupted; previous version restored"))


def install(project: Path, state: dict):
    stage = Path(state["stage"])
    process = None
    try:
        manifest = read_json(stage / "manifest.json")
        prepare_runtime(project, stage, state, manifest)
        ensure_install_space(project, Path(manifest["source_root"]), manifest["files"])
        save_state(project, state, phase="stopping")
        if state.get("service"):
            systemctl(state["service"], "stop")
        else:
            local_request(state, "shutdown", "POST")
        wait_parent(state["parent_pid"])
        save_state(project, state, phase="installing")
        backup_and_install(project, stage, Path(state["backup"]), manifest, state)
        save_state(project, state, phase="validating")
        process = start_application(state)
        if process:
            save_state(project, state, candidate_pid=process.pid, candidate_identity=process_identity(process.pid))
        wait_ready(state, process)
        finish(project, state, "committed")
        return 0
    except Exception as exc:
        state["error"] = str(exc)
        try:
            needs_restart = state["phase"] not in ("downloading", "preparing")
            recover(project, state, process)
            if needs_restart:
                start_application(state)
        except Exception as recovery_error:
            error = f"{exc}; recovery failed: {recovery_error}"
            if (project / ".tdm-update-lock").exists():
                save_state(project, state, error=error)
            else:
                write_json(project / ".tdm-update-result", {"outcome": "rolled_back", "error": error})
            print(state["error"], file=sys.stderr)
            return 2
        return 0


def startup_gate(project: Path):
    """Never import application code from an unfinished installation."""
    path = project / ".tdm-update-lock"
    deadline = time.monotonic() + 180
    while path.exists():
        state = read_json(path)
        try:
            with transaction_guard(project):
                owner_running = False
        except OSError:
            owner_running = True
        if owner_running and state["phase"] == "validating":
            os.environ["TDM_UPDATE_TOKEN"] = state["token"]
            save_state(project, state, candidate_pid=os.getpid(), candidate_identity=process_identity(os.getpid()))
            break
        if state["phase"] in ("downloading", "preparing") and same_process(state, "parent"):
            owner_running = True
        if not owner_running:
            result = subprocess.run([sys.executable, state["worker"], "--project", str(project), "--recover", "--startup-recovery"], check=False)
            if result.returncode:
                raise RuntimeError("Update recovery failed; backups retained. See .tdm-update-lock")
            continue
        if time.monotonic() >= deadline:
            raise RuntimeError("Update is still running; refusing to start mixed application files")
        time.sleep(0.25)
    marker = project / ".tdm-runtime"
    if marker.exists():
        interpreter = Path(read_json(marker)["python"])
        if project / ".tdm-runtimes" not in interpreter.parents or not interpreter.is_file():
            raise RuntimeError("Invalid or missing update runtime")
        if os.path.abspath(sys.executable) != os.path.abspath(interpreter):
            os.execv(str(interpreter), [str(interpreter), str(project / "main.py"), *sys.argv[1:]])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--recover", action="store_true")
    parser.add_argument("--startup-recovery", action="store_true")
    options = parser.parse_args()
    project = Path(options.project).resolve()
    try:
        with transaction_guard(project):
            path = project / ".tdm-update-lock"
            if not path.exists():
                return 0
            state = read_json(path)
            if options.recover or state["phase"] != "preparing":
                phase = state["phase"]
                recover(project, state, startup=options.startup_recovery)
                if not options.startup_recovery and phase not in ("downloading", "preparing", "committed", "aborted"):
                    start_application(state)
                return 0
            return install(project, state)
    except Exception as exc:
        print(f"Update/recovery stopped; transaction and backups retained: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
