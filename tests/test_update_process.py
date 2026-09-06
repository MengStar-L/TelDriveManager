"""Exercise the real launcher, HTTP readiness and rollback with isolated web apps."""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
from urllib.request import ProxyHandler, build_opener

import update_worker as worker

ROOT = Path(__file__).resolve().parents[1]
APP_SOURCE = '''
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from app.updater import update_manager
import update_worker as worker
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
@asynccontextmanager
async def lifespan(app):
    if FAIL_STARTUP and os.getenv("TDM_UPDATE_TOKEN"):
        raise RuntimeError("injected startup failure")
    yield
app = FastAPI(lifespan=lifespan)
@app.get("/ping")
async def ping():
    return {"version": update_manager._startup_version}
@app.get("/api/update/health")
async def health():
    return {"token": os.getenv("TDM_UPDATE_TOKEN"), "version": update_manager._startup_version, "ready": True}
@app.post("/api/update/shutdown")
async def shutdown(request: Request):
    state = worker.read_json(ROOT / ".tdm-update-lock")
    assert request.headers["X-TDM-Update"] == state["token"]
    update_manager.shutdown_callback()
    return {"success": True}
'''


class ProcessUpdateTests(unittest.TestCase):
    def exercise(self, fail, systemd=False):
        with tempfile.TemporaryDirectory() as folder:
            project = Path(folder).resolve()
            stage = project / ".tdm-update-stage-test"
            source = stage / "source"
            backup = project / ".tdm-update-backup-test"
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            for root, failure in ((project, False), (source, fail)):
                (root / "app").mkdir(parents=True)
                worker.copy_atomic(ROOT / "main.py", root / "main.py")
                worker.copy_atomic(ROOT / "update_worker.py", root / "update_worker.py")
                (root / "app/__init__.py").write_text("")
                (root / "app/config.py").write_text(f"def load_config():\n    return {{'server': {{'port': {port}}}}}\n")
                (root / "app/updater.py").write_text(
                    "from pathlib import Path\nfrom types import SimpleNamespace\n"
                    "root = Path(__file__).resolve().parents[1]\n"
                    "update_manager = SimpleNamespace(shutdown_callback=None, _startup_version=(root / '.tdm-version').read_text().strip() if (root / '.tdm-version').exists() else 'dev')\n")
                (root / "app/main.py").write_text(f"FAIL_STARTUP = {failure!r}\n" + APP_SOURCE)
                (root / "requirements.txt").write_text("")
            (project / ".tdm-version").write_text("1.0.0")
            (project / "config.toml").write_text(f"[server]\nport={port}\n[aria2]\ndisk_protection_threshold_gb=0.1\n")
            (project / "tasks.db").write_bytes(b"preserved database fixture")
            (project / "channel.session").write_bytes(b"preserved session fixture")
            (project / "downloads").mkdir()
            (project / "downloads/partial.bin").write_bytes(b"preserved partial download")
            preserved = {name: (project / name).read_bytes() for name in
                         ("config.toml", "tasks.db", "channel.session", "downloads/partial.bin")}
            unit_file = None
            children = []
            def spawn(args):
                child = subprocess.Popen(args, cwd=project, stdin=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                         env=dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONDONTWRITEBYTECODE="1"))
                children.append(child)
                return child
            opener = build_opener(ProxyHandler({}))
            def ping():
                with opener.open(f"http://127.0.0.1:{port}/ping", timeout=1) as response:
                    return json.load(response)
            def await_ping(version):
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    try:
                        if ping()["version"] == version:
                            return
                    except Exception:
                        pass
                    time.sleep(0.05)
                self.fail("Isolated application did not serve " + version)
            try:
                if systemd:
                    unit = "tdm-integration-" + uuid.uuid4().hex + ".service"
                    unit_file = Path("/run/systemd/system") / unit
                    unit_file.write_text(f"[Service]\nType=simple\nUser=root\nWorkingDirectory={project}\n"
                                         f"ExecStart={sys.executable} {project / 'main.py'}\nRestart=always\nRestartSec=1\n")
                    subprocess.run(["systemctl", "daemon-reload"], check=True)
                    subprocess.run(["systemctl", "start", unit], check=True)
                    await_ping("1.0.0")
                    source_worker = source / "update_worker.py"
                    source_worker.write_text(source_worker.read_text().replace("STARTUP_TIMEOUT_SECONDS = 120", "STARTUP_TIMEOUT_SECONDS = 5"))
                    result = subprocess.run([sys.executable, str(ROOT / "deploy/update-linux.py"),
                                             "--project", str(project), "--source", str(source), "--stage", str(stage),
                                             "--service", unit, "--version", "99.0.0"],
                                            capture_output=True, text=True, timeout=180)
                    self.assertEqual(result.returncode, 1 if fail else 0, result.stdout + result.stderr)
                    await_ping("1.0.0" if fail else "99.0.0")
                    self.assertFalse((project / ".tdm-update-lock").exists())
                    for name, contents in preserved.items():
                        self.assertEqual((project / name).read_bytes(), contents, name)
                    return
                parent = spawn([sys.executable, str(project / "main.py")])
                await_ping("1.0.0")
                saved_worker = stage / "worker.py"
                worker.copy_atomic(ROOT / "update_worker.py", saved_worker)
                state = dict(token="integration-token", phase="preparing", stage=str(stage), backup=str(backup),
                             version="99.0.0", worker=str(saved_worker), parent_pid=parent.pid,
                             restart_args=[sys.executable, str(project / "main.py")], port=port, service=None)
                manifest = dict(version="99.0.0", source_root=str(source),
                                files=[str(p.relative_to(source).as_posix()) for p in source.rglob("*") if p.is_file()])
                worker.write_json(stage / "manifest.json", manifest)
                worker.acquire_update(project, state)
                original_wait = worker.wait_ready
                def assert_ready(transaction, process):
                    self.assertTrue(backup.exists())
                    original_wait(transaction, process)
                    self.assertEqual(ping()["version"], "99.0.0")
                with worker.transaction_guard(project), patch.object(worker, "start_process", side_effect=spawn), \
                        patch.object(worker, "wait_parent", side_effect=lambda pid: parent.wait(timeout=15)), \
                        patch.object(worker, "wait_ready", side_effect=assert_ready), \
                        patch.object(worker, "STARTUP_TIMEOUT_SECONDS", 10), \
                        patch.object(worker, "safety_floor", return_value=worker.MIN_FREE_BYTES):
                    result = worker.install(project, state)
                self.assertEqual(result, 0)
                expected = "1.0.0" if fail else "99.0.0"
                await_ping(expected)
                self.assertEqual((project / ".tdm-version").read_text().strip(), expected)
                self.assertFalse((project / ".tdm-update-lock").exists())
                self.assertFalse(backup.exists())
                self.assertEqual(worker.read_json(project / ".tdm-update-result")["outcome"], "rolled_back" if fail else "committed")
            finally:
                if unit_file:
                    subprocess.run(["systemctl", "stop", unit_file.name], check=False)
                    unit_file.unlink(missing_ok=True)
                    subprocess.run(["systemctl", "daemon-reload"], check=True)
                for child in children:
                    if child.poll() is None:
                        child.terminate()
                    child.wait(timeout=15)

    def test_real_process_is_healthy_before_commit(self):
        self.exercise(False)

    def test_real_startup_failure_rolls_back_and_serves_old_version(self):
        self.exercise(True)


@unittest.skipUnless(os.environ.get("TDM_SYSTEMD_TEST") == "1", "Set TDM_SYSTEMD_TEST=1 on a root systemd test host")
class SystemdUpdateTests(unittest.TestCase):
    exercise = ProcessUpdateTests.exercise

    def test_systemd_upgrade_preserves_data(self):
        self.exercise(False, systemd=True)

    def test_systemd_failed_upgrade_restores_old_service(self):
        self.exercise(True, systemd=True)
