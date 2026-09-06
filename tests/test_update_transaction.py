import asyncio
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import update_worker as worker
from app import updater

ROOT = Path(__file__).resolve().parents[1]


class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.project = Path(self.temp.name).resolve() / "project"
        self.stage = self.project / ".tdm-update-stage-test"
        self.source = self.stage / "source"
        self.backup = self.project / ".tdm-update-backup-test"
        for root in (self.project, self.source):
            (root / "app").mkdir(parents=True)
        for name in ("main.py", "app/main.py"):
            (self.project / name).write_text("old " + name)
            (self.source / name).write_text("new " + name)
        self.manifest = {"source_root": str(self.source), "files": ["main.py", "app/main.py"], "version": "99.0.0"}
        worker.write_json(self.stage / "manifest.json", self.manifest)
        self.state = dict(token="test-token", phase="preparing", stage=str(self.stage), backup=str(self.backup),
                          version="99.0.0", worker=str(self.stage / "worker.py"), parent_pid=99999999,
                          restart_args=[sys.executable, str(self.project / "main.py")], port=12345, service=None)
        worker.acquire_update(self.project, self.state)
        self.lock = self.project / ".tdm-update-lock"
        space = patch.object(worker, "ensure_install_space")
        space.start()
        self.addCleanup(space.stop)

    def install_files(self):
        worker.backup_and_install(self.project, self.stage, self.backup, self.manifest)

    def test_competing_update_and_wrong_token_cannot_remove_lock(self):
        original = self.lock.read_bytes()
        with self.assertRaises(RuntimeError):
            worker.acquire_update(self.project, dict(self.state, token="other"))
        worker.release_update(self.project, "other")
        self.assertEqual(self.lock.read_bytes(), original)

    def test_project_locks_are_independent(self):
        other = self.project.parent / "another-project"
        other.mkdir()
        worker.acquire_update(other, dict(self.state, token="other"))
        worker.release_update(other, "other")
        self.assertTrue(self.lock.exists())

    def test_interruption_after_first_replace_has_complete_undo_log(self):
        original = worker.copy_atomic
        def interrupt(source, target):
            original(source, target)
            if target == self.project / "main.py":
                raise KeyboardInterrupt("simulated hard termination")
        with patch.object(worker, "copy_atomic", side_effect=interrupt), self.assertRaises(KeyboardInterrupt):
            self.install_files()
        saved = worker.read_json(self.backup / "manifest.json")
        self.assertEqual(len(saved["files"]), 4)
        worker.rollback(self.project, self.backup, saved["files"])
        self.assertEqual((self.project / "main.py").read_text(), "old main.py")
        self.assertEqual((self.project / "app/main.py").read_text(), "old app/main.py")
        self.assertFalse((self.project / ".tdm-version").exists())

    def test_failure_before_undo_log_never_modifies_source(self):
        with patch.object(worker, "write_json", side_effect=OSError(28, "full")), self.assertRaises(OSError):
            self.install_files()
        self.assertEqual((self.project / "main.py").read_text(), "old main.py")

    def test_failed_rollback_preserves_backups_and_lock_then_can_retry(self):
        self.install_files()
        worker.save_state(self.project, self.state, phase="installing")
        with patch.object(worker, "copy_atomic", side_effect=OSError(28, "full")), self.assertRaises(OSError):
            worker.recover(self.project, self.state)
        self.assertTrue(self.lock.exists())
        self.assertTrue((self.backup / "main.py").exists())
        worker.recover(self.project, self.state)
        self.assertEqual((self.project / "main.py").read_text(), "old main.py")
        self.assertFalse(self.lock.exists())

    def test_missing_backup_is_not_silently_accepted(self):
        self.install_files()
        (self.backup / "main.py").unlink()
        worker.save_state(self.project, self.state, phase="installing")
        with self.assertRaisesRegex(RuntimeError, "missing"):
            worker.recover(self.project, self.state)
        self.assertTrue(self.lock.exists())

    def test_missing_undo_log_after_installation_blocks_recovery(self):
        self.install_files()
        (self.backup / "manifest.json").unlink()
        worker.save_state(self.project, self.state, phase="validating", undo_ready=True)
        with self.assertRaisesRegex(RuntimeError, "undo log"):
            worker.recover(self.project, self.state)
        self.assertTrue(self.lock.exists())
        self.assertTrue((self.backup / "main.py").exists())

    def test_hard_killed_installer_recovers_on_next_startup(self):
        saved_worker = Path(self.state["worker"])
        worker.copy_atomic(ROOT / "update_worker.py", saved_worker)
        worker.save_state(self.project, self.state, phase="installing")
        script = '''
import os
from pathlib import Path
import update_worker as w
project = Path(PROJECT)
state = w.read_json(project / '.tdm-update-lock')
stage = Path(state['stage'])
original = w.copy_atomic
def interrupted(source, target):
    original(source, target)
    if target == project / 'main.py':
        os._exit(79)
w.copy_atomic = interrupted
w.ensure_install_space = lambda *args: None
with w.transaction_guard(project):
    w.backup_and_install(project, stage, Path(state['backup']), w.read_json(stage / 'manifest.json'), state)
'''.replace("PROJECT", repr(str(self.project)))
        result = subprocess.run([sys.executable, "-B", "-c", script], cwd=ROOT, timeout=15)
        self.assertEqual(result.returncode, 79)
        self.assertEqual((self.project / "main.py").read_text(), "new main.py")
        worker.startup_gate(self.project)
        self.assertEqual((self.project / "main.py").read_text(), "old main.py")
        self.assertFalse(self.lock.exists())

    def test_startup_timeout_never_imports_or_unlocks_installing_files(self):
        worker.save_state(self.project, self.state, phase="installing")
        with worker.transaction_guard(self.project), patch.object(worker.time, "monotonic", side_effect=[0, 181]):
            with self.assertRaisesRegex(RuntimeError, "refusing"):
                worker.startup_gate(self.project)
        self.assertTrue(self.lock.exists())

    def test_alive_process_without_health_response_is_not_success(self):
        with patch.object(worker, "STARTUP_TIMEOUT_SECONDS", 0):
            with self.assertRaisesRegex(RuntimeError, "ready"):
                worker.wait_ready(self.state, Mock(poll=Mock(return_value=None)))

    def test_wrong_instance_health_is_rejected(self):
        with patch.object(worker, "local_request", return_value={"ready": True, "token": "wrong", "version": "99.0.0"}), \
                patch.object(worker.time, "monotonic", side_effect=[0, 0, 200]), patch.object(worker.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "ready"):
                worker.wait_ready(self.state)

    def test_systemd_starts_new_service_before_health_and_only_then_commits(self):
        self.state["service"] = "teldrive-manager.service"
        events = []
        def health(state, process):
            self.assertTrue(self.backup.exists())
            self.assertEqual(worker.read_json(self.lock)["phase"], "validating")
            events.append("healthy")
        with patch.object(worker, "prepare_runtime"), patch.object(worker, "wait_parent"), \
                patch.object(worker, "systemctl", side_effect=lambda unit, action: events.append(action)), \
                patch.object(worker, "wait_ready", side_effect=health):
            result = worker.install(self.project, self.state)
        self.assertEqual(result, 0)
        self.assertEqual(events, ["stop", "start", "healthy"])
        self.assertFalse(self.backup.exists())
        self.assertFalse(self.lock.exists())

    def test_systemd_failure_stops_candidate_before_rollback_and_restarts_old(self):
        self.state["service"] = "teldrive-manager.service"
        events = []
        def service(unit, action):
            events.append((action, (self.project / "main.py").read_text()))
        with patch.object(worker, "prepare_runtime"), patch.object(worker, "wait_parent"), \
                patch.object(worker, "systemctl", side_effect=service), \
                patch.object(worker, "wait_ready", side_effect=RuntimeError("startup failed")):
            self.assertEqual(worker.install(self.project, self.state), 0)
        self.assertEqual(events, [("stop", "old main.py"), ("start", "new main.py"),
                                  ("stop", "new main.py"), ("start", "old main.py")])

    def test_dependency_failure_does_not_stop_or_modify_running_application(self):
        with patch.object(worker, "prepare_runtime", side_effect=RuntimeError("dependency unavailable")), \
                patch.object(worker, "systemctl") as service, patch.object(worker, "local_request") as request:
            self.assertEqual(worker.install(self.project, self.state), 0)
        service.assert_not_called()
        request.assert_not_called()
        self.assertEqual((self.project / "main.py").read_text(), "old main.py")
        self.assertFalse(self.lock.exists())

    def test_changed_dependencies_use_new_venv_and_leave_original_untouched(self):
        (self.source / "main.py").write_text("UPDATE_PROTOCOL = 2\n")
        (self.source / "app/main.py").write_text("")
        (self.source / "requirements.txt").write_text("new-package==1.0\n")
        calls = []
        def run(command, *args, **kwargs):
            calls.append(command)
            if "--dry-run" in command:
                raise worker.CommandFailure("new dependency needed")
        with patch.object(worker, "run_checked", side_effect=run):
            worker.prepare_runtime(self.project, self.stage, self.state, self.manifest)
        installs = [command for command in calls if "install" in command and "--dry-run" not in command]
        self.assertEqual(len(installs), 1)
        self.assertIn(".tdm-runtimes", installs[0][0])
        self.assertNotEqual(installs[0][0], sys.executable)
        self.assertFalse((self.project / ".tdm-runtime").exists())

    def test_disk_guard_failure_does_not_start_dependency_installation(self):
        (self.source / "main.py").write_text("UPDATE_PROTOCOL = 2\n")
        (self.source / "app/main.py").write_text("")
        (self.source / "requirements.txt").write_text("new-package==1.0\n")
        with patch.object(worker, "run_checked", side_effect=RuntimeError("disk reserve reached")) as run:
            with self.assertRaisesRegex(RuntimeError, "disk reserve"):
                worker.prepare_runtime(self.project, self.stage, self.state, self.manifest)
        self.assertEqual(run.call_count, 1)
        self.assertNotIn("new_runtime", self.state)

    def test_pid_reuse_does_not_block_recovery(self):
        self.state.update(parent_pid=123, parent_identity="old-process")
        with patch.object(worker, "process_identity", return_value="different-process"):
            self.assertFalse(worker.same_process(self.state, "parent"))

    def test_protected_data_and_parent_links_cannot_be_overwritten(self):
        for name in (".env", ".tdm-runtime", "app/file_msg_map.json", "a.session-journal", "../escape", "C:/escape"):
            self.assertTrue(worker.protected(name), name)
        with patch.object(Path, "is_symlink", side_effect=lambda: True):
            with self.assertRaisesRegex(RuntimeError, "filesystem link"):
                worker.checked_path(self.project, "app/main.py")

    def test_rollback_restores_previous_runtime_pointer(self):
        old = {"python": "old-runtime"}
        worker.write_json(self.project / ".tdm-runtime", old)
        self.manifest["runtime"] = "new-runtime"
        self.install_files()
        worker.rollback(self.project, self.backup, worker.read_json(self.backup / "manifest.json")["files"])
        self.assertEqual(worker.read_json(self.project / ".tdm-runtime"), old)

    def test_systemd_installer_uses_separate_transient_service(self):
        with patch.object(updater.subprocess, "run") as run, patch.object(updater.subprocess, "Popen") as popen:
            updater._spawn_detached(["python", "worker.py"], "app.service")
        command = run.call_args.args[0]
        self.assertEqual(command[0], "systemd-run")
        self.assertIn("--property=Type=exec", command)
        self.assertIn("--property=Restart=on-failure", command)
        popen.assert_not_called()


class UpdateAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_lock_acquisition_keeps_existing_owner(self):
        with tempfile.TemporaryDirectory() as folder:
            project = Path(folder)
            worker.copy_atomic(ROOT / "update_worker.py", project / "update_worker.py")
            worker.acquire_update(project, {"token": "owner"})
            with patch.object(updater, "PROJECT_ROOT", project), patch.object(updater, "_systemd_service", return_value=None), \
                    patch.object(updater, "_ensure_update_space"), \
                    patch.object(updater, "_read_version", return_value="1.0.0"), patch.object(updater, "_spawn_detached") as spawn:
                manager = updater.UpdateManager()
                manager._state["latest_version"] = "2.0.0"
                await manager._download_and_schedule()
            self.assertEqual(worker.read_json(project / ".tdm-update-lock")["token"], "owner")
            spawn.assert_not_called()

    async def test_check_does_not_overwrite_active_installation(self):
        manager = updater.UpdateManager()
        manager._state["state"] = "validating"
        manager._apply_task = asyncio.create_task(asyncio.Event().wait())
        try:
            with patch.object(manager, "_check_locked", AsyncMock()) as check:
                result = await manager.check(force=True)
            self.assertEqual(result["state"], "validating")
            check.assert_not_awaited()
        finally:
            manager._apply_task.cancel()
            await asyncio.gather(manager._apply_task, return_exceptions=True)
