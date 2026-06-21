import asyncio
import copy
from datetime import datetime, timedelta, timezone
import unittest
from typing import Any, cast

from app import config as config_module
from app.modules.pikpak import account_pool as account_pool_module
from app.modules.pikpak import routes as pikpak_routes
from app.modules.pikpak.account_pool import PikPakAccountPool
from app.modules.pikpak.scheduler import MagnetParseScheduler


class PikPakConfigMigrationTests(unittest.TestCase):
    def normalize(self, raw_pikpak):
        merged = copy.deepcopy(config_module.DEFAULTS)
        merged["pikpak"].update(raw_pikpak)
        return config_module._normalize_config(merged, {"pikpak": raw_pikpak})["pikpak"]

    def test_legacy_password_account_migrates_to_accounts_list(self):
        pikpak = self.normalize({
            "login_mode": "password",
            "username": "user@example.test",
            "password": "secret",
        })

        self.assertEqual(len(pikpak["accounts"]), 1)
        account = pikpak["accounts"][0]
        self.assertTrue(account["id"].startswith("legacy-"))
        self.assertEqual(account["login_mode"], "password")
        self.assertEqual(account["username"], "user@example.test")
        self.assertTrue(account["enabled"])

    def test_parse_concurrency_is_clamped(self):
        self.assertEqual(self.normalize({"parse_concurrency": 0})["parse_concurrency"], 1)
        self.assertEqual(self.normalize({"parse_concurrency": 99})["parse_concurrency"], 16)
        self.assertEqual(self.normalize({"parse_concurrency": "bad"})["parse_concurrency"], 1)

    def test_account_runtime_metadata_survives_normalization(self):
        pikpak = self.normalize({
            "accounts": [{
                "id": "account-1",
                "name": "Account 1",
                "login_mode": "password",
                "username": "user@example.test",
                "password": "secret",
                "session": "encoded-token",
                "enabled": True,
                "vip": {"expire": "2030-01-02T03:04:05Z", "is_vip": True},
                "last_login_refresh_at": "2026-06-21T10:00:00+00:00",
            }],
        })

        account = pikpak["accounts"][0]
        self.assertEqual(account["session"], "encoded-token")
        self.assertEqual(account["vip"]["expire"], "2030-01-02T03:04:05Z")
        self.assertEqual(account["last_login_refresh_at"], "2026-06-21T10:00:00+00:00")


class PikPakAccountPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_robin_skips_disabled_accounts(self):
        pool = PikPakAccountPool()
        cfg = {
            "pikpak": {
                "save_dir": "/",
                "accounts": [
                    {"id": "a", "name": "A", "login_mode": "password", "username": "a", "password": "p", "enabled": True},
                    {"id": "b", "name": "B", "login_mode": "password", "username": "b", "password": "p", "enabled": False},
                    {"id": "c", "name": "C", "login_mode": "password", "username": "c", "password": "p", "enabled": True},
                ],
            }
        }

        original_load_config = account_pool_module.load_config
        try:
            account_pool_module.load_config = cast(Any, lambda force_reload=False: cfg)

            async def fake_client(account):
                return {"client_for": account["id"]}

            pool._get_or_create_client = cast(Any, fake_client)

            sequence = []
            for _ in range(4):
                account, client = await pool.next_client()
                sequence.append((account.id, client["client_for"]))
        finally:
            account_pool_module.load_config = original_load_config

        self.assertEqual(sequence, [("a", "a"), ("c", "c"), ("a", "a"), ("c", "c")])


class MagnetParseSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_failure_completes_job_and_records_error(self):
        class FakePool:
            def __init__(self):
                self.index = 0
                self.accounts = [
                    account_pool_module.PikPakAccountContext("a", "A", "password", "a"),
                    account_pool_module.PikPakAccountContext("b", "B", "password", "b"),
                ]

            async def next_client(self):
                account = self.accounts[self.index % len(self.accounts)]
                self.index += 1
                return account, {"account": account.id}

        scheduler = MagnetParseScheduler(cast(Any, FakePool()))
        broadcasts = []
        finished = {}
        recorded_errors = []

        async def parse_one(client, magnet, index, total, job_id, *args, account=None, **kwargs):
            if magnet == "bad":
                raise RuntimeError("broken link")
            return {
                "file_id": f"root-{index}",
                "file_name": f"file-{index}",
                "account_id": account.id,
                "files": [{"file_id": f"f-{index}", "name": f"file-{index}", "path": f"file-{index}", "size": 1}],
            }

        async def broadcast(msg):
            broadcasts.append(msg)

        async def finish_job(job_id, status, **kwargs):
            finished.update({"job_id": job_id, "status": status, **kwargs})
            return finished

        original_load_config = account_pool_module.load_config
        original_scheduler_load_config = __import__("app.modules.pikpak.scheduler", fromlist=["load_config"]).load_config
        original_add_error = __import__("app.modules.pikpak.scheduler", fromlist=["db"]).db.add_pikpak_account_error
        try:
            scheduler_module = __import__("app.modules.pikpak.scheduler", fromlist=["load_config", "db"])
            scheduler_module.load_config = lambda: {"pikpak": {"parse_concurrency": 2, "poll_interval": 0, "max_wait_time": 1, "magnet_parse_timeout": 1}}

            async def fake_add_error(account_id, job_id, link, stage, message):
                recorded_errors.append((account_id, job_id, link, stage, message))
                return {}

            scheduler_module.db.add_pikpak_account_error = fake_add_error
            await scheduler.run(
                "job-1",
                ["good", "bad"],
                parse_one=parse_one,
                broadcast=broadcast,
                finish_job=finish_job,
                sort_files=lambda files: files,
            )
        finally:
            scheduler_module = __import__("app.modules.pikpak.scheduler", fromlist=["load_config", "db"])
            scheduler_module.load_config = original_scheduler_load_config
            scheduler_module.db.add_pikpak_account_error = original_add_error
            account_pool_module.load_config = original_load_config

        self.assertEqual(finished["status"], "completed")
        self.assertEqual(len(finished["result_payload"]["files"]), 1)
        self.assertEqual(finished["result_payload"]["parse_concurrency"], 2)
        self.assertEqual(finished["result_payload"]["total"], 2)
        self.assertEqual(finished["result_payload"]["errors"][0]["link"], "bad")
        self.assertEqual(recorded_errors[0][0], "b")
        self.assertTrue(any(msg.get("type") == "task_error" for msg in broadcasts))

    async def test_all_failures_keep_failed_links_in_result_payload(self):
        class FakePool:
            async def next_client(self):
                account = account_pool_module.PikPakAccountContext("a", "A", "password", "a")
                return account, {"account": account.id}

        scheduler = MagnetParseScheduler(cast(Any, FakePool()))
        finished = {}

        async def parse_one(*args, **kwargs):
            raise RuntimeError("broken link")

        async def broadcast(_msg):
            return None

        async def finish_job(job_id, status, **kwargs):
            finished.update({"job_id": job_id, "status": status, **kwargs})
            return finished

        scheduler_module = __import__("app.modules.pikpak.scheduler", fromlist=["load_config", "db"])
        original_scheduler_load_config = scheduler_module.load_config
        original_add_error = scheduler_module.db.add_pikpak_account_error
        try:
            scheduler_module.load_config = lambda: {"pikpak": {"parse_concurrency": 2, "poll_interval": 0, "max_wait_time": 1, "magnet_parse_timeout": 1}}

            async def fake_add_error(*_args, **_kwargs):
                return {}

            scheduler_module.db.add_pikpak_account_error = fake_add_error
            await scheduler.run(
                "job-failed",
                ["bad-1", "bad-2"],
                parse_one=parse_one,
                broadcast=broadcast,
                finish_job=finish_job,
                sort_files=lambda files: files,
            )
        finally:
            scheduler_module.load_config = original_scheduler_load_config
            scheduler_module.db.add_pikpak_account_error = original_add_error

        self.assertEqual(finished["status"], "failed")
        self.assertEqual(finished["result_payload"]["total"], 2)
        self.assertEqual([item["link"] for item in finished["result_payload"]["errors"]], ["bad-1", "bad-2"])


class PikPakShareLinkTests(unittest.TestCase):
    def test_clean_pikpak_share_link_strips_query_and_fragment(self):
        self.assertEqual(
            pikpak_routes._clean_pikpak_share_link("https://mypikpak.com/s/abc?act=play"),
            "https://mypikpak.com/s/abc",
        )
        self.assertEqual(
            pikpak_routes._clean_pikpak_share_link("https://mypikpak.com/s/abc?act=play&x=1#frag"),
            "https://mypikpak.com/s/abc",
        )
        self.assertEqual(
            pikpak_routes._clean_pikpak_share_link(" https://mypikpak.com/s/abc "),
            "https://mypikpak.com/s/abc",
        )


class ParseJobStateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_active_parse_job_id = pikpak_routes._active_parse_job_id
        pikpak_routes._active_parse_job_id = None

    async def asyncTearDown(self):
        pikpak_routes._active_parse_job_id = self.original_active_parse_job_id

    async def test_share_list_uses_cleaned_share_link_for_job_and_worker(self):
        captured = {}

        class FakeRequest:
            async def json(self):
                return {
                    "share_link": "https://mypikpak.com/s/VOvb6etR01ViAz9VVKGev6pXo2?act=play",
                    "pass_code": "1234",
                }

        original_create = pikpak_routes._create_parse_job
        original_run = pikpak_routes._run_share_parse_job
        original_background = pikpak_routes._create_parse_background_task
        try:
            async def fake_create(job_type, request_payload):
                captured["create"] = (job_type, request_payload)
                return {
                    "job_id": "share-job",
                    "job_type": job_type,
                    "status": "running",
                    "request_payload": request_payload,
                }, None

            def fake_run(job_id, share_link, pass_code):
                captured["run"] = (job_id, share_link, pass_code)
                return "fake-coro"

            def fake_background(coro, job_id):
                captured["background"] = (coro, job_id)
                return None

            pikpak_routes._create_parse_job = fake_create
            pikpak_routes._run_share_parse_job = fake_run
            pikpak_routes._create_parse_background_task = fake_background

            response = await pikpak_routes.api_share_list(FakeRequest())
        finally:
            pikpak_routes._create_parse_job = original_create
            pikpak_routes._run_share_parse_job = original_run
            pikpak_routes._create_parse_background_task = original_background

        cleaned = "https://mypikpak.com/s/VOvb6etR01ViAz9VVKGev6pXo2"
        self.assertEqual(response.status_code, 202)
        self.assertEqual(captured["create"], ("share", {"share_link": cleaned, "pass_code": "1234"}))
        self.assertEqual(captured["run"], ("share-job", cleaned, "1234"))
        self.assertEqual(captured["background"], ("fake-coro", "share-job"))

    async def test_create_parse_job_returns_existing_active_job(self):
        active = {
            "job_id": "active-1",
            "job_type": "magnet",
            "status": "running",
            "request_payload": {"magnets": ["magnet:?xt=1"], "total": 1, "parse_concurrency": 1},
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        created = []

        original_get_active = pikpak_routes.db.get_active_parse_job
        original_create = pikpak_routes.db.create_parse_job
        original_broadcast = pikpak_routes._broadcast_parse_job_state
        try:
            async def fake_get_active():
                return active

            async def fake_create(*args, **kwargs):
                created.append((args, kwargs))
                return {"job_id": "new"}

            async def fake_broadcast(_job):
                return None

            pikpak_routes.db.get_active_parse_job = fake_get_active
            pikpak_routes.db.create_parse_job = fake_create
            pikpak_routes._broadcast_parse_job_state = fake_broadcast

            job, active_job = await pikpak_routes._create_parse_job("magnet", {"magnets": ["new"]})
        finally:
            pikpak_routes.db.get_active_parse_job = original_get_active
            pikpak_routes.db.create_parse_job = original_create
            pikpak_routes._broadcast_parse_job_state = original_broadcast

        self.assertIsNone(job)
        self.assertEqual(active_job, active)
        self.assertEqual(created, [])

    async def test_finish_parse_job_clears_active_pointer(self):
        finished = {}
        broadcasts = []

        original_update = pikpak_routes.db.update_parse_job
        original_broadcast = pikpak_routes._broadcast_parse_job_state
        try:
            async def fake_update(job_id, **kwargs):
                finished.update({"job_id": job_id, **kwargs})
                return {"job_id": job_id, "job_type": "magnet", **kwargs}

            async def fake_broadcast(job):
                broadcasts.append(job)

            pikpak_routes.db.update_parse_job = fake_update
            pikpak_routes._broadcast_parse_job_state = fake_broadcast
            pikpak_routes._active_parse_job_id = "job-1"

            await pikpak_routes._finish_parse_job("job-1", "completed", result_payload={"files": []})
        finally:
            pikpak_routes.db.update_parse_job = original_update
            pikpak_routes._broadcast_parse_job_state = original_broadcast

        self.assertIsNone(pikpak_routes._active_parse_job_id)
        self.assertEqual(finished["status"], "completed")
        self.assertEqual(broadcasts[0]["status"], "completed")

    async def test_stale_active_parse_job_is_marked_failed(self):
        stale = {
            "job_id": "stale-1",
            "job_type": "magnet",
            "status": "running",
            "request_payload": {"magnets": ["m"], "total": 1, "parse_concurrency": 1},
            "updated_at": (datetime.now(timezone.utc) - timedelta(seconds=125)).isoformat(),
        }
        active_results = [stale, None]
        updates = []

        original_get_active = pikpak_routes.db.get_active_parse_job
        original_update = pikpak_routes.db.update_parse_job
        original_broadcast = pikpak_routes._broadcast_parse_job_state
        original_load_config = pikpak_routes.load_config
        try:
            async def fake_get_active():
                return active_results.pop(0)

            async def fake_update(job_id, **kwargs):
                updates.append({"job_id": job_id, **kwargs})
                return {**stale, **kwargs}

            async def fake_broadcast(_job):
                return None

            pikpak_routes.db.get_active_parse_job = fake_get_active
            pikpak_routes.db.update_parse_job = fake_update
            pikpak_routes._broadcast_parse_job_state = fake_broadcast
            pikpak_routes.load_config = lambda: {"pikpak": {"max_wait_time": 60, "parse_concurrency": 1}}

            active_job = await pikpak_routes._get_active_parse_job()
        finally:
            pikpak_routes.db.get_active_parse_job = original_get_active
            pikpak_routes.db.update_parse_job = original_update
            pikpak_routes._broadcast_parse_job_state = original_broadcast
            pikpak_routes.load_config = original_load_config

        self.assertIsNone(active_job)
        self.assertEqual(updates[0]["job_id"], "stale-1")
        self.assertEqual(updates[0]["status"], "failed")

    async def test_crashed_background_task_marks_active_job_failed(self):
        updates = []
        active = {"job_id": "crash-1", "job_type": "rss", "status": "running"}

        async def boom():
            raise RuntimeError("boom")

        original_get = pikpak_routes.db.get_parse_job
        original_update = pikpak_routes.db.update_parse_job
        original_broadcast = pikpak_routes._broadcast_parse_job_state
        try:
            async def fake_get(_job_id):
                return active

            async def fake_update(job_id, **kwargs):
                updates.append({"job_id": job_id, **kwargs})
                return {**active, **kwargs}

            async def fake_broadcast(_job):
                return None

            pikpak_routes.db.get_parse_job = fake_get
            pikpak_routes.db.update_parse_job = fake_update
            pikpak_routes._broadcast_parse_job_state = fake_broadcast

            task = pikpak_routes._create_parse_background_task(boom(), "crash-1")
            with self.assertRaises(RuntimeError):
                await task
            for _ in range(3):
                await asyncio.sleep(0)
        finally:
            pikpak_routes.db.get_parse_job = original_get
            pikpak_routes.db.update_parse_job = original_update
            pikpak_routes._broadcast_parse_job_state = original_broadcast

        self.assertEqual(updates[0]["job_id"], "crash-1")
        self.assertEqual(updates[0]["status"], "failed")
        self.assertIn("boom", updates[0]["error"])


if __name__ == "__main__":
    unittest.main()
