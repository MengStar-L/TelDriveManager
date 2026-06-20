import copy
import unittest
from typing import Any, cast

from app import config as config_module
from app.modules.pikpak import account_pool as account_pool_module
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
        self.assertEqual(finished["result_payload"]["errors"][0]["link"], "bad")
        self.assertEqual(recorded_errors[0][0], "b")
        self.assertTrue(any(msg.get("type") == "task_error" for msg in broadcasts))


if __name__ == "__main__":
    unittest.main()
