import copy
import tempfile
import unittest
from pathlib import Path

from app import config
from app.modules.tel2teldrive import service


def normalize(raw: dict) -> dict:
    return config._normalize_config(config._deep_merge(config.DEFAULTS, raw), raw)


class GlobalChannelConfigTests(unittest.TestCase):
    def test_migrates_legacy_telegram_channel(self):
        normalized = normalize({"telegram": {"channel_id": -1003854656012}})

        self.assertEqual(normalized["teldrive"]["channel_id"], -1003854656012)
        self.assertNotIn("channel_id", normalized["telegram"])
        self.assertFalse(normalized["_meta"]["telegram_channel_conflict"])

    def test_accepts_equivalent_channel_forms(self):
        normalized = normalize(
            {
                "telegram": {"channel_id": -1003854656012},
                "teldrive": {"channel_id": 3854656012},
            }
        )

        self.assertEqual(normalized["teldrive"]["channel_id"], 3854656012)
        self.assertFalse(normalized["_meta"]["telegram_channel_conflict"])

    def test_marks_conflicting_legacy_channels(self):
        normalized = normalize(
            {
                "telegram": {"channel_id": -1003854656012},
                "teldrive": {"channel_id": -1003819048300},
            }
        )

        self.assertEqual(normalized["teldrive"]["channel_id"], -1003819048300)
        self.assertTrue(normalized["_meta"]["telegram_channel_conflict"])
        self.assertEqual(normalized["_meta"]["legacy_telegram_channel_id"], -1003854656012)

    def test_partial_normalization_preserves_conflict_until_channel_saved(self):
        current = normalize(
            {
                "telegram": {"channel_id": -1003854656012},
                "teldrive": {"channel_id": -1003819048300},
            }
        )
        merged = config._deep_merge(current, {"upload": {"max_retries": 5}})

        still_conflicted = config._normalize_config(merged, {"upload": {"max_retries": 5}})
        resolved = config._normalize_config(
            config._deep_merge(still_conflicted, {"teldrive": {"channel_id": -1003819048300}}),
            {"teldrive": {"channel_id": -1003819048300}},
        )

        self.assertTrue(still_conflicted["_meta"]["telegram_channel_conflict"])
        self.assertFalse(resolved["_meta"]["telegram_channel_conflict"])


class ServiceChannelConfigTests(unittest.TestCase):
    def make_store(self, directory: str) -> service.ConfigStore:
        return service.ConfigStore(Path(directory) / "config.toml")

    def test_uses_teldrive_channel_for_telegram_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory)
            runtime = store.runtime_from_payload(
                {
                    "telegram": {"api_id": 1, "api_hash": "hash"},
                    "teldrive": {
                        "api_host": "http://teldrive",
                        "access_token": "token",
                        "channel_id": -1003854656012,
                    },
                }
            )

        self.assertEqual(runtime.telegram_channel_id, -1003854656012)
        self.assertEqual(runtime.teldrive_channel_id, -1003854656012)
        self.assertFalse(runtime.telegram_channel_conflict)
        self.assertTrue(runtime.telegram_deletion_enabled)

    def test_conflict_disables_only_telegram_deletion(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory)
            runtime = store.runtime_from_payload(
                {
                    "telegram": {
                        "api_id": 1,
                        "api_hash": "hash",
                        "channel_id": -1003854656012,
                    },
                    "teldrive": {
                        "api_host": "http://teldrive",
                        "access_token": "token",
                        "channel_id": -1003819048300,
                    },
                }
            )

        self.assertEqual(runtime.telegram_channel_id, -1003819048300)
        self.assertTrue(runtime.telegram_channel_conflict)
        self.assertFalse(runtime.telegram_deletion_enabled)
        self.assertTrue(runtime.is_ready)

    def test_dump_omits_legacy_telegram_channel(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self.make_store(directory)
            data = store._normalize(
                {
                    "telegram": {
                        "api_id": 1,
                        "api_hash": "hash",
                        "channel_id": -1003854656012,
                    },
                    "teldrive": {
                        "api_host": "http://teldrive",
                        "access_token": "token",
                        "channel_id": 3854656012,
                    },
                }
            )
            dumped = store._dump_toml(copy.deepcopy(data))
        telegram_section = dumped.split("[telegram]", 1)[1].split("[telegram_relay]", 1)[0]

        self.assertNotIn("channel_id", telegram_section)
        self.assertIn("channel_id = 3854656012", dumped)


if __name__ == "__main__":
    unittest.main()
