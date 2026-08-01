import asyncio
import unittest
from typing import cast
from unittest.mock import AsyncMock

from app.modules.pikpak.client import PikPakClient
from app.modules.pikpak import routes as pikpak_routes


def build_client(raw_client) -> PikPakClient:
    client = cast(PikPakClient, object.__new__(PikPakClient))
    client.client = raw_client
    client.save_dir = "/"
    client._save_dir_id = None
    return client


class IsolatedShareRestoreClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_restore_targets_owned_scope_and_ignores_response_file_id(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.restore_calls = []
                self.deleted = []

            async def create_folder(self, name, parent_id):
                self.folder_call = (name, parent_id)
                return {"file": {"id": "owned-scope"}}

            async def _request_post(self, url, data):
                self.restore_calls.append((url, data))
                return {
                    "file_id": "reused-unrelated-folder",
                    "restore_status": "RESTORE_START",
                }

            async def delete_forever(self, ids):
                self.deleted.append(ids)

        raw = RawClient()
        client = build_client(raw)

        scope_id = await client.start_isolated_share_restore(
            "share-1", ["selected-1"], "pass-token"
        )

        self.assertEqual(scope_id, "owned-scope")
        self.assertEqual(raw.folder_call[1], None)
        self.assertTrue(raw.folder_call[0].startswith(".teldrive-share-"))
        payload = raw.restore_calls[0][1]
        self.assertEqual(
            payload,
            {
                "share_id": "share-1",
                "pass_code_token": "pass-token",
                "file_ids": ["selected-1"],
                "to": {"parent_id": "owned-scope"},
            },
        )
        self.assertNotIn("to_parent_id", payload)
        self.assertEqual(raw.deleted, [])

    async def test_restore_failure_cleans_only_the_owned_scope(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.deleted = []

            async def create_folder(self, name, parent_id):
                return {"id": "owned-scope"}

            async def _request_post(self, url, data):
                raise RuntimeError("restore rejected")

            async def delete_forever(self, ids):
                self.deleted.append(ids)

        raw = RawClient()
        client = build_client(raw)

        with self.assertRaisesRegex(RuntimeError, "restore rejected"):
            await client.start_isolated_share_restore(
                "share-1", ["selected-1"], "pass-token"
            )

        self.assertEqual(raw.deleted, [["owned-scope"]])

    async def test_restore_error_payload_cleans_only_the_owned_scope(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.deleted = []

            async def create_folder(self, name, parent_id):
                return {"file": {"id": "owned-scope"}}

            async def _request_post(self, url, data):
                return {"error": {"message": "restore denied"}}

            async def delete_forever(self, ids):
                self.deleted.append(ids)

        raw = RawClient()
        client = build_client(raw)

        with self.assertRaisesRegex(RuntimeError, "restore denied"):
            await client.start_isolated_share_restore(
                "share-1", ["selected-1"], "pass-token"
            )

        self.assertEqual(raw.deleted, [["owned-scope"]])

    async def test_missing_scope_id_stops_before_restore(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.restore_called = False

            async def create_folder(self, name, parent_id):
                return {}

            async def _request_post(self, url, data):
                self.restore_called = True

        raw = RawClient()
        client = build_client(raw)

        with self.assertRaisesRegex(RuntimeError, "未返回隔离目录 ID"):
            await client.start_isolated_share_restore(
                "share-1", ["selected-1"], "pass-token"
            )

        self.assertFalse(raw.restore_called)

    async def test_scope_polling_waits_for_exact_count_without_scope_prefix(self):
        client = build_client(object())
        selected = {
            "name": "paid_8k.mp4",
            "url": "https://download/selected",
            "file_id": "restored-selected",
            "path": "Movie/paid_8k.mp4",
            "size": 100,
        }
        client._list_folder_files = AsyncMock(side_effect=[[], [selected]])

        files = await client.wait_for_isolated_share_urls(
            "owned-scope", expected_count=1, timeout=0.2, poll_interval=0.01
        )

        self.assertEqual(files, [selected])
        self.assertEqual(
            client._list_folder_files.await_args_list[0].args,
            ("owned-scope",),
        )
        self.assertEqual(
            client._list_folder_files.await_args_list[0].kwargs,
            {"prefix": ""},
        )

    async def test_scope_polling_retries_transient_listing_error(self):
        client = build_client(object())
        selected = {
            "name": "paid_8k.mp4",
            "url": "https://download/selected",
            "file_id": "restored-selected",
            "path": "Movie/paid_8k.mp4",
            "size": 100,
        }
        client._list_folder_files = AsyncMock(
            side_effect=[RuntimeError("temporary API failure"), [selected]]
        )

        files = await client.wait_for_isolated_share_urls(
            "owned-scope", expected_count=1, timeout=0.2, poll_interval=0.01
        )

        self.assertEqual(files, [selected])
        self.assertEqual(client._list_folder_files.await_count, 2)

    async def test_scope_polling_rejects_extra_files_instead_of_filtering(self):
        client = build_client(object())
        client._list_folder_files = AsyncMock(
            return_value=[
                {
                    "name": "SAVR-1127-1.mp4",
                    "url": "https://download/old-1",
                },
                {
                    "name": "SAVR-1127-2.mp4",
                    "url": "https://download/old-2",
                },
                {
                    "name": "paid_8k.mp4",
                    "url": "https://download/selected",
                },
            ]
        )

        with self.assertRaisesRegex(
            RuntimeError, "隔离目录出现 3 个文件.*勾选 1 个"
        ):
            await client.wait_for_isolated_share_urls(
                "owned-scope", expected_count=1, timeout=0.2, poll_interval=0.01
            )

    async def test_scope_polling_rejects_partial_result_after_timeout(self):
        client = build_client(object())
        client._list_folder_files = AsyncMock(
            return_value=[
                {"name": "one.mp4", "url": "https://download/one"},
            ]
        )

        with self.assertRaisesRegex(
            RuntimeError, "应有 2 个文件，实际就绪 1 个"
        ):
            await client.wait_for_isolated_share_urls(
                "owned-scope", expected_count=2, timeout=0.02, poll_interval=0.01
            )


class ShareSelectionContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_share_download_endpoint_rejects_missing_selected_paths(self):
        class Request:
            async def json(self):
                return {
                    "share_id": "share-1",
                    "file_ids": ["selected-1"],
                    "pass_code_token": "pass-token",
                    "file_paths": {},
                }

        original_register = pikpak_routes._register_share_download_job
        original_create_task = pikpak_routes.asyncio.create_task
        try:
            async def fake_register(_job_key):
                return True

            def close_background(coro):
                coro.close()
                return None

            pikpak_routes._register_share_download_job = fake_register
            pikpak_routes.asyncio.create_task = close_background
            response = await pikpak_routes.api_share_download(Request())
        finally:
            pikpak_routes._register_share_download_job = original_register
            pikpak_routes.asyncio.create_task = original_create_task

        self.assertEqual(response.status_code, 400)
        self.assertIn("文件路径元数据不完整", response.body.decode("utf-8"))

    async def test_isolated_result_mismatch_fails_instead_of_filtering(self):
        files = [
            {
                "name": "SAVR-1127-1.mp4",
                "path": "SAVR-1127/SAVR-1127-1.mp4",
                "url": "https://download/old",
            }
        ]

        with self.assertRaisesRegex(
            RuntimeError, "隔离解析结果与勾选文件不一致"
        ):
            pikpak_routes._bind_selected_share_paths(
                files,
                ["selected-1"],
                {"selected-1": "Movie/paid_8k.mp4"},
            )

    async def test_selected_paths_replace_restore_scope_paths(self):
        files = [
            {
                "name": "paid_8k.mp4",
                "path": "temporary-wrapper/paid_8k.mp4",
                "url": "https://download/selected",
            }
        ]

        result = pikpak_routes._bind_selected_share_paths(
            files,
            ["selected-1"],
            {"selected-1": "Movie/paid_8k.mp4"},
        )

        self.assertEqual(result[0]["path"], "Movie/paid_8k.mp4")


class ShareDownloadIsolationRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.originals = {
            name: getattr(pikpak_routes, name)
            for name in (
                "_next_pikpak_client",
                "load_config",
                "_broadcast",
                "_broadcast_resolved_files",
                "_broadcast_link_pushed",
                "_ensure_aria2_client",
                "_register_aria2_task",
            )
        }
        self.original_add_error = pikpak_routes.db.add_pikpak_account_error
        self.original_defer = (
            pikpak_routes.task_manager.should_defer_new_downloads
        )
        self.original_hold = pikpak_routes.task_manager.hold_gids_for_disk_gate
        self.original_enqueue = pikpak_routes.task_manager.enqueue_serial_task

    async def asyncTearDown(self):
        for name, value in self.originals.items():
            setattr(pikpak_routes, name, value)
        pikpak_routes.db.add_pikpak_account_error = self.original_add_error
        pikpak_routes.task_manager.should_defer_new_downloads = self.original_defer
        pikpak_routes.task_manager.hold_gids_for_disk_gate = self.original_hold
        pikpak_routes.task_manager.enqueue_serial_task = self.original_enqueue

    def install_common_fakes(self, pikpak, aria2, *, serial_mode=False):
        account = pikpak_routes.PikPakAccountContext(
            "account-1", "A", "token"
        )

        async def next_client():
            return account, pikpak

        async def no_op(*args, **kwargs):
            return None

        pikpak_routes._next_pikpak_client = next_client
        pikpak_routes.load_config = lambda: {
            "pikpak": {
                "share_parse_timeout": 5,
                "share_download_url_timeout": 5,
                "share_download_url_poll_interval": 0.01,
            },
            "aria2": {"download_dir": "C:/downloads"},
            "upload": {"serial_transfer_mode": serial_mode},
            "teldrive": {"target_path": "/"},
        }
        pikpak_routes._broadcast = no_op
        pikpak_routes._broadcast_resolved_files = no_op
        pikpak_routes._broadcast_link_pushed = no_op
        pikpak_routes._ensure_aria2_client = lambda: asyncio.sleep(
            0, result=aria2
        )
        pikpak_routes._register_aria2_task = no_op
        pikpak_routes.db.add_pikpak_account_error = no_op
        pikpak_routes.task_manager.should_defer_new_downloads = lambda: False
        pikpak_routes.task_manager.hold_gids_for_disk_gate = lambda gids: None

    async def test_one_selected_file_pushes_one_isolated_url(self):
        class PikPak:
            def __init__(self):
                self.deleted = []

            async def start_isolated_share_restore(
                self, share_id, file_ids, token
            ):
                self.start_call = (share_id, file_ids, token)
                return "owned-scope"

            async def wait_for_isolated_share_urls(
                self, scope_id, expected_count, **kwargs
            ):
                self.wait_call = (scope_id, expected_count)
                return [
                    {
                        "name": "paid_8k.mp4",
                        "path": "temporary/paid_8k.mp4",
                        "url": "https://download/selected",
                        "size": 100,
                    }
                ]

            async def delete_files(self, ids):
                self.deleted.append(ids)

        class Aria2:
            def __init__(self):
                self.added = []

            async def add_uri(self, url, options):
                self.added.append((url, options))
                return "gid-selected"

        pikpak = PikPak()
        aria2 = Aria2()
        self.install_common_fakes(pikpak, aria2)

        await pikpak_routes._process_share_download(
            "share-1",
            ["selected-1"],
            "pass-token",
            file_paths={"selected-1": "Movie/paid_8k.mp4"},
        )

        self.assertEqual(pikpak.start_call[1], ["selected-1"])
        self.assertEqual(pikpak.wait_call, ("owned-scope", 1))
        self.assertEqual(
            [item[0] for item in aria2.added],
            ["https://download/selected"],
        )
        self.assertEqual(aria2.added[0][1]["dir"], "C:/downloads")
        self.assertEqual(pikpak.deleted, [["owned-scope"]])

    async def test_serial_mode_enqueues_only_the_isolated_url(self):
        class PikPak:
            def __init__(self):
                self.deleted = []

            async def start_isolated_share_restore(
                self, share_id, file_ids, token
            ):
                return "owned-scope"

            async def wait_for_isolated_share_urls(
                self, scope_id, expected_count, **kwargs
            ):
                return [
                    {
                        "name": "paid_8k.mp4",
                        "path": "temporary/paid_8k.mp4",
                        "url": "https://download/selected",
                        "size": 100,
                    }
                ]

            async def delete_files(self, ids):
                self.deleted.append(ids)

        class Aria2:
            def __init__(self):
                self.added = []

            async def add_uri(self, url, options):
                self.added.append((url, options))
                return "unexpected-gid"

        pikpak = PikPak()
        aria2 = Aria2()
        queued = []
        self.install_common_fakes(pikpak, aria2, serial_mode=True)

        async def enqueue(url, filename, **kwargs):
            queued.append((url, filename, kwargs))
            return {"task_id": "queued-1"}

        pikpak_routes.task_manager.enqueue_serial_task = enqueue

        await pikpak_routes._process_share_download(
            "share-1",
            ["selected-1"],
            "pass-token",
            file_paths={"selected-1": "Movie/paid_8k.mp4"},
        )

        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0][0], "https://download/selected")
        self.assertEqual(aria2.added, [])
        self.assertEqual(pikpak.deleted, [["owned-scope"]])

    async def test_isolation_failure_pushes_nothing_and_cleans_scope(self):
        class PikPak:
            def __init__(self):
                self.deleted = []

            async def start_isolated_share_restore(self, *args):
                return "owned-scope"

            async def wait_for_isolated_share_urls(self, *args, **kwargs):
                raise RuntimeError(
                    "隔离目录出现 3 个文件，但只勾选 1 个"
                )

            async def delete_files(self, ids):
                self.deleted.append(ids)

        class Aria2:
            def __init__(self):
                self.added = []

            async def add_uri(self, url, options):
                self.added.append((url, options))
                return "unexpected-gid"

        pikpak = PikPak()
        aria2 = Aria2()
        self.install_common_fakes(pikpak, aria2)

        await pikpak_routes._process_share_download(
            "share-1",
            ["selected-1"],
            "pass-token",
            file_paths={"selected-1": "Movie/paid_8k.mp4"},
        )

        self.assertEqual(aria2.added, [])
        self.assertEqual(pikpak.deleted, [["owned-scope"]])


if __name__ == "__main__":
    unittest.main()
