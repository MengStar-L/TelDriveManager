import asyncio
import json
import unittest
from typing import cast
from unittest.mock import AsyncMock

from app.modules.pikpak.client import PikPakClient, ShareRestoreReceipt
from app.modules.pikpak import routes as pikpak_routes


def build_client(raw_client) -> PikPakClient:
    client = cast(PikPakClient, object.__new__(PikPakClient))
    client.client = raw_client
    client.save_dir = "/"
    client._save_dir_id = None
    return client


class ShareListScopeTests(unittest.IsolatedAsyncioTestCase):
    async def test_deep_link_requests_explicit_target_and_preserves_source_metadata(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.calls = []

            async def _request_get(self, url, params=None):
                self.calls.append((url, params))
                return {
                    "pass_code_token": "pass-token",
                    "files": [
                        {
                            "id": "source-1",
                            "parent_id": "target-folder",
                            "name": "original.mkv",
                            "kind": "drive#file",
                            "size": "100",
                            "mime_type": "video/x-matroska",
                        }
                    ],
                }

        raw = RawClient()
        client = build_client(raw)
        result = await client.get_share_file_list(
            "https://mypikpak.com/s/share-1/target-folder"
        )

        self.assertEqual(raw.calls[0][1]["share_id"], "share-1")
        self.assertEqual(raw.calls[0][1]["parent_id"], "target-folder")
        self.assertEqual(result["target_id"], "target-folder")
        self.assertEqual(result["files"][0]["source_file_id"], "source-1")
        self.assertEqual(result["files"][0]["source_name"], "original.mkv")
        self.assertEqual(result["files"][0]["source_path"], "original.mkv")

    async def test_deep_link_rejects_share_root_fallback(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            async def _request_get(self, url, params=None):
                return {
                    "pass_code_token": "pass-token",
                    "files": [
                        {
                            "id": "unrelated-folder",
                            "parent_id": "",
                            "name": "Unrelated",
                            "kind": "drive#folder",
                        }
                    ],
                }

        with self.assertRaisesRegex(RuntimeError, "链接目标节点不一致"):
            await build_client(RawClient()).get_share_file_list(
                "https://mypikpak.com/s/share-1/target-folder"
            )


class IsolatedShareRestoreClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_restore_uses_explicit_target_protocol_and_returns_receipt(self):
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
                    "file_id": "owned-scope",
                    "restore_status": "RESTORE_START",
                    "restore_task_id": "restore-task",
                }

            async def delete_forever(self, ids):
                self.deleted.append(ids)

        raw = RawClient()
        client = build_client(raw)

        receipt = await client.start_isolated_share_restore(
            "share-1", ["selected-1", "selected-2"], "pass-token"
        )

        self.assertEqual(receipt.scope_id, "owned-scope")
        self.assertEqual(receipt.task_id, "restore-task")
        self.assertEqual(receipt.selected_ids, ("selected-1", "selected-2"))
        self.assertEqual(raw.folder_call[1], None)
        self.assertTrue(raw.folder_call[0].startswith(".teldrive-share-"))
        payload = raw.restore_calls[0][1]
        self.assertEqual(
            payload,
            {
                "parent_id": "owned-scope",
                "share_id": "share-1",
                "pass_code_token": "pass-token",
                "file_ids": ["selected-1", "selected-2"],
                "ancestor_ids": [],
                "specify_parent_id": True,
                "params": {"trace_file_ids": "selected-1,selected-2"},
            },
        )
        self.assertNotIn("to", payload)
        self.assertNotIn("to_parent_id", payload)
        self.assertEqual(raw.deleted, [])

    async def test_restore_rejects_response_target_outside_owned_scope(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.deleted = []

            async def create_folder(self, name, parent_id):
                return {"file": {"id": "owned-scope"}}

            async def _request_post(self, url, data):
                return {
                    "file_id": "Pack From Shared",
                    "restore_status": "RESTORE_START",
                    "restore_task_id": "restore-task",
                }

            async def delete_forever(self, ids):
                self.deleted.append(ids)

        raw = RawClient()
        client = build_client(raw)

        with self.assertRaisesRegex(RuntimeError, "恢复目标目录不一致"):
            await client.start_isolated_share_restore(
                "share-1", ["selected-1"], "pass-token"
            )

        self.assertEqual(raw.deleted, [["owned-scope"]])

    async def test_restore_rejects_missing_task_id(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            def __init__(self):
                self.deleted = []

            async def create_folder(self, name, parent_id):
                return {"file": {"id": "owned-scope"}}

            async def _request_post(self, url, data):
                return {
                    "file_id": "owned-scope",
                    "restore_status": "RESTORE_START",
                }

            async def delete_forever(self, ids):
                self.deleted.append(ids)

        raw = RawClient()
        client = build_client(raw)

        with self.assertRaisesRegex(RuntimeError, "未返回恢复任务 ID"):
            await client.start_isolated_share_restore(
                "share-1", ["selected-1"], "pass-token"
            )

        self.assertEqual(raw.deleted, [["owned-scope"]])

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

    async def test_restore_task_mapping_resolves_direct_children_in_selected_order(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            async def _request_get(self, url, params=None):
                self.task_call = (url, params)
                return {
                    "phase": "PHASE_TYPE_COMPLETE",
                    "message": "Completed",
                    "params": {
                        "trace_file_ids": json.dumps({
                            "selected-1": "restored-1",
                            "selected-2": "restored-2",
                        })
                    },
                }

            async def file_list(self, parent_id, next_page_token=None):
                self.list_call = (parent_id, next_page_token)
                return {
                    "files": [
                        {
                            "id": "restored-2",
                            "name": "two.mp4",
                            "kind": "drive#file",
                            "parent_id": "owned-scope",
                            "size": "200",
                            "web_content_link": "https://download/two",
                        },
                        {
                            "id": "restored-1",
                            "name": "one.mp4",
                            "kind": "drive#file",
                            "parent_id": "owned-scope",
                            "size": "100",
                            "web_content_link": "https://download/one",
                        },
                    ]
                }

        raw = RawClient()
        client = build_client(raw)
        client._list_folder_files = AsyncMock(
            side_effect=AssertionError("restore scope must not be recursive")
        )
        receipt = ShareRestoreReceipt(
            "owned-scope", "restore-task", ("selected-1", "selected-2")
        )

        files = await client.wait_for_isolated_share_urls(
            receipt, timeout=0.2, poll_interval=0.01
        )

        self.assertEqual(
            files,
            [
                {
                    "source_file_id": "selected-1",
                    "destination_file_id": "restored-1",
                    "name": "one.mp4",
                    "url": "https://download/one",
                    "file_id": "restored-1",
                    "path": "one.mp4",
                    "size": 100,
                },
                {
                    "source_file_id": "selected-2",
                    "destination_file_id": "restored-2",
                    "name": "two.mp4",
                    "url": "https://download/two",
                    "file_id": "restored-2",
                    "path": "two.mp4",
                    "size": 200,
                },
            ],
        )
        self.assertTrue(raw.task_call[0].endswith("/drive/v1/tasks/restore-task"))
        self.assertEqual(raw.list_call, ("owned-scope", None))
        self.assertEqual(client._list_folder_files.await_count, 0)

    async def test_restore_task_terminal_error_fails_before_listing(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            async def _request_get(self, url, params=None):
                return {
                    "phase": "PHASE_TYPE_ERROR",
                    "message": "restore rejected",
                    "params": {"error_detail": "target denied"},
                }

            async def file_list(self, parent_id, next_page_token=None):
                raise AssertionError("failed task must not list the scope")

        client = build_client(RawClient())
        receipt = ShareRestoreReceipt(
            "owned-scope", "restore-task", ("selected-1",)
        )

        with self.assertRaisesRegex(RuntimeError, "target denied"):
            await client.wait_for_isolated_share_urls(
                receipt, timeout=0.2, poll_interval=0.01
            )

    async def test_restore_task_rejects_invalid_or_mismatched_trace_mapping(self):
        cases = [
            ("not-json", "映射格式无效"),
            (json.dumps({}), "源文件映射不一致"),
            (
                json.dumps({
                    "selected-1": "restored-1",
                    "unselected": "restored-extra",
                }),
                "源文件映射不一致",
            ),
            (
                json.dumps({
                    "selected-1": "restored-same",
                    "selected-2": "restored-same",
                }),
                "目标文件映射无效",
            ),
        ]

        for trace_value, error_pattern in cases:
            with self.subTest(trace_value=trace_value):
                class RawClient:
                    PIKPAK_API_HOST = "api-drive.mypikpak.com"

                    async def _request_get(self, url, params=None):
                        return {
                            "phase": "PHASE_TYPE_COMPLETE",
                            "params": {"trace_file_ids": trace_value},
                        }

                    async def file_list(self, parent_id, next_page_token=None):
                        raise AssertionError("invalid mapping must not list scope")

                client = build_client(RawClient())
                receipt = ShareRestoreReceipt(
                    "owned-scope",
                    "restore-task",
                    ("selected-1", "selected-2"),
                )

                with self.assertRaisesRegex(RuntimeError, error_pattern):
                    await client.wait_for_isolated_share_urls(
                        receipt, timeout=0.2, poll_interval=0.01
                    )

    async def test_restore_scope_partial_children_times_out(self):
        class RawClient:
            PIKPAK_API_HOST = "api-drive.mypikpak.com"

            async def _request_get(self, url, params=None):
                return {
                    "phase": "PHASE_TYPE_COMPLETE",
                    "params": {
                        "trace_file_ids": json.dumps({
                            "selected-1": "restored-1",
                            "selected-2": "restored-2",
                        })
                    },
                }

            async def file_list(self, parent_id, next_page_token=None):
                return {
                    "files": [{
                        "id": "restored-1",
                        "name": "one.mp4",
                        "kind": "drive#file",
                        "parent_id": "owned-scope",
                        "size": "100",
                        "web_content_link": "https://download/one",
                    }]
                }

        client = build_client(RawClient())
        receipt = ShareRestoreReceipt(
            "owned-scope", "restore-task", ("selected-1", "selected-2")
        )

        with self.assertRaisesRegex(RuntimeError, "直属文件等待超时"):
            await client.wait_for_isolated_share_urls(
                receipt, timeout=0.02, poll_interval=0.01
            )

    async def test_restore_scope_rejects_extra_or_non_direct_file(self):
        cases = [
            (
                [
                    {
                        "id": "restored-1",
                        "name": "one.mp4",
                        "kind": "drive#file",
                        "parent_id": "owned-scope",
                    },
                    {
                        "id": "unrelated",
                        "name": "old.mp4",
                        "kind": "drive#file",
                        "parent_id": "owned-scope",
                    },
                ],
                "出现未映射文件",
            ),
            (
                [{
                    "id": "restored-1",
                    "name": "one.mp4",
                    "kind": "drive#file",
                    "parent_id": "other-scope",
                }],
                "不属于本次隔离目录",
            ),
            (
                [{
                    "id": "restored-1",
                    "name": "folder",
                    "kind": "drive#folder",
                    "parent_id": "owned-scope",
                }],
                "不是直属文件",
            ),
        ]

        for children, error_pattern in cases:
            with self.subTest(error_pattern=error_pattern):
                class RawClient:
                    PIKPAK_API_HOST = "api-drive.mypikpak.com"

                    async def _request_get(self, url, params=None):
                        return {
                            "phase": "PHASE_TYPE_COMPLETE",
                            "params": {
                                "trace_file_ids": json.dumps({
                                    "selected-1": "restored-1"
                                })
                            },
                        }

                    async def file_list(self, parent_id, next_page_token=None):
                        return {"files": children}

                client = build_client(RawClient())
                receipt = ShareRestoreReceipt(
                    "owned-scope", "restore-task", ("selected-1",)
                )

                with self.assertRaisesRegex(RuntimeError, error_pattern):
                    await client.wait_for_isolated_share_urls(
                        receipt, timeout=0.2, poll_interval=0.01
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

    async def test_selected_paths_bind_by_source_id_when_names_change(self):
        files = [
            {
                "source_file_id": "selected-1",
                "destination_file_id": "restored-1",
                "file_id": "restored-1",
                "name": "original.mkv",
                "url": "https://download/selected",
            }
        ]

        result = pikpak_routes._bind_selected_share_paths(
            files,
            ["selected-1"],
            {"selected-1": "Series/original.mkv"},
        )

        self.assertEqual(result[0]["path"], "Series/original.mkv")
        self.assertEqual(result[0]["source_path"], "Series/original.mkv")
        self.assertEqual(result[0]["source_name"], "original.mkv")

    async def test_duplicate_basenames_bind_to_their_own_source_ids(self):
        files = [
            {"source_file_id": "source-b", "name": "same.mkv"},
            {"source_file_id": "source-a", "name": "same.mkv"},
        ]

        result = pikpak_routes._bind_selected_share_paths(
            files,
            ["source-a", "source-b"],
            {
                "source-a": "Season A/same.mkv",
                "source-b": "Season B/same.mkv",
            },
        )

        self.assertEqual(
            [item["source_file_id"] for item in result],
            ["source-a", "source-b"],
        )
        self.assertEqual(
            [item["path"] for item in result],
            ["Season A/same.mkv", "Season B/same.mkv"],
        )

    async def test_unknown_or_duplicate_source_ids_fail_closed(self):
        files = [
            {"source_file_id": "selected-1", "name": "one.mkv"},
            {"source_file_id": "selected-1", "name": "two.mkv"},
        ]

        with self.assertRaisesRegex(RuntimeError, "源文件 ID"):
            pikpak_routes._bind_selected_share_paths(
                files,
                ["selected-1", "selected-2"],
                {"selected-1": "one.mkv", "selected-2": "two.mkv"},
            )


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
                return ShareRestoreReceipt(
                    "owned-scope", "restore-task", tuple(file_ids)
                )

            async def wait_for_isolated_share_urls(
                self, receipt, **kwargs
            ):
                self.wait_call = receipt
                return [
                    {
                        "source_file_id": "selected-1",
                        "destination_file_id": "restored-1",
                        "file_id": "restored-1",
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
        self.assertEqual(
            pikpak.wait_call,
            ShareRestoreReceipt(
                "owned-scope", "restore-task", ("selected-1",)
            ),
        )
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
                return ShareRestoreReceipt(
                    "owned-scope", "restore-task", tuple(file_ids)
                )

            async def wait_for_isolated_share_urls(
                self, receipt, **kwargs
            ):
                return [
                    {
                        "source_file_id": "selected-1",
                        "destination_file_id": "restored-1",
                        "file_id": "restored-1",
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
                return ShareRestoreReceipt(
                    "owned-scope", "restore-task", ("selected-1",)
                )

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
