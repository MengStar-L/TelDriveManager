# PikPak Share Restore Isolation Implementation Plan

**Goal:** Make PikPak share downloads resolve links only from a fresh directory owned by the current request, so one selected file can never release unrelated files from a reused PikPak directory.

**Architecture:** The PikPak client will create a unique empty scope, submit the existing restore request with that scope as `to_parent_id`, and poll only the scope's children for direct links. The share route will require the already-sent selected path manifest, verify the isolated result as an all-or-nothing contract, preserve original paths, push only after complete validation, and clean only the owned scope ID.

**Tech Stack:** Python 3.11+, asyncio, FastAPI, pikpakapi authenticated request helpers, unittest, aria2 JSON-RPC integration.

---

## File Map

- Create `tests/test_pikpak_share_restore_isolation.py`: focused regression coverage for owned restore scopes, fail-closed resolution, path contract validation, aria2 push count, and cleanup ownership.
- Modify `app/modules/pikpak/client.py:3-9,369-383`: replace the unsafe `save_share_files()` return-ID behavior with isolated restore creation and exact-count scope polling.
- Modify `app/modules/pikpak/routes.py:1385-1423,1772-1958`: validate selected path metadata, consume the isolated scope API, validate before any aria2 call, and clean only the owned scope.
- No change to `app/static/app.js`: both share download entry points already send `file_ids` and `file_paths` for checked items.
- No change to magnet/RSS paths, aria2 session handling, or PikPak account health checks.

## Success Invariants

1. `restore.file_id` is never passed to `get_download_urls()` or used as a traversal root by the share download workflow.
2. No aria2 call occurs until the isolated scope contains exactly the selected files and their basenames match the selected path manifest.
3. A partial, extra, or mismatched result fails the entire batch; the code does not filter and continue.
4. Cleanup receives only the ID returned by this request's successful `create_folder()` call.
5. The temporary scope name never appears in aria2 download directories or TelDrive target paths.

### Task 1: Add the owned restore scope API

**Files:**
- Create: `tests/test_pikpak_share_restore_isolation.py`
- Modify: `app/modules/pikpak/client.py:3-9,369-383`

- **Step 1: Write failing tests for scope creation and restore targeting**

Create `tests/test_pikpak_share_restore_isolation.py` with the client builder and the first four tests:

```python
import asyncio
import unittest
from typing import Any, cast
from unittest.mock import AsyncMock

from app.modules.pikpak.client import PikPakClient


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
                return {"file_id": "reused-unrelated-folder", "restore_status": "RESTORE_START"}

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
        self.assertEqual(raw.restore_calls[0][1], {
            "share_id": "share-1",
            "pass_code_token": "pass-token",
            "file_ids": ["selected-1"],
            "to_parent_id": "owned-scope",
        })
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
```

- **Step 2: Run the tests and verify the new API is missing**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: all four tests fail with `AttributeError: 'PikPakClient' object has no attribute 'start_isolated_share_restore'`.

- **Step 3: Implement the minimal isolated restore method**

Add `import uuid` to `app/modules/pikpak/client.py`, replace `save_share_files()` with:

```python
    async def start_isolated_share_restore(
        self, share_id: str, file_ids: List[str], pass_code_token: str
    ) -> str:
        parent_id = await self._get_save_dir_id()
        folder_name = f".teldrive-share-{uuid.uuid4().hex}"
        folder = await self.client.create_folder(name=folder_name, parent_id=parent_id)
        scope_id = str(
            folder.get("file", {}).get("id") or folder.get("id") or ""
        ).strip()
        if not scope_id:
            raise RuntimeError("PikPak 未返回隔离目录 ID，本次未转存任何文件")

        payload = {
            "share_id": share_id,
            "pass_code_token": pass_code_token,
            "file_ids": list(file_ids),
            "to_parent_id": scope_id,
        }
        try:
            result = await self.client._request_post(
                url=f"https://{self.client.PIKPAK_API_HOST}/drive/v1/share/restore",
                data=payload,
            )
            if not isinstance(result, dict):
                raise RuntimeError("PikPak 转存响应格式无效")
            if result.get("error"):
                error = result["error"]
                detail = error.get("message") if isinstance(error, dict) else str(error)
                raise RuntimeError(f"PikPak 隔离转存失败: {detail or error}")
        except BaseException:
            try:
                await self.delete_files([scope_id])
            except Exception as cleanup_error:
                logger.warning(f"清理未启用的分享隔离目录失败: {scope_id}, {cleanup_error}")
            raise

        logger.info(
            "分享转存已提交到隔离目录: scope=%s, selected=%s, status=%s",
            scope_id,
            len(file_ids),
            result.get("restore_status", ""),
        )
        return scope_id
```

Use `BaseException` only around the post-create restore block so an `asyncio.wait_for()` cancellation also cleans the known owned scope. Do not catch `BaseException` around folder creation because no reliable scope ID exists before its response.

- **Step 4: Run the focused tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: `4 passed`.

- **Step 5: Commit the owned-scope API**

```powershell
git add app/modules/pikpak/client.py tests/test_pikpak_share_restore_isolation.py
git commit -m "fix: isolate PikPak share restores"
```

### Task 2: Resolve only complete isolated scopes

**Files:**
- Modify: `tests/test_pikpak_share_restore_isolation.py`
- Modify: `app/modules/pikpak/client.py:204-272`

- **Step 1: Add failing tests for exact-count polling**

Append to `IsolatedShareRestoreClientTests`:

```python
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
        client._list_folder_files = AsyncMock(return_value=[
            {"name": "SAVR-1127-1.mp4", "url": "https://download/old-1"},
            {"name": "SAVR-1127-2.mp4", "url": "https://download/old-2"},
            {"name": "paid_8k.mp4", "url": "https://download/selected"},
        ])

        with self.assertRaisesRegex(RuntimeError, "隔离目录出现 3 个文件.*勾选 1 个"):
            await client.wait_for_isolated_share_urls(
                "owned-scope", expected_count=1, timeout=0.2, poll_interval=0.01
            )

    async def test_scope_polling_rejects_partial_result_after_timeout(self):
        client = build_client(object())
        client._list_folder_files = AsyncMock(return_value=[
            {"name": "one.mp4", "url": "https://download/one"},
        ])

        with self.assertRaisesRegex(RuntimeError, "应有 2 个文件，实际就绪 1 个"):
            await client.wait_for_isolated_share_urls(
                "owned-scope", expected_count=2, timeout=0.02, poll_interval=0.01
            )
```

- **Step 2: Run the tests and verify the resolver is missing**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: the three new tests fail with `AttributeError` for `wait_for_isolated_share_urls`.

- **Step 3: Implement exact-count scope polling**

Add this method next to `wait_for_download_urls()` in `app/modules/pikpak/client.py`:

```python
    async def wait_for_isolated_share_urls(
        self, scope_id: str, expected_count: int,
        timeout: float = 60.0, poll_interval: float = 3.0,
    ) -> List[Dict[str, str]]:
        scope_id = str(scope_id or "").strip()
        expected_count = int(expected_count or 0)
        if not scope_id or expected_count <= 0:
            raise ValueError("分享隔离目录和文件数量必须有效")

        timeout = max(float(timeout or 0.0), 0.0)
        poll_interval = max(float(poll_interval or 0.0), 0.01)
        request_timeout = max(1.0, min(timeout if timeout > 0 else 15.0, 15.0))
        deadline = time.monotonic() + timeout
        last_count = 0
        last_error: Optional[Exception] = None

        while True:
            try:
                files = await asyncio.wait_for(
                    self._list_folder_files(scope_id, prefix=""),
                    timeout=request_timeout,
                )
                last_count = len(files)
                if last_count > expected_count:
                    raise RuntimeError(
                        f"分享隔离目录出现 {last_count} 个文件，但只勾选 {expected_count} 个；"
                        "本次未推送任何下载链接"
                    )
                if last_count == expected_count:
                    return files
            except RuntimeError:
                raise
            except Exception as error:
                last_error = error

            if time.monotonic() >= deadline:
                detail = f"，最后错误：{last_error}" if last_error else ""
                raise RuntimeError(
                    f"分享隔离目录等待超时：应有 {expected_count} 个文件，"
                    f"实际就绪 {last_count} 个{detail}"
                )
            await asyncio.sleep(poll_interval)
```

Calling `_list_folder_files(scope_id, prefix="")` is mandatory. Do not call `get_download_urls(scope_id)`, because that method deliberately prefixes folder names and would leak the temporary scope name into download paths.

- **Step 4: Run all client isolation tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: `8 passed`.

- **Step 5: Commit exact-count isolated resolution**

```powershell
git add app/modules/pikpak/client.py tests/test_pikpak_share_restore_isolation.py
git commit -m "fix: resolve only complete PikPak share scopes"
```

### Task 3: Enforce the selected-file contract before orchestration

**Files:**
- Modify: `tests/test_pikpak_share_restore_isolation.py`
- Modify: `app/modules/pikpak/routes.py:1385-1423,1772-1775`

- **Step 1: Add failing tests for request metadata and exact basename validation**

Add imports:

```python
from app.modules.pikpak import routes as pikpak_routes
```

Add the following test class:

```python
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

        response = await pikpak_routes.api_share_download(Request())

        self.assertEqual(response.status_code, 400)
        self.assertIn("文件路径元数据不完整", response.body.decode("utf-8"))

    async def test_isolated_result_mismatch_fails_instead_of_filtering(self):
        files = [{
            "name": "SAVR-1127-1.mp4",
            "path": "SAVR-1127/SAVR-1127-1.mp4",
            "url": "https://download/old",
        }]

        with self.assertRaisesRegex(RuntimeError, "隔离解析结果与勾选文件不一致"):
            pikpak_routes._bind_selected_share_paths(
                files,
                ["selected-1"],
                {"selected-1": "Movie/paid_8k.mp4"},
            )

    async def test_selected_paths_replace_restore_scope_paths(self):
        files = [{
            "name": "paid_8k.mp4",
            "path": "temporary-wrapper/paid_8k.mp4",
            "url": "https://download/selected",
        }]

        result = pikpak_routes._bind_selected_share_paths(
            files,
            ["selected-1"],
            {"selected-1": "Movie/paid_8k.mp4"},
        )

        self.assertEqual(result[0]["path"], "Movie/paid_8k.mp4")
```

- **Step 2: Run tests and verify request/helper failures**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: the endpoint test returns success instead of `400`, and helper tests fail with missing `_bind_selected_share_paths`.

- **Step 3: Validate the existing `file_paths` field at the API boundary**

In `api_share_download()`, normalize `file_paths` and reject a request when any selected ID lacks a non-empty original path:

```python
        raw_file_paths = body.get("file_paths", {})
        file_paths = raw_file_paths if isinstance(raw_file_paths, dict) else {}
        file_paths = {
            str(file_id): str(path or "").strip().replace("\\", "/")
            for file_id, path in file_paths.items()
        }
        missing_paths = [file_id for file_id in file_ids if not file_paths.get(file_id)]
        if not share_id or not file_ids:
            return JSONResponse({"error": "缺少参数"}, status_code=400)
        if missing_paths:
            return JSONResponse(
                {"error": "所选文件路径元数据不完整，请重新解析分享链接后再下载"},
                status_code=400,
            )
```

The current unified and legacy share UIs already populate `file_paths`; no frontend change is needed.

- **Step 4: Add all-or-nothing result validation and path rebasing**

Add this helper near `_normalize_selected_ids()` in `app/modules/pikpak/routes.py`:

```python
def _bind_selected_share_paths(
    files: List[dict], file_ids: List[str], file_paths: Dict[str, str]
) -> List[dict]:
    expected_paths = [
        str(file_paths.get(file_id) or "").strip().replace("\\", "/")
        for file_id in file_ids
    ]
    expected_names = [posixpath.basename(path) for path in expected_paths]
    actual_names = [str(item.get("name") or "") for item in files]
    if sorted(actual_names) != sorted(expected_names):
        raise RuntimeError(
            "PikPak 隔离解析结果与勾选文件不一致，本次未推送任何下载链接"
        )

    paths_by_name: Dict[str, List[str]] = {}
    for path in expected_paths:
        paths_by_name.setdefault(posixpath.basename(path), []).append(path)
    for paths in paths_by_name.values():
        paths.sort(key=_natural_sort_key)

    rebound = []
    for item in files:
        copy = dict(item)
        copy["path"] = paths_by_name[copy["name"]].pop(0)
        rebound.append(copy)
    return rebound
```

This helper validates an already isolated scope. It must never receive results from `restore.file_id`, and it must raise on mismatch rather than selecting matching entries.

- **Step 5: Run the contract tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: `11 passed`.

- **Step 6: Commit the request and result contract**

```powershell
git add app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
git commit -m "fix: validate isolated PikPak share results"
```

### Task 4: Replace unsafe share orchestration with the isolated flow

**Files:**
- Modify: `tests/test_pikpak_share_restore_isolation.py`
- Modify: `app/modules/pikpak/routes.py:1772-1958`

- **Step 1: Add route-level regression fakes and success test**

Append the route test class below. It verifies the full boundary from selected ID to aria2 URL and cleanup target:

```python
class ShareDownloadIsolationRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.originals = {
            name: getattr(pikpak_routes, name)
            for name in (
                "_next_pikpak_client", "load_config", "_broadcast",
                "_broadcast_resolved_files", "_broadcast_link_pushed",
                "_ensure_aria2_client", "_register_aria2_task",
            )
        }
        self.original_add_error = pikpak_routes.db.add_pikpak_account_error
        self.original_defer = pikpak_routes.task_manager.should_defer_new_downloads
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
        account = pikpak_routes.PikPakAccountContext("account-1", "A", "token")

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
        pikpak_routes._ensure_aria2_client = lambda: asyncio.sleep(0, result=aria2)
        pikpak_routes._register_aria2_task = no_op
        pikpak_routes.db.add_pikpak_account_error = no_op
        pikpak_routes.task_manager.should_defer_new_downloads = lambda: False
        pikpak_routes.task_manager.hold_gids_for_disk_gate = lambda gids: None

    async def test_one_selected_file_pushes_one_isolated_url(self):
        class PikPak:
            def __init__(self):
                self.deleted = []

            async def start_isolated_share_restore(self, share_id, file_ids, token):
                self.start_call = (share_id, file_ids, token)
                return "owned-scope"

            async def wait_for_isolated_share_urls(self, scope_id, expected_count, **kwargs):
                self.wait_call = (scope_id, expected_count)
                return [{
                    "name": "paid_8k.mp4",
                    "path": "temporary/paid_8k.mp4",
                    "url": "https://download/selected",
                    "size": 100,
                }]

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
        self.assertEqual([item[0] for item in aria2.added], ["https://download/selected"])
        self.assertEqual(aria2.added[0][1]["dir"], "C:/downloads")
        self.assertEqual(pikpak.deleted, [["owned-scope"]])

    async def test_serial_mode_enqueues_only_the_isolated_url(self):
        class PikPak:
            def __init__(self):
                self.deleted = []

            async def start_isolated_share_restore(self, share_id, file_ids, token):
                return "owned-scope"

            async def wait_for_isolated_share_urls(self, scope_id, expected_count, **kwargs):
                return [{
                    "name": "paid_8k.mp4",
                    "path": "temporary/paid_8k.mp4",
                    "url": "https://download/selected",
                    "size": 100,
                }]

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
```

- **Step 2: Add the failure test proving aria2 is untouched**

Append to the same class:

```python
    async def test_isolation_failure_pushes_nothing_and_cleans_scope(self):
        class PikPak:
            def __init__(self):
                self.deleted = []

            async def start_isolated_share_restore(self, *args):
                return "owned-scope"

            async def wait_for_isolated_share_urls(self, *args, **kwargs):
                raise RuntimeError("隔离目录出现 3 个文件，但只勾选 1 个")

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
```

- **Step 3: Run the route tests and verify they fail on the old methods**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: route tests fail because `_process_share_download()` still calls `save_share_files()` and `wait_for_download_urls()`.

- **Step 4: Replace `saved_ids` with one owned scope**

In `_process_share_download()`:

```python
    owned_scope_id = ""
```

Replace the `save_share_files()` block and the loop over `saved_ids` with:

```python
        try:
            owned_scope_id = await asyncio.wait_for(
                pikpak.start_isolated_share_restore(
                    share_id, file_ids, pass_code_token
                ),
                timeout=share_parse_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                _build_share_fallback_message(
                    f"分享隔离转存超时 ({int(share_parse_timeout)}s)"
                )
            ) from exc

        await _broadcast({
            "type": "task_status",
            "index": 1,
            "status": f"隔离转存已提交，共 {total} 项，正在解析所选文件的下载链接...",
        })
        all_urls = await pikpak.wait_for_isolated_share_urls(
            owned_scope_id,
            expected_count=total,
            timeout=share_url_timeout,
            poll_interval=share_poll_interval,
        )
        all_urls = _bind_selected_share_paths(all_urls, file_ids, file_paths or {})
        await _broadcast_resolved_files(1, all_urls)
```

Keep the existing `orig_paths_by_name`, sorting, aria2/TelDrive push, rename, and progress broadcasting after this block. Remove `share_timeout_hits` because isolated resolution is all-or-nothing and never continues after one selected item fails.

- **Step 5: Restrict final cleanup to the owned scope**

Replace the existing `saved_ids` cleanup in `finally` with:

```python
        if owned_scope_id and pikpak is not None:
            try:
                await pikpak.delete_files([owned_scope_id])
            except Exception as error:
                logger.warning(
                    f"清理 PikPak 分享隔离目录失败: scope={owned_scope_id}, error={error}"
                )
```

Do not add response `file_id`, `task_info` IDs, or resolved child IDs to this cleanup call.

- **Step 6: Run the complete isolation test file**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: `14 passed`.

- **Step 7: Commit the route migration**

```powershell
git add app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
git commit -m "fix: fail closed on PikPak share resolution"
```

### Task 5: Run regression and source-safety checks

**Files:**
- Verify: `app/modules/pikpak/client.py`
- Verify: `app/modules/pikpak/routes.py`
- Verify: `tests/test_pikpak_share_restore_isolation.py`

- **Step 1: Prove the unsafe traversal is gone**

Run:

```powershell
rg -n "save_share_files|wait_for_download_urls\(.*restore|delete_files\(saved_ids\)" app/modules/pikpak tests
```

Expected: no share-download production call remains. Existing generic `wait_for_download_urls()` may remain for non-share workflows.

- **Step 2: Run focused PikPak and scheduling tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation tests.test_pikpak_multi_account tests.test_parallel_dispatch tests.test_serial_gate -v
```

Expected: all tests pass.

- **Step 3: Run Python syntax validation**

Run:

```powershell
.\.venv\Scripts\python.exe -m py_compile app/modules/pikpak/client.py app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
```

Expected: exit code `0` with no output.

- **Step 4: Run the complete Python regression suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -v
```

Expected: all tests pass. If an existing unrelated dirty-worktree test fails, record the exact test and verify the three files in this plan still pass independently; do not modify unrelated Telegram cleanup work to force a green run.

- **Step 5: Review the final diff for ownership guarantees**

Run:

```powershell
git diff --check
git diff -- app/modules/pikpak/client.py app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
```

Expected: no whitespace errors; every delete target originates from `create_folder()`, and no code traverses `restore.file_id`.

### Task 6: Production rollout and live proof (separate authorization gate)

**Files:**
- Deploy: `app/modules/pikpak/client.py`
- Deploy: `app/modules/pikpak/routes.py`
- Preserve: `/opt/TelDriveManager/tasks.db`, configuration, tokens, sessions, downloads, and aria2 state.

This task changes the remote server and restarts `teldrive-manager.service`. Execute it only after local work is complete and the user explicitly authorizes deployment.

- **Step 1: Record the production revision and service state**

Run:

```powershell
ssh root@107.175.185.21 "cd /opt/TelDriveManager && git rev-parse HEAD"
ssh root@107.175.185.21 "sha256sum /opt/TelDriveManager/app/modules/pikpak/client.py /opt/TelDriveManager/app/modules/pikpak/routes.py"
ssh root@107.175.185.21 "systemctl is-active teldrive-manager.service"
```

Expected: the revision and both hashes are recorded, and the service prints `active`.

- **Step 2: Back up only the two target production files**

Run:

```powershell
ssh root@107.175.185.21 "mkdir -p /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-share-restore-isolation"
ssh root@107.175.185.21 "cp --preserve=all /opt/TelDriveManager/app/modules/pikpak/client.py /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-share-restore-isolation/client.py.before"
ssh root@107.175.185.21 "cp --preserve=all /opt/TelDriveManager/app/modules/pikpak/routes.py /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-share-restore-isolation/routes.py.before"
ssh root@107.175.185.21 "sha256sum /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-share-restore-isolation/*.before"
```

These commands create copies of:

```text
/opt/TelDriveManager/app/modules/pikpak/client.py
/opt/TelDriveManager/app/modules/pikpak/routes.py
```

Expected: two regular backup files exist in the dedicated directory and their hashes match Step 1; no database, config, session, download, or aria2 file is moved or deleted.

- **Step 3: Upload the verified local files and compile them remotely**

Upload only the two target files to temporary names:

```powershell
scp app/modules/pikpak/client.py root@107.175.185.21:/tmp/teldrive-pikpak-client.py
scp app/modules/pikpak/routes.py root@107.175.185.21:/tmp/teldrive-pikpak-routes.py
ssh root@107.175.185.21 "install -m 0644 /tmp/teldrive-pikpak-client.py /opt/TelDriveManager/app/modules/pikpak/client.py"
ssh root@107.175.185.21 "install -m 0644 /tmp/teldrive-pikpak-routes.py /opt/TelDriveManager/app/modules/pikpak/routes.py"
```

Then run:

```bash
/opt/TelDriveManager/venv/bin/python -m py_compile \
  /opt/TelDriveManager/app/modules/pikpak/client.py \
  /opt/TelDriveManager/app/modules/pikpak/routes.py
```

Expected: exit code `0` with no output.

- **Step 4: Restart and verify the service**

Run:

```powershell
ssh root@107.175.185.21 "systemctl restart teldrive-manager.service"
ssh root@107.175.185.21 "systemctl is-active teldrive-manager.service"
ssh root@107.175.185.21 "systemctl status teldrive-manager.service --no-pager -l"
ssh root@107.175.185.21 "journalctl -u teldrive-manager.service --since '2 minutes ago' --no-pager"
```

Expected: service reports `active`; there are no import, syntax, startup, database, Telegram, or aria2 connection errors.

- **Step 5: Perform one user-triggered share download proof**

Before the click, record aria2 state:

```powershell
ssh root@107.175.185.21 "curl -fsS -H 'Content-Type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":\"before\",\"method\":\"aria2.tellActive\"}' http://127.0.0.1:6822/jsonrpc"
ssh root@107.175.185.21 "curl -fsS -H 'Content-Type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":\"before\",\"method\":\"aria2.tellWaiting\",\"params\":[0,1000]}' http://127.0.0.1:6822/jsonrpc"
```

Have the user parse a share, check exactly one file, and click “推送下载链接”. Then run:

```powershell
ssh root@107.175.185.21 "journalctl -u teldrive-manager.service --since '5 minutes ago' --no-pager | grep -E '分享转存|隔离目录|发现 aria2 任务|下载链接'"
ssh root@107.175.185.21 "curl -fsS -H 'Content-Type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":\"after\",\"method\":\"aria2.tellActive\"}' http://127.0.0.1:6822/jsonrpc"
ssh root@107.175.185.21 "curl -fsS -H 'Content-Type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":\"after\",\"method\":\"aria2.tellWaiting\",\"params\":[0,1000]}' http://127.0.0.1:6822/jsonrpc"
```

Expected:

```text
selected file count: 1
isolated scope resolved file count: 1
new aria2 GID count: 1
new aria2 filename: exactly the selected parsed filename
cleanup target: the owned scope ID only
```

If any count differs, stop testing, do not retry with broader cleanup, restore the two backed-up source files, restart the service, and report the captured logs.
