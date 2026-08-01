# PikPak Explicit Restore Target Implementation Plan

**Goal:** Restore only the selected PikPak share files into a request-owned directory using the live-verified target protocol, and push links only after the restore task mapping and direct directory members match exactly.

**Architecture:** `PikPakClient` will return a typed restore receipt containing the owned scope, task ID, and ordered source IDs. It will poll the restore task, parse the authoritative source-to-destination mapping, validate the owned directory's direct children against that mapping, and resolve links only for those children. The share route will pass the receipt through this validation and retain its current fail-closed push and owned-scope cleanup behavior.

**Tech Stack:** Python 3.11, `asyncio`, `dataclasses`, PikPakAPI 0.1.11 authenticated request helpers, `unittest`, FastAPI route helpers, systemd deployment over SSH.

---

## File Map

- Modify `app/modules/pikpak/client.py`: define the restore receipt, send the verified payload, poll the task, validate trace mappings and direct children, then resolve direct links.
- Modify `app/modules/pikpak/routes.py`: retain the owned scope ID from the receipt and pass the receipt into strict URL resolution.
- Modify `tests/test_pikpak_share_restore_isolation.py`: replace the obsolete nested-target expectations and add task-mapping/direct-child failure coverage.
- Create no new runtime modules or configuration fields.

### Task 1: Lock the Verified Restore Request Contract

**Files:**
- Modify: `tests/test_pikpak_share_restore_isolation.py:18-144`
- Test: `tests/test_pikpak_share_restore_isolation.py`

- **Step 1: Replace the obsolete request test with a failing explicit-target test**

Use a fake restore response whose `file_id` equals `owned-scope` and whose task ID is `restore-task`. Assert that the return value exposes all three receipt fields and that the payload is exactly:

```python
{
    "parent_id": "owned-scope",
    "share_id": "share-1",
    "pass_code_token": "pass-token",
    "file_ids": ["selected-1", "selected-2"],
    "ancestor_ids": [],
    "specify_parent_id": True,
    "params": {"trace_file_ids": "selected-1,selected-2"},
}
```

Also assert that neither `to` nor `to_parent_id` exists.

- **Step 2: Add a failing response-target mismatch test**

Return `{"file_id": "Pack From Shared", "restore_task_id": "restore-task"}` from the fake API. Require `start_isolated_share_restore()` to raise an error containing `恢复目标目录不一致` and require `delete_forever()` to receive only `["owned-scope"]`.

- **Step 3: Add a failing missing-task-ID test**

Return an otherwise successful response without `restore_task_id`. Require an error containing `未返回恢复任务 ID` and require cleanup of only the owned scope.

- **Step 4: Run the focused tests and verify the new contract fails**

Run:

```powershell
python -m unittest tests.test_pikpak_share_restore_isolation.IsolatedShareRestoreClientTests -v
```

Expected: failures because the implementation still sends `to.parent_id`, accepts a mismatched response target, returns a string, and does not require a task ID.

### Task 2: Implement the Explicit Restore Receipt and Payload

**Files:**
- Modify: `app/modules/pikpak/client.py:1-15`
- Modify: `app/modules/pikpak/client.py:412-456`
- Test: `tests/test_pikpak_share_restore_isolation.py`

- **Step 1: Define the receipt type**

Add an immutable receipt near the module constants:

```python
@dataclass(frozen=True)
class ShareRestoreReceipt:
    scope_id: str
    task_id: str
    selected_ids: tuple[str, ...]
```

- **Step 2: Normalize and validate selected IDs before creating a directory**

Build an ordered tuple of non-empty string IDs, reject an empty tuple, and reject duplicates. This keeps the trace mapping one-to-one and preserves the user's selection order.

- **Step 3: Send the live-verified request body**

Replace the nested `to` object with the exact payload from Task 1. Keep using the PikPak client's authenticated `_request_post()` method and the same `/drive/v1/share/restore` endpoint.

- **Step 4: Fail closed on response mismatch**

After existing response-format and API-error checks, require:

```python
returned_scope_id = str(result.get("file_id") or "").strip()
task_id = str(result.get("restore_task_id") or "").strip()
if returned_scope_id != scope_id:
    raise RuntimeError("PikPak 恢复目标目录不一致，本次未解析或推送任何文件")
if not task_id:
    raise RuntimeError("PikPak 未返回恢复任务 ID，本次未解析或推送任何文件")
```

Return `ShareRestoreReceipt(scope_id, task_id, selected_ids)`. The existing exception cleanup must continue deleting only `scope_id`.

- **Step 5: Run the focused client tests**

Run the Task 1 command again.

Expected: the payload, receipt, target mismatch, missing task ID, create failure, and API failure tests pass; task polling tests may still fail until Task 3.

### Task 3: Validate the Restore Task Mapping and Direct Children

**Files:**
- Modify: `tests/test_pikpak_share_restore_isolation.py:145-235`
- Modify: `app/modules/pikpak/client.py:233-316`
- Test: `tests/test_pikpak_share_restore_isolation.py`

- **Step 1: Add task-mapping success tests**

Make `_request_get()` return a completed task with:

```python
{
    "phase": "PHASE_TYPE_COMPLETE",
    "message": "Completed",
    "params": {
        "trace_file_ids": json.dumps({
            "selected-1": "restored-1",
            "selected-2": "restored-2",
        })
    },
}
```

Make `file_list(parent_id="owned-scope")` return exactly two direct `drive#file` entries with those restored IDs, matching `parent_id`, and usable links. Assert the result order follows `selected_ids`, not the API listing order.

- **Step 2: Add task failure and mapping mismatch tests**

Cover each condition separately:

```text
PHASE_TYPE_ERROR -> restore task failed
invalid trace JSON -> trace mapping invalid
missing selected source ID -> selected mapping mismatch
extra source ID -> selected mapping mismatch
duplicate/empty destination ID -> target mapping invalid
```

Each case must raise before returning any URL.

- **Step 3: Add direct-child mismatch tests**

Cover partial children as a retry-until-timeout condition. Cover extra child IDs, mapped IDs outside the scope, and a mapped `drive#folder` as immediate hard failures. Assert `_list_folder_files()` is not used, proving the restore scope is never recursively expanded.

- **Step 4: Implement trace parsing and task polling**

Add a focused parser that accepts a JSON string or dictionary, normalizes string keys/values, and requires exact source keys plus unique non-empty destination IDs. Poll:

```text
GET https://{PIKPAK_API_HOST}/drive/v1/tasks/{receipt.task_id}
```

until `PHASE_TYPE_COMPLETE`, a terminal error phase, or timeout. Include `params.error_detail` or `message` in terminal errors when present.

- **Step 5: Implement direct-child listing without recursion**

Call `file_list(parent_id=receipt.scope_id)` page by page. Preserve `id`, `name`, `kind`, `parent_id`, and `size`. Never descend into folders. Require the child ID set to equal the mapping destination set and require every item to be a direct `drive#file` child of the receipt scope.

- **Step 6: Resolve all links before returning**

For each destination ID in selected-source order, use any link already present in the direct listing, then fall back to `get_download_url(destination_id)`. If any link is unavailable, retry until timeout. Return the complete list only after every mapped item has a URL.

- **Step 7: Run strict client tests**

Run:

```powershell
python -m unittest tests.test_pikpak_share_restore_isolation.IsolatedShareRestoreClientTests -v
```

Expected: all client isolation tests pass, including mapping and non-recursion assertions.

### Task 4: Pass the Receipt Through the Share Route

**Files:**
- Modify: `tests/test_pikpak_share_restore_isolation.py:236-506`
- Modify: `app/modules/pikpak/routes.py:1811-1870`
- Test: `tests/test_pikpak_share_restore_isolation.py`

- **Step 1: Update route fakes to return a receipt-shaped object**

Use `ShareRestoreReceipt("owned-scope", "restore-task", ("selected-1",))` from fake `start_isolated_share_restore()` methods. Require fake `wait_for_isolated_share_urls()` calls to receive the receipt instead of separate scope/count values.

- **Step 2: Add a route-level fail-closed test**

Make strict URL resolution raise a task mapping mismatch. Assert aria2 `add_uri()` and the serial queue are never called, while `delete_files()` receives only `["owned-scope"]`.

- **Step 3: Update production route wiring**

Store the receipt returned by `start_isolated_share_restore()`, immediately copy `receipt.scope_id` into `owned_scope_id` for cleanup ownership, and call:

```python
all_urls = await pikpak.wait_for_isolated_share_urls(
    restore_receipt,
    timeout=share_url_timeout,
    poll_interval=share_poll_interval,
)
```

Do not retain `expected_count`; the receipt's selected IDs and task trace mapping define the exact contract.

- **Step 4: Run the full isolation test module**

Run:

```powershell
python -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: all tests pass and no fake receives an unexpected aria2 call.

### Task 5: Regression and Static Verification

**Files:**
- Verify: `app/modules/pikpak/client.py`
- Verify: `app/modules/pikpak/routes.py`
- Verify: `tests/test_pikpak_share_restore_isolation.py`

- **Step 1: Verify obsolete restore target fields are absent from runtime code**

Run:

```powershell
rg -n -F '"to"' app/modules/pikpak/client.py
rg -n -F 'to_parent_id' app/modules/pikpak
rg -n -F 'specify_parent_id' app/modules/pikpak/client.py
```

Expected: the first two searches return no runtime matches; the third returns the explicit restore payload.

- **Step 2: Run syntax compilation**

Run:

```powershell
python -m py_compile app/modules/pikpak/client.py app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
```

Expected: exit code 0 with no output.

- **Step 3: Run focused PikPak regression tests**

Run:

```powershell
python -m unittest discover -s tests -p "test_pikpak*.py" -v
```

Expected: all PikPak tests pass.

- **Step 4: Run the full repository test suite**

Run:

```powershell
python -m unittest discover -s tests -v
```

Expected: all tests pass. Any pre-existing unrelated failure must be reported with its exact test name and must not be hidden by changing unrelated files.

- **Step 5: Review the scoped diff and commit only the PikPak fix**

Run:

```powershell
git diff --check -- app/modules/pikpak/client.py app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
git diff -- app/modules/pikpak/client.py app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py
git add app/modules/pikpak/client.py app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py .openteams/specs/2026-08-01-pikpak-explicit-restore-target-design.html .openteams/plans/2026-08-01-pikpak-explicit-restore-target.md
git commit -m "fix: target PikPak share restores explicitly"
```

Expected: diff check passes and the commit excludes unrelated Telegram, TelDrive, frontend, config, and test changes.

### Task 6: Deploy and Prove the Supplied Share End to End

**Files:**
- Deploy: `/opt/TelDriveManager/app/modules/pikpak/client.py`
- Deploy: `/opt/TelDriveManager/app/modules/pikpak/routes.py`
- Backup: `/opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-explicit-restore-target/`

- **Step 1: Capture pre-deployment state and create targeted backups**

Record checksums of both deployed files. Create the named backup directory and copy only `client.py` and `routes.py` into it before uploading replacements.

- **Step 2: Upload the verified local files and restart the service**

Upload both modules, run remote `py_compile`, restart `teldrive-manager.service`, and require `systemctl is-active teldrive-manager.service` to print `active`.

- **Step 3: Run a no-aria2 single-file probe**

Using the supplied share, select `paid_4k.mp4`. Require response `file_id == scope_id`, a completed task mapping with exactly that source ID, and one matching direct child. Delete only the probe scope.

- **Step 4: Run a no-aria2 two-file probe**

Select the supplied 8K and 4K files together. Require two exact source mappings, two exact destination children, matching names/sizes, and no extras. Delete only the probe scope.

- **Step 5: Exercise the deployed application entry point**

Submit one selected file through the application's share download API while observing aria2. Require exactly one new aria2 task whose output name matches the selected share item, and verify logs contain no traversal or use of `Pack From Shared`.

- **Step 6: Verify cleanup and service health**

Confirm the owned restore scope no longer exists, `Pack From Shared` was not deleted or modified by cleanup, the service remains active, and recent service logs contain no unhandled traceback.

- **Step 7: Roll back on any failed acceptance check**

Restore only the two module backups from Step 1, run remote syntax compilation, restart the service, and verify it is active. Do not delete any user-created PikPak or Telegram content during rollback.
