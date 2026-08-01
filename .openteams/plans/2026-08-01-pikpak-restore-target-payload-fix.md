# PikPak Restore Target Payload Fix Implementation Plan

**Goal:** Send PikPak share restores to the request-owned isolation directory using the API's accepted nested `to.parent_id` payload, without weakening any existing fail-closed validation or cleanup ownership rule.

**Architecture:** Keep the existing isolated share workflow intact and change only the restore request contract in `PikPakClient.start_isolated_share_restore()`. Lock the wire format with an exact unit-test assertion, then deploy only `client.py` over the currently safe failure-closed production version and prove one selected file creates exactly one aria2 task.

**Tech Stack:** Python 3.11+, asyncio, pikpakapi authenticated request helper, unittest, FastAPI service under systemd, aria2 JSON-RPC.

---

## File Map

- Modify `tests/test_pikpak_share_restore_isolation.py:19-60`: require nested `to.parent_id` and explicitly reject the ineffective flat `to_parent_id` field.
- Modify `app/modules/pikpak/client.py:412-439`: change only the restore request payload shape.
- Preserve `app/modules/pikpak/routes.py`: its isolated polling, all-or-nothing result binding, aria2 push gate, and owned-scope cleanup are already correct and are not part of this correction.
- Preserve all Telegram, TelDrive, aria2 task-manager, configuration, database, session, and frontend files.

## Success Invariants

1. The restore request contains `"to": {"parent_id": scope_id}` and does not contain `to_parent_id`.
2. `restore.file_id` remains ignored and is never used as a traversal root or cleanup target.
3. No aria2 call occurs until the owned isolation directory has exactly the selected files and their names match the selected manifest.
4. Cleanup still receives only the ID returned by this request's `create_folder()` call.
5. Production proof for one selected file yields one ready isolated file and exactly one new aria2 GID.
6. The program does not scan or delete a possible file misplaced into PikPak's default directory by the earlier invalid payload.

### Task 1: Lock the accepted PikPak payload with a failing test

**Files:**
- Modify: `tests/test_pikpak_share_restore_isolation.py:19-60`
- Verify: `app/modules/pikpak/client.py:412-439`

- **Step 1: Change the exact request assertion**

Replace the expected payload in `test_restore_targets_owned_scope_and_ignores_response_file_id()` with:

```python
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
```

Keep the existing assertions that the returned scope is `owned-scope`, the created folder name starts with `.teldrive-share-`, and the response's `reused-unrelated-folder` ID is not deleted or returned.

- **Step 2: Run only the changed regression test**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.IsolatedShareRestoreClientTests.test_restore_targets_owned_scope_and_ignores_response_file_id -v
```

Expected: one failure showing that the actual payload contains `to_parent_id` instead of nested `to.parent_id`. A pass at this step means the production implementation has already changed and the baseline must be rechecked before proceeding.

- **Step 3: Review the test diff**

Run:

```powershell
git diff -- tests/test_pikpak_share_restore_isolation.py
```

Expected: only the payload assertion changes; no isolation, mismatch, aria2, serial-mode, or cleanup assertion is removed.

### Task 2: Make the minimal request-contract correction

**Files:**
- Modify: `app/modules/pikpak/client.py:423-429`
- Test: `tests/test_pikpak_share_restore_isolation.py`

- **Step 1: Replace the flat field with the nested target object**

Change the payload in `start_isolated_share_restore()` to:

```python
        payload = {
            "share_id": share_id,
            "pass_code_token": pass_code_token,
            "file_ids": list(file_ids),
            "to": {"parent_id": scope_id},
        }
```

Do not call `self.client.restore()`, because pikpakapi 0.1.11 does not expose a target-directory argument. Keep the authenticated `_request_post()` call and all surrounding creation, response validation, cancellation cleanup, logging, and return behavior unchanged.

- **Step 2: Run the changed regression test**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation.IsolatedShareRestoreClientTests.test_restore_targets_owned_scope_and_ignores_response_file_id -v
```

Expected: `Ran 1 test` and `OK`.

- **Step 3: Run all PikPak isolation tests**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_pikpak_share_restore_isolation -v
```

Expected: `Ran 14 tests` and `OK`. This includes extra-file rejection, partial-result timeout, transient listing retry, name mismatch failure, one-link parallel push, one-link serial enqueue, zero-link failure, and owned-scope cleanup.

- **Step 4: Prove the flat field is absent from production code**

Run:

```powershell
rg -n -F "to_parent_id" app\modules\pikpak
rg -n -F '"to": {"parent_id": scope_id}' app\modules\pikpak\client.py
```

Expected: the first command has no matches; the second command points to the payload in `start_isolated_share_restore()`.

- **Step 5: Run syntax validation**

Run:

```powershell
.\.venv\Scripts\python.exe -m py_compile app\modules\pikpak\client.py tests\test_pikpak_share_restore_isolation.py
```

Expected: exit code `0` with no output.

- **Step 6: Run the full Python regression suite**

Run:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py" -v
```

Expected: all 149 tests pass. Any different total must be explained by newly added or removed tests; unrelated Telegram or TelDrive test failures must not be fixed by changing those modules.

- **Step 7: Review whitespace and scope**

Run:

```powershell
git diff --check
git diff -- app/modules/pikpak/client.py tests/test_pikpak_share_restore_isolation.py
```

Expected: no whitespace error in the two target files, exactly one production payload field changes, and the test asserts the same wire format.

- **Step 8: Commit the complete PikPak isolation fix without unrelated changes**

Stage only the complete PikPak isolation implementation, its tests, and its approved documents:

```powershell
git add app/modules/pikpak/client.py app/modules/pikpak/routes.py tests/test_pikpak_share_restore_isolation.py .openteams/specs/2026-08-01-pikpak-share-restore-isolation-design.html .openteams/specs/2026-08-01-pikpak-restore-target-payload-fix-design.html .openteams/plans/2026-08-01-pikpak-share-restore-isolation.md .openteams/plans/2026-08-01-pikpak-restore-target-payload-fix.md
git diff --cached --check
git commit -m "fix: isolate PikPak share restore target"
```

Expected: the commit succeeds and no README, Telegram, TelDrive, configuration, frontend, or unrelated test file is staged.

### Task 3: Deploy only the corrected client and prove one-file behavior

**Files:**
- Deploy: `app/modules/pikpak/client.py`
- Preserve remotely: `/opt/TelDriveManager/app/modules/pikpak/routes.py`
- Preserve remotely: `/opt/TelDriveManager/tasks.db`, configuration, credentials, sessions, downloads, aria2 state, and every Telegram/TelDrive file.

- **Step 1: Record the current safe production state**

Run through the approved SSH credential channel without placing the password in command output:

```powershell
ssh root@107.175.185.21 "cd /opt/TelDriveManager && git rev-parse HEAD && sha256sum app/modules/pikpak/client.py app/modules/pikpak/routes.py && systemctl is-active teldrive-manager.service"
```

Expected before deployment:

```text
revision: d584bcd45d9d695c1903956fc58b7031232382e5
client.py: 7aaf300235b46419d5a79709ec1627e19c651bad22acd7e68475155b38df2ed6
routes.py: 56f1d7ff4bc2fd22568881ca28035ef28ee78fe60e13bd21fbf9df6a78f4f514
service: active
```

If either source hash differs, stop and compare the remote file before overwriting it.

- **Step 2: Back up only the current failure-closed client**

Run:

```powershell
ssh root@107.175.185.21 "install -d -m 0700 /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-restore-target-payload-fix"
ssh root@107.175.185.21 "cp --preserve=all /opt/TelDriveManager/app/modules/pikpak/client.py /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-restore-target-payload-fix/client.py.before"
ssh root@107.175.185.21 "sha256sum /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-restore-target-payload-fix/client.py.before"
```

Expected: the backup hash is `7aaf300235b46419d5a79709ec1627e19c651bad22acd7e68475155b38df2ed6`. Do not back up, copy, move, or delete any database, config, token, session, download, or aria2 file.

- **Step 3: Upload and compile only `client.py`**

Run:

```powershell
scp app/modules/pikpak/client.py root@107.175.185.21:/tmp/teldrive-pikpak-client-to-payload-fix.py
ssh root@107.175.185.21 "install -m 0644 /tmp/teldrive-pikpak-client-to-payload-fix.py /opt/TelDriveManager/app/modules/pikpak/client.py"
ssh root@107.175.185.21 "/opt/TelDriveManager/venv/bin/python -m py_compile /opt/TelDriveManager/app/modules/pikpak/client.py /opt/TelDriveManager/app/modules/pikpak/routes.py"
```

Expected: compile exits `0` with no output. Verify the deployed SHA-256 equals the local corrected `client.py` hash before restarting.

- **Step 4: Restart and perform delayed health checks**

Run:

```powershell
ssh root@107.175.185.21 "systemctl restart teldrive-manager.service"
ssh root@107.175.185.21 "systemctl is-active teldrive-manager.service"
ssh root@107.175.185.21 "curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8888/"
ssh root@107.175.185.21 "journalctl -u teldrive-manager.service --since '3 minutes ago' --no-pager -n 200"
```

Expected: service is `active`, HTTP returns the application's expected `303` redirect, and the journal contains none of `Traceback`, `SyntaxError`, `ImportError`, `ModuleNotFoundError`, or `Failed to start`.

- **Step 5: Capture the aria2 baseline**

Before the user clicks, query local aria2 for active and waiting tasks, retaining each GID and filename:

```bash
curl -fsS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"before-active","method":"aria2.tellActive","params":[["gid","status","files"]]}' \
  http://127.0.0.1:6822/jsonrpc
curl -fsS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"before-waiting","method":"aria2.tellWaiting","params":[0,1000,["gid","status","files"]]}' \
  http://127.0.0.1:6822/jsonrpc
```

Expected: a baseline snapshot is retained; no task is removed, paused, or modified.

- **Step 6: Perform the user-triggered one-file proof**

Have the user parse the same share, check only `(正版VR资源 飞机号 VR77580) paid_8k.mp4`, and click the existing push-download command once. Then capture:

```powershell
ssh root@107.175.185.21 "journalctl -u teldrive-manager.service --since '5 minutes ago' --no-pager | grep -E '分享转存已提交到隔离目录|分享下载失败|隔离目录'"
```

Re-run the two aria2 JSON-RPC queries from Step 5 with IDs `after-active` and `after-waiting`.

Expected:

```text
selected file count: 1
isolated scope ready file count: 1
new aria2 GID count: 1
new aria2 filename: (正版VR资源 飞机号 VR77580) paid_8k.mp4
cleanup target: only the request-owned scope ID
```

No `SAVR-1127` task or other unselected filename may appear in the new GID set.

- **Step 7: Roll back on any failed production invariant**

If service health fails, the isolated scope does not become ready, or the new aria2 GID count/filename is not exact, stop testing and run:

```powershell
ssh root@107.175.185.21 "install -m 0644 /opt/TelDriveManager/deploy-backups/2026-08-01-pikpak-restore-target-payload-fix/client.py.before /opt/TelDriveManager/app/modules/pikpak/client.py"
ssh root@107.175.185.21 "/opt/TelDriveManager/venv/bin/python -m py_compile /opt/TelDriveManager/app/modules/pikpak/client.py /opt/TelDriveManager/app/modules/pikpak/routes.py"
ssh root@107.175.185.21 "systemctl restart teldrive-manager.service && systemctl is-active teldrive-manager.service"
```

Expected: the service returns to `active` with the previous failure-closed client. Do not broaden cleanup, delete a possible default-directory copy, or automatically remove aria2 tasks during rollback; report the exact observed IDs and filenames for a separate decision.
