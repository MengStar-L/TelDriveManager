# Telegram Channel Cleanup Safety Implementation Plan

**Goal:** Replace the duplicate Telegram channel settings with one authoritative TelDrive channel, make upload-part cleanup fail closed, and persist/display one timestamped audit record for every Telegram deletion decision.

**Architecture:** Normalize legacy configuration at both global and tel2teldrive service boundaries so every runtime consumer uses `teldrive.channel_id`, while a conflict flag disables automatic Telegram deletion. Route all four deletion paths through one audited helper, and make upload-part cleanup compute deletable IDs only from fresh upload-session data minus final-file and active-file protected IDs. Reuse the existing SQLite `progress_logs` table and SSE broker for an isolated `telegram_deletions` stream and expose it as a third monitor view.

**Tech Stack:** Python 3.11+, FastAPI, Telethon, aiosqlite, vanilla JavaScript, HTML/CSS, pytest, Node.js syntax checks.

---

### Task 1: Canonical channel configuration and legacy migration

**Files:**
- Modify: `app/config.py:37-445`
- Modify: `app/modules/tel2teldrive/service.py:130-610`
- Create: `tests/test_telegram_channel_config.py`

- **Step 1: Write failing global configuration migration tests**

```python
def test_normalize_migrates_legacy_telegram_channel():
    raw = {"telegram": {"channel_id": -10012345}, "teldrive": {}}
    merged = config._deep_merge(config.DEFAULTS, raw)
    normalized = config._normalize_config(merged, raw)
    assert normalized["teldrive"]["channel_id"] == -10012345
    assert "channel_id" not in normalized["telegram"]
    assert normalized["_meta"]["telegram_channel_conflict"] is False


def test_normalize_accepts_equivalent_channel_forms():
    raw = {
        "telegram": {"channel_id": -10012345},
        "teldrive": {"channel_id": 12345},
    }
    normalized = config._normalize_config(config._deep_merge(config.DEFAULTS, raw), raw)
    assert normalized["teldrive"]["channel_id"] == 12345
    assert normalized["_meta"]["telegram_channel_conflict"] is False


def test_normalize_marks_conflicting_legacy_channels():
    raw = {
        "telegram": {"channel_id": -10012345},
        "teldrive": {"channel_id": -10067890},
    }
    normalized = config._normalize_config(config._deep_merge(config.DEFAULTS, raw), raw)
    assert normalized["_meta"]["telegram_channel_conflict"] is True
```

- **Step 2: Run the migration tests and confirm failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_channel_config.py -q`

Expected: FAIL because legacy channel migration and conflict metadata do not exist.

- **Step 3: Implement canonical normalization**

Add one channel comparison primitive and apply it before the merged defaults hide whether a field was present:

```python
def normalize_telegram_channel_id(value: Any) -> int | None:
    try:
        channel_id = int(value)
    except (TypeError, ValueError):
        return None
    return channel_id or None


def telegram_channel_ids_equivalent(left: Any, right: Any) -> bool:
    left_id = normalize_telegram_channel_id(left)
    right_id = normalize_telegram_channel_id(right)
    if left_id is None or right_id is None:
        return left_id is right_id
    return str(abs(left_id)).removeprefix("100") == str(abs(right_id)).removeprefix("100")
```

In `_normalize_config`, choose `raw.teldrive.channel_id` when present, otherwise migrate `raw.telegram.channel_id`; remove `telegram.channel_id` from the normalized data; set internal metadata containing `telegram_channel_conflict` and the two original values. In `save_config`, remove the internal metadata before TOML serialization and remove any legacy `telegram.channel_id` key.

- **Step 4: Add service-local migration tests**

```python
def test_config_store_uses_teldrive_channel_for_telegram_runtime(tmp_path):
    store = service.ConfigStore(tmp_path / "config.toml")
    runtime = store.runtime_from_payload({
        "telegram": {"api_id": 1, "api_hash": "hash"},
        "teldrive": {"api_host": "http://td", "access_token": "token", "channel_id": -10012345},
    })
    assert runtime.telegram_channel_id == -10012345
    assert runtime.teldrive_channel_id == -10012345
    assert runtime.telegram_channel_conflict is False


def test_config_store_conflict_disables_telegram_deletion(tmp_path):
    store = service.ConfigStore(tmp_path / "config.toml")
    runtime = store.runtime_from_payload({
        "telegram": {"api_id": 1, "api_hash": "hash", "channel_id": -100111},
        "teldrive": {"api_host": "http://td", "access_token": "token", "channel_id": -100222},
    })
    assert runtime.telegram_channel_conflict is True
    assert runtime.telegram_deletion_enabled is False
```

- **Step 5: Implement the service runtime contract**

Add `telegram_channel_conflict: bool` to `RuntimeConfig`, derive both runtime channel properties from normalized `teldrive.channel_id`, expose conflict metadata in `payload().meta`, and make `is_ready` false while a legacy conflict exists. `_collect_missing_fields` must require only the canonical channel. `_dump_toml` and `save()` must omit the legacy Telegram channel key.

- **Step 6: Run configuration tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_channel_config.py -q`

Expected: PASS.

- **Step 7: Commit the configuration boundary**

```bash
git add app/config.py app/modules/tel2teldrive/service.py tests/test_telegram_channel_config.py
git commit -m "fix: unify Telegram channel configuration"
```

### Task 2: Persistent Telegram deletion audit helper

**Files:**
- Modify: `app/modules/tel2teldrive/service.py:62-1150`
- Create: `tests/test_telegram_delete_audit.py`

- **Step 1: Write failing audit persistence tests**

```python
@pytest.mark.asyncio
async def test_delete_helper_persists_one_success_record(monkeypatch, runtime_config):
    client = AsyncMock()
    monkeypatch.setattr(service.db, "add_progress_log", AsyncMock(return_value={"id": 7}))
    await service.delete_telegram_messages_with_audit(
        client,
        runtime_config,
        [11, 12],
        reason="duplicate_incoming_message",
        file_names=["movie.mkv"],
    )
    client.delete_messages.assert_awaited_once_with(runtime_config.teldrive_channel_id, [11, 12])
    payload = service.db.add_progress_log.await_args.args[1]
    assert payload["status"] == "deleted"
    assert payload["message_ids"] == [11, 12]
    assert datetime.fromisoformat(payload["occurred_at"]).tzinfo is not None


@pytest.mark.asyncio
async def test_delete_helper_persists_one_failure_record(monkeypatch, runtime_config):
    client = AsyncMock()
    client.delete_messages.side_effect = RuntimeError("telegram unavailable")
    monkeypatch.setattr(service.db, "add_progress_log", AsyncMock(return_value={"id": 8}))
    result = await service.delete_telegram_messages_with_audit(
        client, runtime_config, [21], reason="relay_source_after_upload", job_id="job-1"
    )
    assert result is False
    assert service.db.add_progress_log.await_count == 1
    assert service.db.add_progress_log.await_args.args[1]["status"] == "failed"
```

- **Step 2: Run the audit tests and confirm failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_delete_audit.py -q`

Expected: FAIL because the audit helper and stream do not exist.

- **Step 3: Implement one audit record and one deletion entry point**

Add:

```python
TELEGRAM_DELETE_LOG_STREAM = "telegram_deletions"
TELEGRAM_DELETE_REASONS = {
    "duplicate_incoming_message",
    "relay_source_after_upload",
    "teldrive_file_removed",
    "upload_orphan_parts",
    "polluted_upload_parts",
}


async def record_telegram_delete_audit(
    *, status: str, reason: str, channel_id: int | None,
    message_ids: list[int], deleted_message_ids: list[int] | None = None,
    file_names: list[str] | None = None, file_ids: list[str] | None = None,
    task_id: str | None = None, job_id: str | None = None,
    upload_id: str | None = None, protected_final_ids: list[int] | None = None,
    protected_active_ids: list[int] | None = None, detail: str | None = None,
) -> dict:
    payload = {
        "occurred_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "status": status,
        "reason": reason,
        "channel_id": channel_id,
        "message_ids": sorted(set(message_ids)),
        "deleted_message_ids": sorted(set(deleted_message_ids or [])),
        "file_names": file_names or [],
        "file_ids": file_ids or [],
        "task_id": task_id,
        "job_id": job_id,
        "upload_id": upload_id,
        "protected_final_ids": sorted(set(protected_final_ids or [])),
        "protected_active_ids": sorted(set(protected_active_ids or [])),
        "detail": detail,
    }
    row = await db.add_progress_log(
        "telegram_delete", payload, stream=TELEGRAM_DELETE_LOG_STREAM,
        job_id=task_id or job_id, limit=500,
    )
    broker.publish({"type": "telegram_delete_audit", "payload": {**payload, "id": row.get("id")}})
    return payload
```

`delete_telegram_messages_with_audit` must normalize and deduplicate positive IDs, block on configuration conflict/invalid channel/disconnected client, call `remember_internal_deleted_message_ids` only immediately before a real Telegram call, and persist exactly one final `deleted`, `failed`, or `blocked` record.

- **Step 4: Run audit tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_delete_audit.py -q`

Expected: PASS with one DB record per helper call and no tokens or credentials in payloads.

- **Step 5: Commit the audit helper**

```bash
git add app/modules/tel2teldrive/service.py tests/test_telegram_delete_audit.py
git commit -m "feat: audit Telegram deletion decisions"
```

### Task 3: Route the three required deletion behaviors through the audited helper

**Files:**
- Modify: `app/modules/tel2teldrive/service.py:2015-2080,2500-2550`
- Modify: `app/modules/tel2teldrive/relay.py:600-665`
- Modify: `tests/test_t2td_delete_guard.py`
- Modify: `tests/test_telegram_relay_independent.py`

- **Step 1: Write failing regression assertions for all three reasons**

For deletion sync, duplicate incoming message, and relay source cleanup, patch `delete_telegram_messages_with_audit` with `AsyncMock(return_value=True)` and assert calls include respectively:

```python
reason="teldrive_file_removed"
reason="duplicate_incoming_message"
reason="relay_source_after_upload"
```

Also assert the canonical `runtime.teldrive_channel_id` is the channel attached to every audit context.

- **Step 2: Run the focused regressions and confirm failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_t2td_delete_guard.py tests/test_telegram_relay_independent.py -q`

Expected: FAIL because these paths still invoke Telethon directly.

- **Step 3: Replace all three direct calls**

Use the centralized helper at each call site. Relay cleanup must reject a source channel that is not equivalent to the canonical storage channel, recording a `blocked` result rather than deleting from the source channel. Preserve existing mapping updates, retry behavior, and local-file cleanup ordering.

- **Step 4: Run focused regressions**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_t2td_delete_guard.py tests/test_telegram_relay_independent.py tests/test_tel2teldrive_resilience.py -q`

Expected: PASS; no direct `delete_messages` calls remain outside the centralized helper.

- **Step 5: Verify the call-site invariant**

Run: `rg -n "delete_messages\(" app`

Expected: only the implementation inside `delete_telegram_messages_with_audit` calls Telethon's `delete_messages`.

- **Step 6: Commit required-path auditing**

```bash
git add app/modules/tel2teldrive/service.py app/modules/tel2teldrive/relay.py tests/test_t2td_delete_guard.py tests/test_telegram_relay_independent.py
git commit -m "feat: audit required Telegram cleanup paths"
```

### Task 4: Fail-closed upload orphan and polluted-session cleanup

**Files:**
- Modify: `app/modules/tel2teldrive/service.py:1490-1540`
- Modify: `app/modules/aria2teldrive/task_manager.py:2350-2545`
- Create: `tests/test_upload_part_cleanup_safety.py`
- Modify: `tests/test_serial_gate.py`

- **Step 1: Write failing protected-ID and query-failure tests**

```python
@pytest.mark.asyncio
async def test_orphan_cleanup_protects_final_and_active_part_ids(manager, monkeypatch):
    manager.teldrive._get_part_message_id.side_effect = lambda part: part["partId"]
    monkeypatch.setattr(service, "query_active_teldrive_part_ids", AsyncMock(return_value={30}))
    delete = AsyncMock(return_value=True)
    monkeypatch.setattr(service, "delete_telegram_messages_with_audit", delete)
    await manager._cleanup_orphan_parts("task-1", {
        "upload_id": "upload-1",
        "remote_parts": [{"partId": 20}],
        "orphan_parts": [{"partId": 20}, {"partId": 30}, {"partId": 40}],
    })
    assert delete.await_args.args[2] == [40]


@pytest.mark.asyncio
async def test_polluted_cleanup_blocks_when_live_fetch_fails(manager, monkeypatch):
    manager.teldrive.get_upload_parts.side_effect = RuntimeError("fetch failed")
    delete = AsyncMock()
    monkeypatch.setattr(service, "delete_telegram_messages_with_audit", delete)
    result = await manager.cleanup_polluted_upload("task-1")
    delete.assert_not_awaited()
    assert result["success"] is True


@pytest.mark.asyncio
async def test_cleanup_blocks_when_active_reference_query_fails(manager, monkeypatch):
    monkeypatch.setattr(service, "query_active_teldrive_part_ids", AsyncMock(return_value=None))
    delete = AsyncMock()
    monkeypatch.setattr(service, "delete_telegram_messages_with_audit", delete)
    await manager._cleanup_orphan_parts("task-1", upload_meta_with_one_orphan)
    delete.assert_not_awaited()
```

- **Step 2: Run cleanup safety tests and confirm failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_upload_part_cleanup_safety.py -q`

Expected: FAIL because cached parts can currently be deleted and active references are not checked.

- **Step 3: Add tri-state active-reference lookup**

Implement `query_active_teldrive_part_ids(config) -> set[int] | None`. Return a set only after a successful DB query; return `None` for unavailable DB, connection errors, malformed parts, or a configuration conflict. Filter rows to the canonical channel using the existing channel equivalence helper.

- **Step 4: Implement protected-set cleanup**

For successful-upload orphan cleanup, compute:

```python
candidate_ids = orphan_ids
deletable_ids = candidate_ids - final_remote_ids - active_reference_ids
```

For polluted cleanup, require a non-empty current `upload_id` and a successful fresh `get_upload_parts(upload_id)` call; persisted or in-memory parts may be recorded for diagnosis but can never authorize deletion. If active references are unknown, or the fresh response cannot be tied to the current session, call `record_telegram_delete_audit(status="blocked", ...)`, persist candidates to `upload_orphans`, reset the polluted session, and continue the existing retry flow without calling Telegram.

- **Step 5: Run cleanup and upload regressions**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_upload_part_cleanup_safety.py tests/test_serial_gate.py tests/test_upload_integrity.py tests/test_upload_fingerprint.py -q`

Expected: PASS; a verified orphan is deleted, protected IDs are retained, and all evidence failures yield zero Telegram deletion calls.

- **Step 6: Commit fail-closed cleanup**

```bash
git add app/modules/tel2teldrive/service.py app/modules/aria2teldrive/task_manager.py tests/test_upload_part_cleanup_safety.py tests/test_serial_gate.py
git commit -m "fix: fail closed when cleaning Telegram upload parts"
```

### Task 5: Telegram audit query and isolated clear API

**Files:**
- Modify: `app/modules/tel2teldrive/routes.py:239-285`
- Extend: `tests/test_telegram_delete_audit.py`

- **Step 1: Write failing route tests**

```python
def test_get_telegram_delete_logs_returns_audit_items(client, seeded_audit_log):
    response = client.get("/api/t2td/telegram-delete-logs?limit=50")
    assert response.status_code == 200
    assert response.json()["items"][0]["status"] == "deleted"


def test_clear_telegram_delete_logs_is_stream_isolated(client, seeded_audit_log, seeded_teldrive_log):
    response = client.delete("/api/t2td/telegram-delete-logs")
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert client.get("/api/t2td/deleted-files").json()["count"] == 1
```

- **Step 2: Run route tests and confirm failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_delete_audit.py -q`

Expected: FAIL with HTTP 404 for the new endpoints.

- **Step 3: Add the two endpoints**

Add `GET /api/t2td/telegram-delete-logs` with bounded newest-first results from `TELEGRAM_DELETE_LOG_STREAM`, and `DELETE /api/t2td/telegram-delete-logs` that calls only `clear_progress_logs(stream=TELEGRAM_DELETE_LOG_STREAM)`. Return the stored structured payload plus `id`, `job_id`, and `created_at`.

- **Step 4: Run API and database tests**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_delete_audit.py -q`

Expected: PASS; clearing Telegram audits leaves TelDrive action logs intact.

- **Step 5: Commit audit API**

```bash
git add app/modules/tel2teldrive/routes.py tests/test_telegram_delete_audit.py
git commit -m "feat: expose Telegram deletion audit logs"
```

### Task 6: Single channel input and three-mode monitor UI

**Files:**
- Modify: `app/static/index.html:1625-1675,1930-2000,2285-2325`
- Modify: `app/static/app.js:120-140,265-285,645-680,3165-3200,3260-3290,3540-3785,4595-4615`
- Modify: `config.example.toml:60-90`
- Modify: `README.md`
- Create: `tests/test_telegram_monitor_ui.py`

- **Step 1: Write failing static UI contract tests**

```python
def test_settings_and_wizard_have_one_channel_input():
    html = Path("app/static/index.html").read_text(encoding="utf-8")
    assert html.count('id="cfgTeldriveChannel"') == 1
    assert 'id="cfgTelegramChannelId"' not in html
    assert html.count('id="wTdChannel"') == 1
    assert 'id="wTgChannel"' not in html


def test_monitor_exposes_three_modes():
    html = Path("app/static/index.html").read_text(encoding="utf-8")
    assert 'data-t2td-mode="logs"' in html
    assert 'data-t2td-mode="deleted"' in html
    assert 'data-t2td-mode="telegram-deletions"' in html
```

- **Step 2: Run the UI contract tests and confirm failure**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_monitor_ui.py -q`

Expected: FAIL because duplicate channel inputs and a two-state toggle still exist.

- **Step 3: Replace duplicate channel controls**

Label `cfgTeldriveChannel` and `wTdChannel` as `Telegram 存储/监听频道 ID`. Remove the duplicate Telegram channel inputs. Use `teldrive.channel_id` for Telegram readiness, Telegram connection testing, wizard collection, settings collection, and settings fill. Do not emit `telegram.channel_id` in browser payloads.

- **Step 4: Implement the three-mode segmented control**

Replace `t2tdViewToggleBtn` with three buttons calling `setT2TDPanelMode('logs')`, `setT2TDPanelMode('deleted')`, and `setT2TDPanelMode('telegram-deletions')`. Add `t2tdTelegramDeleteLogs`, load/render/clear functions for `/api/t2td/telegram-delete-logs`, status labels for `deleted`/`failed`/`blocked`, Chinese reason labels for all five stable reason codes, and a formatter that always returns `YYYY-MM-DD HH:mm:ss`.

Handle the SSE event directly:

```javascript
} else if (data.type === 'telegram_delete_audit') {
    upsertT2TDTelegramDeleteLog(data.payload || data);
}
```

Show `t2tdClearDeletedBtn` only for TelDrive deletion logs and a new `t2tdClearTelegramDeleteBtn` only for Telegram deletion logs. Each clear function must call only its matching audit endpoint.

- **Step 5: Update examples and documentation**

Remove `channel_id` from `[telegram]` in `config.example.toml` and README examples. Document `[teldrive].channel_id` as the shared Telegram storage/listener channel and state that conflicting legacy values block automatic deletion until the single setting is saved.

- **Step 6: Run static checks**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_telegram_monitor_ui.py tests/test_telegram_channel_config.py -q`

Expected: PASS.

Run: `node --check app/static/app.js`

Expected: exit code 0 with no syntax errors.

- **Step 7: Commit the UI and documentation**

```bash
git add app/static/index.html app/static/app.js config.example.toml README.md tests/test_telegram_monitor_ui.py
git commit -m "feat: add Telegram deletion audit view"
```

### Task 7: Full regression and browser verification

**Files:**
- Verify: `app/`
- Verify: `tests/`

- **Step 1: Run the full Python suite**

Run: `.\.venv\Scripts\python.exe -m pytest tests -q`

Expected: all tests PASS.

- **Step 2: Recheck direct deletion calls and saved config shape**

Run: `rg -n "delete_messages\(" app`

Expected: one direct Telethon call inside the audited helper.

Run: `rg -n "channel_id" config.example.toml README.md`

Expected: documentation contains the shared `[teldrive].channel_id` only; unrelated channel identifiers such as relay source metadata may remain.

- **Step 3: Start the local application**

Run: `.\.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8892`

Expected: server stays running and `http://127.0.0.1:8892/` returns the application.

- **Step 4: Verify desktop and mobile monitor behavior**

Using the in-app browser, open `http://127.0.0.1:8892/`, authenticate if the local configuration requires it, and verify at 1440x900 and 390x844:

1. Settings and setup wizard each present one shared channel field.
2. The Telegram monitor header has three mutually exclusive modes without overlap.
3. Telegram deletion entries show status, reason, channel, IDs, and full local timestamp.
4. A synthetic audit row received through SSE appears without a full page refresh.
5. Clearing Telegram deletion logs does not change TelDrive deletion logs and does not call a Telegram or TelDrive deletion endpoint.

- **Step 5: Inspect browser console and final diff**

Expected: no JavaScript errors, no blank monitor panel, no clipped labels at either viewport, and no credentials in audit payloads.

Run: `git diff --check`

Expected: exit code 0.

Run: `git status --short`

Expected: only the planned implementation, tests, spec, and plan are modified or untracked.
