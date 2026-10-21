"""Catalog tests use an isolated database and never import runtime config."""

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import aiosqlite
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, ChatPhotoEmpty, DocumentAttributeFilename, DocumentAttributeVideo

from app import database as db
from app.modules.tel2teldrive import catalog as module


CHANNEL_ID = -1000000000123


def channel():
    return Channel(id=123, title="Video channel", photo=ChatPhotoEmpty(),
                   date=datetime.now(timezone.utc), broadcast=True, access_hash=9,
                   username="videos", noforwards=True)


def message(message_id, *, video=True, ttl=None):
    attributes = [DocumentAttributeFilename(f"video_{message_id}.mp4")]
    if video:
        attributes.append(DocumentAttributeVideo(duration=12, w=1280, h=720))
    document = SimpleNamespace(id=message_id * 1000, size=12000,
        mime_type="video/mp4" if video else "audio/mp3", attributes=attributes)
    return SimpleNamespace(id=message_id, document=document, video=document if video else None,
        media=SimpleNamespace(document=document, ttl_seconds=ttl), date=datetime.now(timezone.utc),
        message="caption <unsafe>", noforwards=True, ttl_period=None)


class StrictLogger:
    def __init__(self):
        self.messages = []

    def warning(self, message):
        self.messages.append(message)


class Client:
    def __init__(self, messages):
        self.entity = channel()
        self.messages = {msg.id: msg for msg in messages}
        self.connected = True
        self.authorized = True
        self.calls = []
        self.iter_error = None
        self.fetch_started = None
        self.fetch_release = None
        self.fetch_error = None
        self.iter_started = None
        self.iter_release = None

    def is_connected(self):
        return self.connected

    async def is_user_authorized(self):
        return self.authorized

    async def get_entity(self, reference):
        return self.entity

    async def get_messages(self, entity, *, ids=None, limit=None):
        if ids is not None:
            if self.fetch_error:
                raise self.fetch_error
            if self.fetch_started:
                self.fetch_started.set()
                await self.fetch_release.wait()
            return [self.messages.get(mid) for mid in ids]
        return sorted(self.messages.values(), key=lambda msg: msg.id, reverse=True)[:limit]

    async def iter_messages(self, entity, **kwargs):
        self.calls.append(kwargs)
        if self.iter_started:
            self.iter_started.set()
            await self.iter_release.wait()
        candidates = sorted(self.messages.values(), key=lambda msg: msg.id, reverse=not kwargs.get("reverse", False))
        candidates = [msg for msg in candidates if msg.id > kwargs.get("min_id", 0)
                      and (not kwargs.get("offset_id") or msg.id < kwargs["offset_id"])]
        for item in candidates[:kwargs["limit"]]:
            yield item
            if self.iter_error:
                raise self.iter_error


class Relay:
    def __init__(self):
        self.calls = []
        self.configs = []
        self.failure = None
        self.after_enqueue = None

    async def enqueue_channel_video(self, config, msg, info, **kwargs):
        if self.failure:
            raise self.failure
        self.calls.append((msg, info, kwargs))
        self.configs.append(config)
        job_id = f"channel-{kwargs['source_channel_id']}-{msg.id}"
        async with aiosqlite.connect(str(db.DB_PATH)) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("""INSERT OR IGNORE INTO telegram_relay_jobs
                (job_id,source_channel_id,source_message_id,file_name,status,source_kind,target_path) VALUES (?,?,?,?,?,'channel',?)""",
                (job_id, kwargs["source_channel_id"], msg.id, info["name"], "pending", kwargs.get("target_path", "")))
            await conn.commit()
            if self.after_enqueue:
                self.after_enqueue()
            async with conn.execute("SELECT * FROM telegram_relay_jobs WHERE job_id=?", (job_id,)) as cursor:
                return dict(await cursor.fetchone())


class CatalogTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tdm-catalog-test-")
        self.path_patch = patch.object(db, "DB_PATH", Path(self.temp.name) / "catalog.db")
        self.path_patch.start()
        self.client = Client([message(mid) for mid in range(1, 4)])
        self.config = SimpleNamespace(relay_enabled=True, telegram_channel_id=999, teldrive_channel_id=999)
        self.relay = Relay()
        self.logger = StrictLogger()
        self.catalog = module.TelegramVideoCatalog(lambda: self.client, lambda: self.config,
                                                  self.relay, self.logger, None)

    async def asyncTearDown(self):
        await self.catalog.stop()
        self.path_patch.stop()
        self.temp.cleanup()

    async def add_and_scan(self):
        await self.catalog.add_channel("@videos")
        return await self.catalog.scan_channel(CHANNEL_ID)

    async def test_watch_is_metadata_only_and_protection_not_forwarded(self):
        scanned = await self.add_and_scan()
        self.assertEqual(scanned["added"], 3)
        self.assertEqual(self.relay.calls, [])
        items = (await self.catalog.list_videos(CHANNEL_ID))["videos"]
        self.assertEqual([item["message_id"] for item in items], [3, 2, 1])
        self.assertTrue(items[0]["protected"])
        self.assertEqual(items[0]["link"], "https://t.me/videos/3")
        self.assertEqual(items[0]["duration"], 12)
        self.assertFalse(any(key in items[0] for key in ("access_hash", "file_reference")))
        self.assertEqual(sorted(p.name for p in Path(self.temp.name).iterdir() if not p.name.endswith(("-wal", "-shm"))), ["catalog.db"])

    async def test_long_reconnect_gap_advances_from_oldest_unseen(self):
        self.client.messages = {mid: message(mid) for mid in range(1, 101)}
        await self.add_and_scan()
        self.client.messages.update({mid: message(mid) for mid in range(101, 351)})
        for expected in (200, 300, 350):
            result = await self.catalog.scan_channel(CHANNEL_ID)
            self.assertEqual(result["channel"]["newest_message_id"], expected)
            self.assertTrue(self.client.calls[-1]["reverse"])
        self.assertEqual((await self.catalog.list_channels())["channels"][0]["video_count"], 350)
        all_ids = await self.catalog._rows("SELECT message_id FROM telegram_catalog_videos ORDER BY message_id")
        self.assertEqual([row["message_id"] for row in all_ids], list(range(1, 351)))

    async def test_older_scan_and_pagination_keep_high_water_mark(self):
        self.client.messages = {mid: message(mid) for mid in range(1, 251)}
        first = await self.add_and_scan()
        self.assertEqual(first["channel"]["oldest_message_id"], 151)
        older = await self.catalog.scan_channel(CHANNEL_ID, older=True)
        self.assertEqual(older["channel"]["oldest_message_id"], 51)
        self.assertEqual(older["channel"]["newest_message_id"], 250)
        last = await self.catalog.scan_channel(CHANNEL_ID, older=True)
        self.assertTrue(last["channel"]["history_complete"])
        page = await self.catalog.list_videos(CHANNEL_ID, limit=40)
        next_page = await self.catalog.list_videos(CHANNEL_ID, before_id=page["next_before_id"], limit=40)
        self.assertEqual(len(page["videos"]), 40)
        self.assertTrue(set(v["message_id"] for v in page["videos"]).isdisjoint(v["message_id"] for v in next_page["videos"]))

    async def test_nonvideos_advance_cursor_too(self):
        self.client.messages = {mid: message(mid, video=False) for mid in range(1, 101)}
        scanned = await self.add_and_scan()
        self.assertEqual(scanned["added"], 0)
        self.assertEqual(scanned["channel"]["newest_message_id"], 100)
        self.client.messages[101] = message(101)
        self.assertEqual((await self.catalog.scan_channel(CHANNEL_ID))["added"], 1)

    async def test_interrupted_scan_does_not_commit_cursor_or_partial_metadata(self):
        await self.catalog.add_channel("videos")
        self.client.iter_error = ConnectionError("connection lost")
        with self.assertRaises(module.CatalogError) as caught:
            await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(caught.exception.status_code, 503)
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["newest_message_id"], 0)
        self.assertEqual(current["video_count"], 0)
        self.assertIsNotNone(current["last_error"])
        self.client.iter_error = None
        self.assertEqual((await self.catalog.scan_channel(CHANNEL_ID))["added"], 3)

    async def test_video_and_cursor_transaction_roll_back_together(self):
        await self.catalog.add_channel("videos")
        await self.catalog._write([("""CREATE TRIGGER reject_cursor BEFORE UPDATE OF newest_message_id
            ON telegram_catalog_channels BEGIN SELECT RAISE(ABORT, 'test failure'); END""", ())])
        with self.assertRaises(module.CatalogError):
            await self.catalog.scan_channel(CHANNEL_ID)
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["video_count"], 0)
        self.assertEqual(current["newest_message_id"], 0)

    async def test_flood_wait_persists_and_blocks_following_calls(self):
        await self.catalog.add_channel("videos")
        self.client.iter_error = FloodWaitError(None, capture=30)
        with self.assertRaises(module.CatalogError) as caught:
            await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(caught.exception.status_code, 429)
        calls_before = len(self.client.calls)
        await self.catalog.stop()
        self.client.iter_error = None
        with self.assertRaises(module.CatalogError) as again:
            await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(again.exception.status_code, 429)
        self.assertEqual(len(self.client.calls), calls_before)
        self.assertIsNotNone((await self.catalog.list_channels())["cooldown_until"])

    async def test_selected_media_is_refetched_and_unavailable_items_fail_individually(self):
        await self.add_and_scan()
        self.client.messages.pop(1)
        self.client.messages[2].media.ttl_seconds = 10
        response = await self.catalog.enqueue_videos(CHANNEL_ID, [1, 2, 3, 3, 999])
        self.assertEqual(response["queued"], 1)
        self.assertEqual(response["failed"], 3)
        self.assertEqual(len(self.relay.calls), 1)
        self.assertEqual(self.relay.calls[0][2]["source_channel_id"], CHANNEL_ID)
        self.assertEqual(self.relay.calls[0][2]["source_title"], "Video channel")
        await self.catalog.enqueue_videos(CHANNEL_ID, [3])
        rows = await self.catalog._rows("SELECT job_id FROM telegram_relay_jobs")
        self.assertEqual(len(rows), 1)
        videos = (await self.catalog.list_videos(CHANNEL_ID))["videos"]
        self.assertEqual(videos[0]["job_status"], "pending")

    async def test_failed_jobs_and_relay_validation_keep_actionable_errors(self):
        await self.add_and_scan()
        self.relay.failure = ValueError("该消息已有自动回源任务，请在回源任务列表中管理")
        failed = await self.catalog.enqueue_videos(CHANNEL_ID, [3])
        self.assertIn("自动回源", failed["results"][0]["error"])
        self.relay.failure = None
        await self.catalog.enqueue_videos(CHANNEL_ID, [3])
        await self.catalog._write([("UPDATE telegram_relay_jobs SET status='failed'", ())])
        failed = await self.catalog.enqueue_videos(CHANNEL_ID, [3])
        self.assertEqual(failed["queued"], 0)
        self.assertEqual(failed["failed"], 1)
        self.assertIn("重试", failed["results"][0]["error"])

    async def test_disabled_transfer_config_and_storage_channel_are_rejected(self):
        await self.add_and_scan()
        self.config.relay_enabled = False
        with self.assertRaises(module.CatalogError) as disabled:
            await self.catalog.enqueue_videos(CHANNEL_ID, [1])
        self.assertEqual(disabled.exception.status_code, 409)
        self.config.relay_enabled = True
        self.config.telegram_channel_id = 123
        with self.assertRaises(module.CatalogError) as own_storage:
            await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(own_storage.exception.status_code, 400)
        with self.assertRaises(module.CatalogError):
            await self.catalog.enqueue_videos(CHANNEL_ID, [1])
        self.assertEqual(self.relay.calls, [])

    async def test_pause_is_honored_when_poll_scan_waits_for_lock(self):
        await self.add_and_scan()
        await self.catalog.update_channel(CHANNEL_ID, False)
        calls_before = len(self.client.calls)
        await self.catalog._scan_channel(CHANNEL_ID, automatic=True)
        self.assertEqual(len(self.client.calls), calls_before)
        # A manual refresh remains available while automatic monitoring is paused.
        await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(len(self.client.calls), calls_before + 1)

    async def test_remove_waits_for_scan_and_cannot_resurrect_metadata_or_delete_jobs(self):
        await self.add_and_scan()
        await self.catalog.enqueue_videos(CHANNEL_ID, [1])
        self.client.iter_started, self.client.iter_release = asyncio.Event(), asyncio.Event()
        scan = asyncio.create_task(self.catalog.scan_channel(CHANNEL_ID))
        await self.client.iter_started.wait()
        remove = asyncio.create_task(self.catalog.remove_channel(CHANNEL_ID))
        await asyncio.sleep(0)
        self.assertFalse(remove.done())
        self.client.iter_release.set()
        await scan
        await remove
        self.assertEqual((await self.catalog.list_channels())["channels"], [])
        self.assertEqual(await self.catalog._rows("SELECT * FROM telegram_catalog_videos"), [])
        self.assertEqual(len(await self.catalog._rows("SELECT * FROM telegram_relay_jobs")), 1)
        with self.assertRaises(module.CatalogError):
            await self.catalog.scan_channel(CHANNEL_ID)

    async def test_stop_blocks_enqueue_which_was_waiting_for_telegram(self):
        await self.add_and_scan()
        self.client.fetch_started, self.client.fetch_release = asyncio.Event(), asyncio.Event()
        enqueue = asyncio.create_task(self.catalog.enqueue_videos(CHANNEL_ID, [1]))
        await self.client.fetch_started.wait()
        await self.catalog.stop()
        self.client.fetch_release.set()
        with self.assertRaises(module.CatalogError) as stopped:
            await enqueue
        self.assertEqual(stopped.exception.status_code, 409)
        self.assertEqual(self.relay.calls, [])

    async def test_hot_disable_during_fetch_does_not_restart_relay(self):
        await self.add_and_scan()
        self.client.fetch_started, self.client.fetch_release = asyncio.Event(), asyncio.Event()
        enqueue = asyncio.create_task(self.catalog.enqueue_videos(CHANNEL_ID, [1]))
        await self.client.fetch_started.wait()
        # RuntimeConfig instances are replaced when settings are applied.
        self.config = SimpleNamespace(relay_enabled=False, telegram_channel_id=999, teldrive_channel_id=999)
        self.client.fetch_release.set()
        with self.assertRaises(module.CatalogError) as disabled:
            await enqueue
        self.assertEqual(disabled.exception.status_code, 409)
        self.assertEqual(self.relay.calls, [])

    async def test_changed_storage_channel_during_fetch_is_revalidated(self):
        await self.add_and_scan()
        self.client.fetch_started, self.client.fetch_release = asyncio.Event(), asyncio.Event()
        enqueue = asyncio.create_task(self.catalog.enqueue_videos(CHANNEL_ID, [1]))
        await self.client.fetch_started.wait()
        self.config = SimpleNamespace(relay_enabled=True, telegram_channel_id=123, teldrive_channel_id=123)
        self.client.fetch_release.set()
        with self.assertRaises(module.CatalogError) as storage:
            await enqueue
        self.assertEqual(storage.exception.status_code, 400)
        self.assertEqual(self.relay.calls, [])

    async def test_enqueue_passes_latest_runtime_and_rechecks_between_items(self):
        await self.add_and_scan()
        self.client.fetch_started, self.client.fetch_release = asyncio.Event(), asyncio.Event()
        enqueue = asyncio.create_task(self.catalog.enqueue_videos(CHANNEL_ID, [1, 2]))
        await self.client.fetch_started.wait()
        latest = SimpleNamespace(relay_enabled=True, telegram_channel_id=999, teldrive_channel_id=999,
                                 relay_target_path="/new-destination")
        self.config = latest
        def disable_after_first():
            self.config = SimpleNamespace(relay_enabled=False, telegram_channel_id=999, teldrive_channel_id=999)
        self.relay.after_enqueue = disable_after_first
        self.client.fetch_release.set()
        result = await enqueue
        self.assertEqual(result["queued"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertIs(self.relay.configs[0], latest)
        self.assertIn("停用", result["results"][1]["error"])

    async def test_unknown_error_uses_single_argument_logger_and_keeps_stopping_safe(self):
        await self.catalog.add_channel("videos")
        self.client.iter_error = RuntimeError("unexpected")
        with self.assertRaises(module.CatalogError) as caught:
            await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(caught.exception.status_code, 502)
        self.assertIn("RuntimeError", self.logger.messages[0])
        async def failed_poller():
            raise RuntimeError("crashed")
        self.catalog._poll_task = asyncio.create_task(failed_poller())
        await asyncio.sleep(0)
        await self.catalog.stop()
        self.assertIsNone(self.catalog._conn)

    async def test_metadata_survives_restart_and_duplicate_add(self):
        await self.add_and_scan()
        await self.catalog.update_channel(CHANNEL_ID, False)
        await self.catalog.stop()
        await self.catalog.add_channel("https://t.me/videos/1")
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertFalse(current["enabled"])
        self.assertEqual(current["video_count"], 3)
        self.assertEqual(current["newest_message_id"], 3)

    async def test_cached_list_available_without_telegram_but_scans_rejected(self):
        await self.add_and_scan()
        self.client.connected = False
        self.assertEqual(len((await self.catalog.list_videos(CHANNEL_ID))["videos"]), 3)
        with self.assertRaises(module.CatalogError) as offline:
            await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(offline.exception.status_code, 409)

    async def test_stalled_telegram_scan_times_out_without_advancing_cursor(self):
        await self.catalog.add_channel("videos")
        self.client.iter_started, self.client.iter_release = asyncio.Event(), asyncio.Event()
        with patch.object(module, "TELEGRAM_TIMEOUT", 0.02):
            with self.assertRaises(module.CatalogError) as timeout:
                await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(timeout.exception.status_code, 503)
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["newest_message_id"], 0)
        self.assertEqual(current["video_count"], 0)

    async def enable_auto(self, since="2020-01-01T00:00:00Z", target_path="/Video archive"):
        return await self.catalog.update_channel(CHANNEL_ID, target_path=target_path, auto_sync=True, sync_since=since)

    async def test_channel_defaults_manual_and_settings_validate_without_data_loss(self):
        await self.add_and_scan()
        initial = (await self.catalog.list_channels())["channels"][0]
        self.assertFalse(initial["auto_sync"])
        self.assertEqual(initial["target_path"], "")
        self.assertEqual(initial["sync_status"], "manual")
        self.assertAlmostEqual(module._published_at(initial["sync_since"]), initial["sync_since_ts"], places=5)
        await self.catalog._poll_once()
        self.assertEqual(self.relay.calls, [])
        with self.assertRaises(module.CatalogError):
            await self.catalog.update_channel(CHANNEL_ID, auto_sync=True)
        for invalid in ("2026-01-01T00:00:00", "not-a-date"):
            with self.assertRaises(module.CatalogError):
                await self.catalog.update_channel(CHANNEL_ID, target_path="/Movies", auto_sync=True, sync_since=invalid)
        await self.catalog.update_channel(CHANNEL_ID, enabled=False)
        saved = await self.enable_auto(target_path=" Video archive//Shows/ ")
        self.assertTrue(saved["enabled"])
        self.assertEqual(saved["target_path"], "/Video archive/Shows")
        self.assertEqual(saved["auto_pending_count"], 3)
        saved = await self.catalog.update_channel(CHANNEL_ID, auto_sync=False)
        self.assertEqual(saved["video_count"], 3)
        self.assertEqual(saved["target_path"], "/Video archive/Shows")

    async def test_manual_transfer_uses_channel_path_or_current_global_fallback(self):
        await self.add_and_scan()
        self.config.relay_target_path = "/Global old"
        await self.catalog.update_channel(CHANNEL_ID, target_path="/Per channel")
        await self.catalog.enqueue_videos(CHANNEL_ID, [1])
        self.assertEqual(self.relay.calls[-1][2]["target_path"], "/Per channel")
        await self.catalog.update_channel(CHANNEL_ID, target_path="")
        self.config.relay_target_path = "/Global new"
        await self.catalog.enqueue_videos(CHANNEL_ID, [2])
        self.assertEqual(self.relay.calls[-1][2]["target_path"], "/Global new")

    async def test_automatic_selection_uses_inclusive_real_utc_publication_time(self):
        base = datetime(2026, 1, 2, tzinfo=timezone.utc)
        for mid, delta in ((1, -1), (2, 0), (3, 1)):
            self.client.messages[mid].date = base + timedelta(seconds=delta)
        await self.add_and_scan()
        saved = await self.enable_auto(since="2026-01-02T08:00:00+08:00")
        self.assertEqual(saved["sync_since"], "2026-01-02T00:00:00+00:00")
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual([call[0].id for call in self.relay.calls], [2, 3])
        self.assertTrue(all(call[2]["target_path"] == "/Video archive" for call in self.relay.calls))

    async def test_same_second_history_boundary_covers_more_than_two_pages(self):
        base = datetime(2026, 1, 2, tzinfo=timezone.utc)
        self.client.messages = {mid: message(mid) for mid in range(1, 253)}
        for mid, item in self.client.messages.items():
            item.date = base - timedelta(seconds=1) if mid == 1 else base
        await self.catalog.add_channel("videos")
        await self.enable_auto(since=base.isoformat())
        for _ in range(3):
            await self.catalog._poll_once()
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["video_count"], 252)
        self.assertFalse(self.catalog._needs_history(current))
        receipts = await self.catalog._rows("SELECT message_id FROM telegram_catalog_dispatches ORDER BY message_id")
        self.assertEqual([row["message_id"] for row in receipts], list(range(2, 253)))
        self.assertLessEqual(len(self.relay.calls), 60)

    async def test_auto_scan_recovers_large_gap_without_losing_pending_videos(self):
        base = datetime(2026, 1, 2, tzinfo=timezone.utc)
        self.client.messages = {mid: message(mid) for mid in range(1, 11)}
        for mid, item in self.client.messages.items():
            item.date = base + timedelta(seconds=mid)
        await self.add_and_scan()
        await self.enable_auto(since=base.isoformat())
        self.client.messages.update({mid: message(mid) for mid in range(11, 261)})
        for mid, item in self.client.messages.items():
            item.date = base + timedelta(seconds=mid)
        for _ in range(3):
            await self.catalog._poll_once()
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["newest_message_id"], 260)
        self.assertEqual(current["video_count"], 260)
        self.assertEqual(current["auto_pending_count"] + len(self.relay.calls), 260)

    async def test_pending_intents_survive_crash_between_scan_and_enqueue(self):
        await self.catalog.add_channel("videos")
        await self.enable_auto()
        await self.catalog.scan_channel(CHANNEL_ID)
        self.assertEqual(self.relay.calls, [])
        await self.catalog.stop()
        self.assertEqual((await self.catalog.list_channels())["channels"][0]["auto_pending_count"], 3)
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual(len(self.relay.calls), 3)

    async def test_receipt_survives_job_cleanup_restart_and_channel_readd(self):
        await self.add_and_scan()
        await self.enable_auto()
        await self.catalog._dispatch_auto(CHANNEL_ID)
        await self.catalog._write([("UPDATE telegram_relay_jobs SET status='completed'", ()),
                                   ("DELETE FROM telegram_relay_jobs", ())])
        await self.catalog.remove_channel(CHANNEL_ID)
        await self.catalog.stop()
        await self.add_and_scan()
        await self.enable_auto()
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual(len(self.relay.calls), 3)
        self.assertEqual(await self.catalog._rows("SELECT * FROM telegram_relay_jobs"), [])
        receipts = await self.catalog._rows("SELECT state,job_status FROM telegram_catalog_dispatches")
        self.assertTrue(all(row == {"state": "submitted", "job_status": "completed"} for row in receipts))

    async def test_enqueue_broadcast_failure_cannot_overwrite_committed_receipt(self):
        await self.add_and_scan()
        await self.enable_auto()
        def after_commit():
            raise RuntimeError("broadcast failed after commit")
        self.relay.after_enqueue = after_commit
        await self.catalog._dispatch_auto(CHANNEL_ID)
        receipts = await self.catalog._rows("SELECT state,attempts FROM telegram_catalog_dispatches")
        self.assertTrue(all(row == {"state": "submitted", "attempts": 0} for row in receipts))
        await self.catalog._write([("DELETE FROM telegram_relay_jobs", ())])
        self.relay.after_enqueue = None
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual(len(self.relay.calls), 3)

    async def test_automatic_backlog_cap_counts_other_transfer_queue(self):
        await self.add_and_scan()
        await self.enable_auto()
        await self.catalog._write([("CREATE TABLE tasks(task_id TEXT PRIMARY KEY,status TEXT)", ())] +
            [("INSERT INTO tasks VALUES (?, 'pending')", (str(index),)) for index in range(100)])
        self.assertEqual(await self.catalog._dispatch_auto(CHANNEL_ID), 0)
        self.assertEqual((await self.catalog.list_channels())["channels"][0]["sync_status"], "backpressure")
        await self.catalog._write([("UPDATE tasks SET status='completed' WHERE task_id='0'", ())])
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual(len(self.relay.calls), 1)
        self.assertEqual(await self.catalog._active_transfer_count(), 100)
        self.assertEqual((await self.catalog.list_channels())["channels"][0]["auto_pending_count"], 2)

    async def test_auto_disabled_globally_preserves_intents_and_has_visible_status(self):
        await self.add_and_scan()
        await self.enable_auto()
        self.config.relay_enabled = False
        await self.catalog._poll_once()
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["sync_status"], "relay_disabled")
        self.assertEqual(current["auto_pending_count"], 3)
        self.assertEqual(self.relay.calls, [])
        self.config.relay_enabled = True
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual(len(self.relay.calls), 3)

    async def test_turning_auto_off_preserves_existing_jobs_and_stops_later_dispatch(self):
        await self.add_and_scan()
        await self.enable_auto()
        await self.catalog._dispatch_auto(CHANNEL_ID, budget=1)
        await self.catalog.update_channel(CHANNEL_ID, auto_sync=False)
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual(len(self.relay.calls), 1)
        self.assertEqual(len(await self.catalog._rows("SELECT * FROM telegram_relay_jobs")), 1)
        self.assertEqual((await self.catalog.list_channels())["channels"][0]["auto_pending_count"], 2)

    async def test_permanently_missing_media_is_not_retried_automatically(self):
        await self.add_and_scan()
        await self.enable_auto()
        self.client.messages.pop(1)
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.client.messages[1] = message(1)
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual([call[0].id for call in self.relay.calls], [2, 3])
        self.assertEqual((await self.catalog.list_channels())["channels"][0]["auto_failed_count"], 1)
        # Explicit user selection can recover a formerly unavailable video.
        await self.catalog.enqueue_videos(CHANNEL_ID, [1])
        self.assertEqual([call[0].id for call in self.relay.calls], [2, 3, 1])

    async def test_transient_automatic_failures_have_persistent_bounded_retries(self):
        await self.add_and_scan()
        await self.enable_auto()
        self.client.fetch_error = ConnectionError("offline")
        await self.catalog._dispatch_auto(CHANNEL_ID)
        first = await self.catalog._rows("SELECT state,attempts,next_retry_at FROM telegram_catalog_dispatches")
        self.assertTrue(all(row["state"] == "pending" and row["attempts"] == 1 and row["next_retry_at"] > 0 for row in first))
        await self.catalog.stop()
        for _ in range(2):
            await self.catalog._write([("UPDATE telegram_catalog_dispatches SET next_retry_at=0", ())])
            await self.catalog._dispatch_auto(CHANNEL_ID)
        final = await self.catalog._rows("SELECT state,attempts FROM telegram_catalog_dispatches")
        self.assertTrue(all(row == {"state": "failed", "attempts": 3} for row in final))
        self.client.fetch_error = None
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual(self.relay.calls, [])

    async def test_since_change_rebuilds_eligible_pending_without_resetting_receipts(self):
        base = datetime(2026, 1, 2, tzinfo=timezone.utc)
        for mid, item in self.client.messages.items():
            item.date = base + timedelta(seconds=mid)
        await self.add_and_scan()
        await self.enable_auto(since=(base + timedelta(seconds=3)).isoformat())
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual([call[0].id for call in self.relay.calls], [3])
        await self.catalog.update_channel(CHANNEL_ID, sync_since=(base + timedelta(seconds=1)).isoformat())
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertEqual([call[0].id for call in self.relay.calls], [3, 1, 2])

    async def test_pending_intent_and_scan_cursor_roll_back_together(self):
        await self.catalog.add_channel("videos")
        await self.enable_auto()
        await self.catalog._write([("""CREATE TRIGGER reject_intent BEFORE INSERT ON telegram_catalog_dispatches
            BEGIN SELECT RAISE(ABORT,'cannot save intent'); END""", ())])
        with self.assertRaises(module.CatalogError):
            await self.catalog.scan_channel(CHANNEL_ID)
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["newest_message_id"], 0)
        self.assertEqual(current["video_count"], 0)
        self.assertEqual(current["auto_pending_count"], 0)

    async def test_catalog_migration_keeps_legacy_channel_manual_and_data_intact(self):
        # Recreate precisely the schema predating automatic channel sync.
        async with aiosqlite.connect(str(db.DB_PATH)) as conn:
            await conn.execute("""CREATE TABLE telegram_catalog_channels (
                channel_id INTEGER PRIMARY KEY,title TEXT NOT NULL,username TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,protected INTEGER NOT NULL DEFAULT 0,
                newest_message_id INTEGER NOT NULL DEFAULT 0,oldest_message_id INTEGER NOT NULL DEFAULT 0,
                history_complete INTEGER NOT NULL DEFAULT 0,last_scan TEXT,last_error TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")
            await conn.execute("""CREATE TABLE telegram_catalog_videos (
                channel_id INTEGER NOT NULL,message_id INTEGER NOT NULL,file_name TEXT NOT NULL,
                file_size INTEGER NOT NULL,mime_type TEXT NOT NULL,duration REAL NOT NULL DEFAULT 0,
                caption TEXT NOT NULL DEFAULT '',date TEXT,protected INTEGER NOT NULL DEFAULT 0,
                link TEXT NOT NULL DEFAULT '',PRIMARY KEY(channel_id,message_id))""")
            await conn.execute("INSERT INTO telegram_catalog_channels(channel_id,title,newest_message_id,oldest_message_id) VALUES (?,'Legacy',9,9)", (CHANNEL_ID,))
            await conn.execute("""INSERT INTO telegram_catalog_videos(channel_id,message_id,file_name,file_size,mime_type,date)
                VALUES (?,9,'legacy.mp4',128,'video/mp4','2026-01-02T08:00:00.123456+08:00')""", (CHANNEL_ID,))
            await conn.commit()
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertFalse(current["auto_sync"])
        self.assertEqual(current["video_count"], 1)
        self.assertEqual(current["newest_message_id"], 9)
        self.assertIsNotNone(current["sync_since_ts"])
        videos = (await self.catalog.list_videos(CHANNEL_ID))["videos"]
        self.assertEqual(videos[0]["published_at"], datetime(2026, 1, 2, microsecond=123456, tzinfo=timezone.utc).timestamp())
        self.assertEqual(await self.catalog._rows("SELECT * FROM telegram_catalog_dispatches"), [])

    async def test_each_poll_submits_only_twenty_videos(self):
        self.client.messages = {mid: message(mid) for mid in range(1, 46)}
        await self.catalog.add_channel("videos")
        await self.enable_auto()
        await self.catalog._poll_once()
        self.assertEqual(len(self.relay.calls), 20)
        await self.catalog._poll_once()
        self.assertEqual(len(self.relay.calls), 40)
        await self.catalog._poll_once()
        self.assertEqual(len(self.relay.calls), 45)

    async def test_backpressure_pauses_old_history_but_keeps_watching_new_messages(self):
        self.client.messages = {mid: message(mid) for mid in range(1, 251)}
        await self.add_and_scan()
        await self.enable_auto()
        await self.catalog._write([("CREATE TABLE tasks(task_id TEXT PRIMARY KEY,status TEXT)", ())] +
            [("INSERT INTO tasks VALUES (?, 'pending')", (str(index),)) for index in range(100)])
        self.client.messages[251] = message(251)
        await self.catalog._poll_once()
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["newest_message_id"], 251)
        self.assertEqual(current["oldest_message_id"], 151)
        self.assertEqual(current["sync_status"], "backpressure")
        await self.catalog._write([("UPDATE tasks SET status='completed'", ())])
        await self.catalog._poll_once()
        self.assertEqual((await self.catalog.list_channels())["channels"][0]["oldest_message_id"], 51)

    async def test_successful_manual_recovery_clears_resolved_sync_error(self):
        await self.add_and_scan()
        await self.enable_auto()
        self.client.messages.pop(1)
        await self.catalog._dispatch_auto(CHANNEL_ID)
        self.assertIsNotNone((await self.catalog.list_channels())["channels"][0]["last_sync_error"])
        self.client.messages[1] = message(1)
        await self.catalog.enqueue_videos(CHANNEL_ID, [1])
        await self.catalog._dispatch_auto(CHANNEL_ID)
        current = (await self.catalog.list_channels())["channels"][0]
        self.assertEqual(current["auto_failed_count"], 0)
        self.assertIsNone(current["last_sync_error"])
        self.assertNotEqual(current["sync_status"], "error")

    async def test_saving_auto_settings_does_not_resume_previously_paused_channel(self):
        await self.add_and_scan()
        await self.enable_auto()
        await self.catalog.update_channel(CHANNEL_ID, enabled=False)
        current = await self.catalog.update_channel(CHANNEL_ID, target_path="/Different folder", auto_sync=True,
                                                     sync_since="2021-01-01T00:00:00Z")
        self.assertFalse(current["enabled"])
        self.assertEqual(current["sync_status"], "paused")
        await self.catalog._poll_once()
        self.assertEqual(self.relay.calls, [])


class MetadataTests(unittest.TestCase):
    def test_teldrive_paths_are_cloud_paths_and_reject_unsafe_forms(self):
        self.assertEqual(module.normalize_target_path(" Shows//字幕/ "), "/Shows/字幕")
        self.assertEqual(module.normalize_target_path("/"), "/")
        self.assertEqual(module.normalize_target_path(""), "")
        for path in ("/a/../b", "D:\\media", "https://host/folder", "/bad\nname", "/./folder"):
            with self.assertRaises(module.CatalogError):
                module.normalize_target_path(path)
    def test_reference_formats_reject_invites_and_arbitrary_hosts(self):
        for reference in ("@videos", "videos", "https://t.me/videos/123", "t.me/s/videos/12"):
            self.assertEqual(module.parse_channel_reference(reference), "videos")
        for reference in (str(CHANNEL_ID), "https://t.me/c/123/1"):
            self.assertEqual(module.parse_channel_reference(reference), CHANNEL_ID)
        for reference in ("https://t.me/+secret", "https://t.me/joinchat/invite", "https://evil.test/videos", "123"):
            with self.assertRaises(module.CatalogError):
                module.parse_channel_reference(reference)

    def test_expiring_and_paid_media_never_become_download_candidates(self):
        self.assertIsNone(module.video_file_info(message(1, ttl=10)))
        ttl = message(1)
        ttl.ttl_period = 30
        self.assertIsNone(module.video_file_info(ttl))
        paid = message(1)
        paid.media = type("MessageMediaPaidMedia", (), {"document": paid.document})()
        self.assertIsNone(module.video_file_info(paid))
        self.assertIsNone(module.video_file_info(message(1, video=False)))

    def test_filename_paths_are_removed_before_queueing(self):
        item = message(1)
        item.document.attributes[0] = DocumentAttributeFilename("../../evil\\video.mp4")
        self.assertEqual(module.video_file_info(item)["name"], "video.mp4")


if __name__ == "__main__":
    unittest.main()
