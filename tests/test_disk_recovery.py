import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiohttp import web

from app import database as db
from app.aria2_client import Aria2Client
from app.disk_budget import DiskSpaceUnavailable, disk_budget
from app.modules.aria2teldrive import disk_recovery, task_manager
from tests.test_serial_gate import FakeAria2


GB = 1024 ** 3


class RecoveryAria2(FakeAria2):
    async def tell_active(self):
        return [dict(item) for item in self.status_by_gid.values() if item['status'] == 'active']

    async def tell_waiting_all(self):
        return [dict(item) for item in self.status_by_gid.values() if item['status'] in ('waiting', 'paused')]

    async def force_pause(self, gid):
        await super().force_pause(gid)
        self.status_by_gid[gid]['status'] = 'paused'

    async def unpause(self, gid):
        await super().unpause(gid)
        self.status_by_gid[gid]['status'] = 'waiting'

    async def remove_for_recovery(self, gid):
        self.removed.append(gid)
        self.status_by_gid[gid]['status'] = 'removed'

    async def save_session(self):
        return 'OK'


class DiskRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await db.close_db()
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        db_path = patch.object(db, 'DB_PATH', self.root / 'tasks.db')
        db_path.start()
        self.addCleanup(db_path.stop)
        await db.init_db()
        self.addAsyncCleanup(db.close_db)
        self.manager = task_manager.TaskManager()
        self.manager.config = {
            'aria2': {'download_dir': str(self.root), 'disk_protection_threshold_gb': 5, 'max_concurrent': 3},
            'upload': {'serial_transfer_mode': False, 'auto_delete': True, 'max_retries': 3},
            'teldrive': {'upload_dir': '', 'target_path': '/', 'upload_concurrency': 2},
        }
        self.manager.aria2 = RecoveryAria2()
        self.manager._broadcast_task_update = AsyncMock()
        self.manager._has_serial_resume_blockers = AsyncMock(return_value=False)
        self.manager._auto_retry_disk_failed_downloads = AsyncMock()
        self.capacity = 14 * GB
        usage = patch('app.disk_budget.shutil.disk_usage', side_effect=lambda _: SimpleNamespace(
            total=self.capacity, used=self.used(), free=self.capacity - self.used()))
        usage.start()
        self.addCleanup(usage.stop)
        allocation = patch.object(disk_recovery, 'allocated_bytes', side_effect=lambda p: p.stat().st_size * 1024 ** 2)
        allocation.start()
        self.addCleanup(allocation.stop)
        source = patch.object(disk_recovery, 'http_file_size', AsyncMock(return_value=8 * GB))
        self.source = source.start()
        self.addCleanup(source.stop)
        disk_budget.retain('aria2:', set())
        self.addCleanup(disk_budget.retain, 'aria2:', set())
        self.addCleanup(disk_budget.retain, 'relay:', set())

    def used(self):
        return sum(path.stat().st_size for path in self.root.glob('*.bin')) * 1024 ** 2

    async def add_partial(self, name, completed=4, total=8, status='downloading'):
        path = self.root / (name + '.bin')
        path.write_bytes(b'x' * (completed * 1024))
        Path(str(path) + '.aria2').write_bytes(b'checkpoint')
        await db.add_task(name, 'https://example.invalid/' + name, path.name,
                          aria2_options_json=json.dumps({'dir': str(self.root), 'out': path.name}))
        await db.update_task(name, status=status, aria2_gid=name, local_path=str(path),
                             source_size_bytes=total * GB, download_progress=100 * completed / total)
        self.manager.aria2.status_by_gid[name] = dict(gid=name, dir=str(self.root), status='paused',
            totalLength=str(total * GB), completedLength=str(completed * GB),
            files=[{'path': str(path), 'length': str(total * GB), 'completedLength': str(completed * GB)}])
        if status != 'paused':
            self.manager._hold_gid_for_disk_gate(name)
        return path

    async def cycle(self):
        await self.manager._check_disk_usage()
        await self.manager._sync_disk_space_download_protection(
            await self.manager.aria2.tell_active(), await self.manager.aria2.tell_waiting_all())

    async def test_two_half_downloads_reclaim_one_and_finish_the_other(self):
        a = await self.add_partial('a')
        b = await self.add_partial('b')
        await self.cycle()
        self.assertEqual(self.manager.aria2.unpaused, ['a'])
        self.assertTrue(a.exists())
        self.assertFalse(b.exists())
        loser = await db.get_task('b')
        self.assertEqual((loser['status'], loser['aria2_gid'], loser['download_progress']), ('pending', None, 0))
        self.assertEqual(loser['source_size_bytes'], 8 * GB)
        self.assertEqual(loser['url'], 'https://example.invalid/b')
        self.assertFalse(self.manager._disk_protection_info['stalled'])
        a.write_bytes(b'x' * 8192)
        self.assertGreaterEqual(self.capacity - self.used(), 5 * GB)
        self.manager.aria2.status_by_gid['a']['status'] = 'complete'
        await db.update_task('a', status='completed', download_progress=100)
        await self.manager._auto_delete_local('a', str(a))
        disk_budget.release('aria2:a')
        await self.manager._dispatch_queued_parallel_downloads()
        queued = await db.get_task('b')
        self.assertTrue(queued['aria2_gid'])
        disk_budget.reserve('aria2:' + queued['aria2_gid'], self.root, 8 * GB, 5 * GB)

    async def test_reserve_blocks_relay_from_taking_survivor_space(self):
        await self.add_partial('a')
        await self.add_partial('b')
        finish = self.manager._disk_recovery.finish_journal
        async def finish_and_contend(*args):
            await finish(*args)
            with self.assertRaises(DiskSpaceUnavailable):
                disk_budget.reserve('relay:new', self.root, 2 * GB, 5 * GB)
        with patch.object(self.manager._disk_recovery, 'finish_journal', side_effect=finish_and_contend):
            await self.cycle()
        self.assertEqual(self.manager.aria2.unpaused, ['a'])

    async def test_insufficient_total_capacity_does_not_discard_partials(self):
        a = await self.add_partial('a')
        b = await self.add_partial('b')
        self.capacity = 10 * GB
        await self.cycle()
        self.assertTrue(a.exists() and b.exists())
        self.assertEqual(self.manager.aria2.removed, [])
        self.assertTrue(self.manager._disk_protection_info['stalled'])

    async def test_source_unavailable_or_changed_preserves_both_files(self):
        a = await self.add_partial('a')
        b = await self.add_partial('b')
        self.source.return_value = 7 * GB
        await self.cycle()
        self.assertTrue(a.exists() and b.exists())
        self.assertEqual(self.manager.aria2.removed, [])

    async def test_complete_or_manually_paused_file_is_never_a_donor(self):
        for status, completed in [('paused', 4), ('failed', 8), ('uploading', 8)]:
            with self.subTest(status=status):
                await db.delete_task('a')
                await db.delete_task('b')
                a = await self.add_partial('a')
                b = await self.add_partial('b', completed=completed, status=status)
                self.manager._disk_recovery.next_attempt = 0
                await self.cycle()
                self.assertTrue(a.exists() and b.exists())
                self.assertEqual(self.manager.aria2.removed, [])

    async def test_shared_pending_path_is_not_reclaimed(self):
        a = await self.add_partial('a')
        b = await self.add_partial('b')
        await db.add_task('other', 'https://example.invalid/shared', b.name,
                          aria2_options_json=json.dumps({'dir': str(self.root), 'out': b.name}))
        await self.cycle()
        self.assertTrue(a.exists() and b.exists())
        self.assertEqual(self.manager.aria2.removed, [])

    async def test_linked_cache_is_not_reclaimed(self):
        a = await self.add_partial('a')
        b = await self.add_partial('b')
        import os
        os.link(b, self.root / 'saved.dat')
        await self.cycle()
        self.assertTrue(a.exists() and b.exists())
        self.assertEqual(self.manager.aria2.removed, [])

    async def test_save_session_failure_preserves_files_then_restart_recovers(self):
        await self.add_partial('a')
        b = await self.add_partial('b')
        with patch.object(self.manager.aria2, 'save_session', AsyncMock(side_effect=ConnectionError('offline'))):
            await self.cycle()
        self.assertTrue(b.exists())
        self.assertNotEqual((await db.get_task('b'))['disk_recovery_json'], '{}')
        result = await self.manager.retry_task('b')
        self.assertFalse(result['success'])
        await db.close_db()
        await db.init_db()
        self.manager._disk_recovery = disk_recovery.DiskRecovery(self.manager)
        await self.manager._disk_recovery.resume_interrupted()
        self.assertFalse(b.exists())
        self.assertEqual((await db.get_task('b'))['disk_recovery_json'], '{}')
        await self.cycle()
        self.assertIn('a', self.manager.aria2.unpaused)

    async def test_manual_delete_can_finish_an_interrupted_recovery(self):
        await self.add_partial('a')
        b = await self.add_partial('b')
        with patch.object(self.manager.aria2, 'save_session', AsyncMock(side_effect=ConnectionError('offline'))):
            await self.cycle()
        self.assertTrue(b.exists())
        self.source.return_value = 0
        result = await self.manager.delete_task('b')
        self.assertTrue(result['success'])
        self.assertFalse(b.exists())
        self.assertIsNone(await db.get_task('b'))

    async def test_unfinished_journal_cannot_be_unpaused_by_normal_scheduler(self):
        await self.add_partial('a')
        await self.add_partial('b')
        self.source.side_effect = [8 * GB, 8 * GB, None]
        await self.cycle()
        self.assertNotEqual((await db.get_task('b'))['disk_recovery_json'], '{}')
        self.capacity = 100 * GB
        await self.cycle()
        await self.cycle()
        self.assertNotIn('b', self.manager.aria2.unpaused)

    async def test_failure_after_unlink_is_idempotent_on_restart(self):
        await self.add_partial('a')
        b = await self.add_partial('b')
        update = db.update_task
        async def fail_final(task_id, **fields):
            if fields.get('disk_recovery_json') == '{}':
                raise OSError('database unavailable')
            await update(task_id, **fields)
        with patch.object(db, 'update_task', side_effect=fail_final):
            await self.cycle()
        self.assertFalse(b.exists())
        self.assertEqual(json.loads((await db.get_task('b'))['disk_recovery_json'])['phase'], 'stopped')
        self.manager._disk_recovery = disk_recovery.DiskRecovery(self.manager)
        await self.manager._disk_recovery.resume_interrupted()
        self.assertEqual((await db.get_task('b'))['status'], 'pending')

    async def test_replacement_file_is_preserved_after_interruption(self):
        await self.add_partial('a')
        b = await self.add_partial('b')
        unlink = Path.unlink
        def deny(path, *args, **kwargs):
            if path == b:
                raise PermissionError('busy')
            return unlink(path, *args, **kwargs)
        with patch.object(Path, 'unlink', deny):
            await self.cycle()
        b.write_bytes(b'new independent data')
        self.manager._disk_recovery = disk_recovery.DiskRecovery(self.manager)
        await self.manager._disk_recovery.resume_interrupted()
        self.assertEqual(b.read_bytes(), b'new independent data')
        self.assertNotEqual((await db.get_task('b'))['disk_recovery_json'], '{}')

    async def test_unconfirmed_pause_never_reclaims_anything(self):
        a = await self.add_partial('a')
        b = await self.add_partial('b')
        self.manager.aria2.status_by_gid['a']['status'] = 'active'
        self.manager.aria2.force_pause = AsyncMock(side_effect=ConnectionError('offline'))
        self.manager.aria2.pause = AsyncMock(side_effect=ConnectionError('offline'))
        await self.cycle()
        self.assertTrue(a.exists() and b.exists())
        self.assertEqual(self.manager.aria2.removed, [])

    async def test_unknown_length_never_unpauses_until_metadata_is_known(self):
        await self.add_partial('a', completed=0)
        item = self.manager.aria2.status_by_gid['a']
        item['totalLength'] = '0'
        await db.update_task('a', source_size_bytes=0)
        with patch.object(task_manager, 'http_file_size', AsyncMock(return_value=None)):
            await self.cycle()
            await asyncio.gather(*self.manager._disk_size_probes.values())
        self.assertEqual(self.manager.aria2.unpaused, [])
        self.manager._disk_probe_retry_at.clear()
        with patch.object(task_manager, 'http_file_size', AsyncMock(return_value=8 * GB)):
            await self.cycle()
            await asyncio.gather(*self.manager._disk_size_probes.values())
        await self.cycle()
        self.assertEqual(self.manager.aria2.unpaused, ['a'])

    async def test_unpause_failure_keeps_gate_for_next_cycle(self):
        await self.add_partial('a', completed=0)
        with patch.object(self.manager.aria2, 'unpause', AsyncMock(side_effect=ConnectionError('offline'))):
            await self.cycle()
        self.assertIn('a', self.manager._disk_gate_paused_gids)
        await self.cycle()
        self.assertEqual(self.manager.aria2.unpaused, ['a'])

    async def test_verified_empty_http_file_does_not_wait_forever(self):
        await self.add_partial('a', completed=0)
        self.manager.aria2.status_by_gid['a']['totalLength'] = '0'
        await db.update_task('a', source_size_bytes=0)
        with patch.object(task_manager, 'http_file_size', AsyncMock(return_value=0)):
            await self.cycle()
            await asyncio.gather(*self.manager._disk_size_probes.values())
        await self.cycle()
        self.assertEqual(self.manager.aria2.unpaused, ['a'])

    async def test_independent_disk_guard_stops_owned_process_when_rpc_fails(self):
        from app.aria2_service import aria2_service
        self.capacity = 0
        self.manager.aria2.pause_all = AsyncMock(side_effect=ConnectionError('offline'))
        with patch.object(aria2_service, 'is_running', return_value=True), \
                patch.object(aria2_service, 'stop', AsyncMock()) as stop:
            await self.manager._check_disk_guard()
        stop.assert_awaited_once()
        self.assertIn('aria2', self.manager._disk_guard_error)

    async def test_rpc_pause_failure_never_kills_an_unowned_process(self):
        from app.aria2_service import aria2_service
        self.capacity = 0
        self.manager.aria2.pause_all = AsyncMock(side_effect=ConnectionError('offline'))
        with patch.object(aria2_service, 'is_running', return_value=False), \
                patch.object(aria2_service, 'stop', AsyncMock()) as stop:
            await self.manager._check_disk_guard()
        stop.assert_not_awaited()

    async def test_recovery_credentials_are_not_returned_to_ui(self):
        await self.add_partial('a')
        task = await db.get_task('a')
        task['disk_recovery_json'] = '{"options":{"header":"Authorization: private"}}'
        self.assertNotIn('disk_recovery_json', self.manager._merge_runtime_task_fields(task))

    async def test_serial_mode_can_reclaim_and_requeue_a_donor(self):
        self.manager.config['upload']['serial_transfer_mode'] = True
        await self.add_partial('a')
        b = await self.add_partial('b')
        await self.cycle()
        self.assertEqual(self.manager.aria2.unpaused, ['a'])
        self.assertFalse(b.exists())
        self.assertEqual((await db.get_task('b'))['status'], 'pending')


class MetadataAndPagingTests(unittest.IsolatedAsyncioTestCase):
    async def test_aria2_log_output_is_bounded_and_rotates(self):
        import io
        from logging.handlers import RotatingFileHandler
        from app.aria2_service import Aria2Service
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'aria2.log'
            handler = RotatingFileHandler(path, maxBytes=32768, backupCount=2, encoding='utf-8')
            try:
                Aria2Service._drain_process_log(SimpleNamespace(stdout=io.BytesIO(b'x' * 300000)), handler)
            finally:
                handler.close()
            logs = list(Path(folder).glob('aria2.log*'))
            self.assertEqual(len(logs), 3)
            self.assertTrue(all(log.stat().st_size <= 32768 for log in logs))

    async def test_waiting_list_is_not_truncated_at_one_thousand(self):
        client = Aria2Client()
        client.tell_waiting = AsyncMock(side_effect=[list(range(500)), list(range(500, 1000)), [1000]])
        self.assertEqual(len(await client.tell_waiting_all()), 1001)

    async def test_metadata_probe_handles_head_fallback_and_unknown_stream(self):
        requests = []
        async def handler(request):
            requests.append((request.method, request.headers.get('Range')))
            if request.path == '/empty':
                return web.Response(body=b'')
            if request.path == '/unknown':
                response = web.StreamResponse()
                await response.prepare(request)
                await response.write_eof()
                return response
            if request.method == 'HEAD':
                return web.Response(status=405)
            return web.Response(status=206, headers={'Content-Range': 'bytes 0-0/8192'}, body=b'x')
        app = web.Application()
        app.router.add_route('*', '/{name}', handler)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        url = 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])
        self.assertEqual(await disk_recovery.http_file_size(url + '/file', {}), 8192)
        self.assertEqual(requests[:2], [('HEAD', None), ('GET', 'bytes=0-0')])
        self.assertIsNone(await disk_recovery.http_file_size(url + '/unknown', {}))
        self.assertEqual(await disk_recovery.http_file_size(url + '/empty', {}), 0)


if __name__ == '__main__':
    unittest.main()
