import asyncio
import os
import socket
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from contextlib import ExitStack, redirect_stdout
from io import StringIO

from aiohttp import web
from app import aria2_service as aria_module
from app.modules.aria2teldrive.task_manager import TaskManager
from app.modules.tel2teldrive import service as telegram
from app.routes import update


class RuntimeHealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_signal_wait_leaves_no_orphan_tasks(self):
        service = SimpleNamespace(stop_event=asyncio.Event(), reload_event=asyncio.Event())
        before = asyncio.all_tasks()
        waiter = asyncio.create_task(telegram.Tel2TelDriveService._wait_for_signal(service))
        await asyncio.sleep(0)
        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_dead_aria2_blocks_update_even_with_ready_web(self):
        aria = aria_module.Aria2Service()
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ready=True)))
        with patch.object(aria_module, 'load_config', return_value={'aria2': {'installed': True}}), \
                patch.object(aria, 'is_installed', return_value=True), \
                patch.object(update, 'aria2_service', aria), \
                patch.object(update, 'verify_worker', return_value={'token': 'health-test'}), \
                patch.dict(os.environ, {'TDM_UPDATE_TOKEN': 'health-test'}):
            self.assertFalse((await update.update_health(request))['ready'])
            aria._process = Mock(poll=Mock(return_value=None))
            client = SimpleNamespace(get_version=AsyncMock(return_value={'version': 'test'}), close=AsyncMock())
            with patch.object(aria, '_build_client', return_value=client):
                self.assertTrue((await update.update_health(request))['ready'])
                client.get_version.side_effect = ConnectionError('offline')
                self.assertFalse((await update.update_health(request))['ready'])
                self.assertEqual(client.close.await_count, 2)

    async def test_rpc_failure_is_visible_bounded_and_recovers(self):
        manager = TaskManager()
        manager._disk_recovery.resume_interrupted = AsyncMock()
        manager.aria2 = SimpleNamespace(tell_active=AsyncMock(side_effect=ConnectionError('offline')),
                                       pause_all=AsyncMock(side_effect=ConnectionError('offline')))
        with self.assertLogs('app.modules.aria2teldrive.task_manager', level='WARNING') as logs:
            for _ in range(10):
                await manager._sync_aria2_tasks()
        self.assertEqual(manager.aria2.tell_active.await_count, 1)
        self.assertEqual(len(logs.output), 1)
        self.assertFalse(manager.get_global_stat()['aria2']['connected'])
        self.assertTrue(manager.get_global_stat()['download_protection']['active'])
        manager._aria2_poll_after = 0
        manager.aria2.tell_active.side_effect = None
        manager.aria2.tell_active.return_value = []
        manager.aria2.tell_waiting_all = AsyncMock(return_value=[])
        manager.aria2.tell_stopped_all = AsyncMock(return_value=[])
        with patch.object(manager, '_sync_disk_space_download_protection', AsyncMock()), \
                patch.object(manager, '_normalize_serial_pending_aria2_tasks', AsyncMock(return_value=set())), \
                patch.object(manager, '_sync_serial_transfer_gate', AsyncMock()), \
                patch.object(manager, '_dispatch_next_serial_download', AsyncMock()), \
                patch.object(manager, '_dispatch_queued_parallel_downloads', AsyncMock()):
            await manager._sync_aria2_tasks()
        self.assertTrue(manager.get_global_stat()['aria2']['connected'])
        self.assertFalse(manager._aria2_rpc_error)

    async def test_message_query_timeout_returns_unknown_not_missing(self):
        client = SimpleNamespace(get_messages=AsyncMock(side_effect=asyncio.TimeoutError()))
        with patch.object(telegram, 'logger', Mock()):
            self.assertIsNone(await telegram.get_existing_message_ids(client, 123, [1, 2]))

    async def test_unchanged_snapshots_do_not_repeat_telegram_queries(self):
        rounds = 0
        async def sleep(_):
            nonlocal rounds
            rounds += 1
            if rounds > 5:
                raise asyncio.CancelledError()
        config = SimpleNamespace(telegram_channel_id=123, sync_enabled=True, db_enabled=False,
                                 sync_interval=10, confirm_cycles=3)
        for response in ({101}, None):
            rounds = 0
            with patch.object(telegram.asyncio, 'sleep', sleep), \
                    patch.object(telegram, 'logger', Mock()), \
                    patch.object(telegram, 'get_teldrive_files', return_value={'file': {'name': 'file.bin'}}), \
                    patch.object(telegram, 'load_mapping', return_value={'file': [101]}), \
                    patch.object(telegram, 'get_existing_message_ids', AsyncMock(return_value=response)) as query, \
                    patch.object(telegram, 'delete_teldrive_files_for_missing_messages', AsyncMock()) as delete:
                with self.assertRaises(asyncio.CancelledError):
                    await telegram.sync_deletions(object(), config)
            self.assertEqual(query.await_count, 1)
            delete.assert_not_awaited()


class ActivityLogTests(unittest.TestCase):
    def test_rotates_and_writes_real_newlines(self):
        with tempfile.TemporaryDirectory() as folder, redirect_stdout(StringIO()):
            path = Path(folder) / 'runtime.log'
            broker = Mock()
            logger = telegram.ActivityLogger(broker, path)
            logger._handler.maxBytes = 1024
            try:
                for i in range(80):
                    logger.info(f'{i} ' + 'x' * 300)
                logs = list(Path(folder).glob('runtime.log*'))
                self.assertEqual(len(logs), 4)
                self.assertTrue(all(p.stat().st_size <= 1024 for p in logs))
                self.assertIn(b'\n', path.read_bytes())
                self.assertNotIn(b'\\n', path.read_bytes())
                with patch.object(logger._handler, 'shouldRollover', side_effect=OSError(28, 'full')):
                    logger.warning('disk full')
                self.assertEqual(broker.push_log.call_args.args[0]['message'], 'disk full')
            finally:
                logger.close()


@unittest.skipUnless(os.environ.get('ARIA2_TEST_BINARY'), 'Set ARIA2_TEST_BINARY for managed aria2 integration')
class ManagedAria2Tests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_service_start_download_pause_restart_and_health(self):
        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            root = Path(folder).resolve()
            downloads = root / 'downloads'
            aria_home = root / 'aria2'
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            cfg = {'aria2': {'installed': True, 'binary_path': os.environ['ARIA2_TEST_BINARY'],
                             'rpc_url': 'http://127.0.0.1', 'rpc_port': port, 'rpc_secret': 'managed-test'}}
            for name, value in {'load_config': lambda **kw: cfg, 'FIXED_DOWNLOAD_DIR': str(downloads),
                                'ARIA2_HOME': aria_home, 'ARIA2_TMP_DIR': aria_home / 'tmp',
                                'ARIA2_SESSION_FILE': aria_home / 'aria2.session',
                                'ARIA2_LOG_FILE': aria_home / 'aria2.log'}.items():
                stack.enter_context(patch.object(aria_module, name, value))
            payload = bytes(range(256)) * 8192
            async def serve(request):
                return web.Response(body=payload)
            app = web.Application()
            app.router.add_get('/fixture', serve)
            runner = web.AppRunner(app)
            await runner.setup()
            self.addAsyncCleanup(runner.cleanup)
            site = web.TCPSite(runner, '127.0.0.1', 0)
            await site.start()
            http_port = site._server.sockets[0].getsockname()[1]
            aria = aria_module.Aria2Service()
            client = aria._build_client(cfg)
            if os.name != 'nt':
                # Reproduce minimal systemd environments with the actual binary.
                stack.enter_context(patch.dict(os.environ, {k: v for k, v in os.environ.items() if k != 'HOME'}, clear=True))
            try:
                with patch.object(aria_module.subprocess, 'Popen', side_effect=OSError('injected launch failure')):
                    with self.assertRaises(OSError):
                        await aria.start()
                self.assertIn('injected launch failure', (await aria.get_runtime_status())['error'])
                await aria.start()
                self.assertTrue((await aria.update_health())['ready'])
                gid = await client.add_uri(f'http://127.0.0.1:{http_port}/fixture', {'out': 'fixture.bin', 'pause': 'true'})
                await client.save_session()
                await aria.restart()
                self.assertEqual((await client.tell_status(gid))['status'], 'paused')
                await client.unpause(gid)
                for _ in range(200):
                    if (await client.tell_status(gid))['status'] == 'complete':
                        break
                    await asyncio.sleep(0.05)
                self.assertEqual((downloads / 'fixture.bin').read_bytes(), payload)
                await aria.stop()
                self.assertFalse((await aria.update_health())['ready'])
            finally:
                await client.close()
                await aria.stop()
