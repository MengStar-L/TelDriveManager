"""Optional real aria2 integration; all traffic/files stay in a temporary sandbox."""

import asyncio
import os
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiohttp import web

from app import database as db
from app.aria2_client import Aria2Client
from app.disk_budget import disk_budget
from app.modules.aria2teldrive.task_manager import TaskManager


MB = 1024 ** 2


@unittest.skipUnless(os.environ.get('ARIA2_TEST_BINARY'), 'Set ARIA2_TEST_BINARY for real aria2 integration')
class RealAria2RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_reclaim_resume_and_session_restart_with_real_aria2(self):
        await db.close_db()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            downloads = root / 'downloads'
            downloads.mkdir()
            source = root / 'source.bin'
            payload = bytes(range(256)) * (8 * MB // 256)
            source.write_bytes(payload)
            async def serve(request):
                return web.FileResponse(source)
            app = web.Application()
            app.router.add_route('*', '/{name}', serve)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '127.0.0.1', 0)
            await site.start()
            http_port = site._server.sockets[0].getsockname()[1]
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                rpc_port = sock.getsockname()[1]
            session_file = root / 'aria2.session'
            session_file.touch()
            command = [os.environ['ARIA2_TEST_BINARY'], '--enable-rpc=true', '--rpc-listen-all=false',
                       '--rpc-secret=disk-recovery-test', f'--rpc-listen-port={rpc_port}',
                       f'--dir={downloads}', '--file-allocation=none', '--split=1',
                       '--max-connection-per-server=1', '--max-download-limit=2M',
                       '--pause=true', '--auto-save-interval=1', '--disk-cache=0',
                       '--enable-dht=false', '--enable-dht6=false', '--enable-peer-exchange=false',
                       f'--input-file={session_file}', f'--save-session={session_file}']
            process = None
            client = Aria2Client(f'http://127.0.0.1:{rpc_port}/jsonrpc', rpc_secret='disk-recovery-test')
            async def launch():
                proc = subprocess.Popen(command, cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                try:
                    for _ in range(100):
                        try:
                            await client.get_version()
                            return proc
                        except Exception:
                            await asyncio.sleep(0.05)
                    raise RuntimeError('Test aria2 did not start')
                except BaseException:
                    proc.terminate()
                    await asyncio.to_thread(proc.wait, 10)
                    raise
            async def wait_for(gid, predicate):
                for _ in range(400):
                    item = await client.tell_status(gid)
                    if predicate(item):
                        return item
                    await asyncio.sleep(0.05)
                self.fail('aria2 did not reach expected state')
            try:
                process = await launch()
                with patch.object(db, 'DB_PATH', root / 'tasks.db'):
                    await db.init_db()
                    try:
                        manager = TaskManager()
                        manager.config = {
                            'aria2': {'download_dir': str(downloads), 'disk_protection_threshold_gb': 5, 'max_concurrent': 2},
                            'upload': {'serial_transfer_mode': False, 'auto_delete': True},
                            'teldrive': {'upload_dir': '', 'target_path': '/'},
                        }
                        manager.aria2 = client
                        manager._get_disk_protection_threshold_bytes = lambda: 5 * MB
                        manager._broadcast_task_update = AsyncMock()
                        manager._auto_retry_disk_failed_downloads = AsyncMock()
                        paths = []
                        gids = []
                        for name in ('a', 'b'):
                            url = f'http://127.0.0.1:{http_port}/{name}'
                            path = downloads / (name + '.bin')
                            gid = await client.add_uri(url, {'out': path.name, 'pause': 'true'})
                            await db.add_task(name, url, path.name)
                            await db.update_task(name, aria2_gid=gid, source_size_bytes=len(payload), status='downloading',
                                                 local_path=str(path), download_progress=50)
                            await client.unpause(gid)
                            await wait_for(gid, lambda item: int(item['completedLength']) >= 4 * MB)
                            await client.force_pause(gid)
                            await wait_for(gid, lambda item: item['status'] == 'paused')
                            manager._hold_gid_for_disk_gate(gid)
                            paths.append(path)
                            gids.append(gid)
                        self.assertTrue(all(Path(str(path) + '.aria2').is_file() for path in paths))
                        def usage(_):
                            used = sum(path.stat().st_size for path in paths if path.exists())
                            return SimpleNamespace(total=14 * MB, used=used, free=14 * MB - used)
                        with patch('app.disk_budget.shutil.disk_usage', side_effect=usage):
                            await manager._check_disk_usage()
                            await manager._sync_disk_space_download_protection([], await client.tell_waiting_all())
                            self.assertEqual(len(await client.tell_waiting_all()) + len(await client.tell_active()), 1)
                            donor = next(name for name in ('a', 'b') if not (downloads / (name + '.bin')).exists())
                            donor_task = await db.get_task(donor)
                            self.assertEqual(donor_task['status'], 'pending')
                            self.assertIsNone(donor_task['aria2_gid'])
                            survivor = 'b' if donor == 'a' else 'a'
                            survivor_gid = (await db.get_task(survivor))['aria2_gid']
                            # Interrupt the survivor and check that --pause=true prevents writes at restart.
                            await client.force_pause(survivor_gid)
                            await client.save_session()
                            process.terminate()
                            await asyncio.to_thread(process.wait, 10)
                            await client.close()
                            process = await launch()
                            restored = await client.tell_waiting_all()
                            self.assertEqual(len(restored), 1)
                            self.assertEqual(restored[0]['status'], 'paused')
                            self.assertEqual(restored[0]['gid'], survivor_gid)
                            await manager._sync_disk_space_download_protection([], restored)
                            await wait_for(survivor_gid, lambda item: item['status'] == 'complete')
                            self.assertEqual((downloads / (survivor + '.bin')).read_bytes(), payload)
                            self.assertGreaterEqual(usage(None).free, 5 * MB)
                            await db.update_task(survivor, status='completed', download_progress=100)
                            await manager._auto_delete_local(survivor, str(downloads / (survivor + '.bin')))
                            disk_budget.release('aria2:' + survivor_gid)
                            await manager._dispatch_queued_parallel_downloads()
                            new_gid = (await db.get_task(donor))['aria2_gid']
                            await manager._sync_disk_space_download_protection([], await client.tell_waiting_all())
                            await wait_for(new_gid, lambda item: item['status'] == 'complete')
                            self.assertEqual((downloads / (donor + '.bin')).read_bytes(), payload)
                            self.assertGreaterEqual(usage(None).free, 5 * MB)
                    finally:
                        await db.close_db()
            finally:
                disk_budget.retain('aria2:', set())
                await client.close()
                if process and process.poll() is None:
                    process.terminate()
                    await asyncio.to_thread(process.wait, 10)
                await runner.cleanup()


if __name__ == '__main__':
    unittest.main()
