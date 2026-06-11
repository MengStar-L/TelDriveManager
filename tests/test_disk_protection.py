"""磁盘闸门（disk gate）测试 — 替代旧"并发数封顶"磁盘保护的行为验证。

回归保护：旧实现仅把 max-concurrent-downloads 设为 max(1, 活跃数)，
- 从不暂停已活跃的下载 → 写满磁盘（No space left / Write disk cache flush）
- 下限 1 导致磁盘满时 aria2 仍逐个提升等待任务 → fallocate 失败连环灭队
"""

import unittest
from typing import Any, cast

from app.modules.aria2teldrive import task_manager as task_manager_module

from tests.test_serial_gate import FakeAria2

GB = 1024 ** 3


class DiskGateTests(unittest.IsolatedAsyncioTestCase):
    def make_manager(self, free_gb=10, threshold_gb=5, serial=False):
        manager = task_manager_module.TaskManager()
        manager.config = {
            "upload": {"serial_transfer_mode": serial, "auto_delete": True, "max_retries": 3},
            "aria2": {
                "max_concurrent": 3,
                "disk_protection_threshold_gb": threshold_gb,
                "download_dir": ".",
            },
            "teldrive": {
                "upload_concurrency": 4, "upload_dir": "",
                "target_path": "/", "chunk_size": "500M",
            },
        }
        manager._disk_usage_info = {"free": free_gb * GB}
        manager.aria2 = cast(Any, FakeAria2())

        # 默认 stub 自愈（依赖真实 DB）；自愈测试单独覆盖
        async def no_auto_retry():
            return None

        manager._auto_retry_disk_failed_downloads = cast(Any, no_auto_retry)
        return manager

    @staticmethod
    def item(gid, status, total_gb=0, completed_gb=0):
        return {
            "gid": gid,
            "status": status,
            "totalLength": str(int(total_gb * GB)),
            "completedLength": str(int(completed_gb * GB)),
        }

    # ── 挡新增：预算不足时暂停 waiting ──

    async def test_projected_shortfall_pauses_waiting_keeps_active(self):
        # free=10G, threshold=5G；active 还需 3G，waiting 还需 4G
        # projected = 10 - 3 - 4 = 3G < 5G → waiting 被暂停，active 不动
        manager = self.make_manager(free_gb=10, threshold_gb=5)

        await manager._sync_disk_space_download_protection(
            active=[self.item("a-1", "active", total_gb=5, completed_gb=2)],
            waiting=[self.item("w-1", "waiting", total_gb=4)],
        )

        self.assertEqual(manager.aria2.force_paused, ["w-1"])
        self.assertIn("w-1", manager._disk_gate_paused_gids)
        self.assertFalse(manager._disk_gate_paused_gids["w-1"])  # 非曾活跃
        self.assertNotIn("a-1", manager._disk_gate_paused_gids)
        self.assertTrue(manager._disk_protection_active)
        self.assertIn("已暂停 1 个下载", manager._disk_protection_info["message"])

    async def test_sufficient_budget_pauses_nothing(self):
        # free=20G, 总需求 7G，projected=13G > 5G → 不动作
        manager = self.make_manager(free_gb=20, threshold_gb=5)

        await manager._sync_disk_space_download_protection(
            active=[self.item("a-1", "active", total_gb=5, completed_gb=2)],
            waiting=[self.item("w-1", "waiting", total_gb=4)],
        )

        self.assertEqual(manager.aria2.force_paused, [])
        self.assertEqual(manager._disk_gate_paused_gids, {})
        self.assertFalse(manager._disk_protection_active)

    # ── 停活跃：实际剩余跌破阈值 ──

    async def test_free_below_threshold_pauses_active_too(self):
        manager = self.make_manager(free_gb=3, threshold_gb=5)

        await manager._sync_disk_space_download_protection(
            active=[self.item("a-1", "active", total_gb=8, completed_gb=1)],
            waiting=[self.item("w-1", "waiting", total_gb=2)],
        )

        self.assertIn("a-1", manager.aria2.force_paused)
        self.assertIn("w-1", manager.aria2.force_paused)
        self.assertTrue(manager._disk_gate_paused_gids["a-1"])   # 曾活跃
        self.assertFalse(manager._disk_gate_paused_gids["w-1"])
        self.assertTrue(manager._disk_protection_active)

    # ── 恢复（滞回） ──

    async def test_recovery_resumes_formerly_active_first(self):
        # resume_threshold = threshold+1 = 6G；free=7G ≥ 6G → 曾活跃放行
        manager = self.make_manager(free_gb=7, threshold_gb=5)
        manager._disk_protection_active = True
        manager._disk_gate_paused_gids = {"a-1": True}

        await manager._sync_disk_space_download_protection(
            active=[],
            waiting=[self.item("a-1", "paused", total_gb=8, completed_gb=7)],
        )

        self.assertEqual(manager.aria2.unpaused, ["a-1"])
        self.assertNotIn("a-1", manager._disk_gate_paused_gids)
        self.assertFalse(manager._disk_protection_active)

    async def test_recovery_below_resume_threshold_keeps_held(self):
        # free=5.5G < resume 6G（滞回带内）→ 不放行
        manager = self.make_manager(free_gb=5.5, threshold_gb=5)
        manager._disk_protection_active = True
        manager._disk_gate_paused_gids = {"a-1": True}

        await manager._sync_disk_space_download_protection(
            active=[],
            waiting=[self.item("a-1", "paused", total_gb=8, completed_gb=7)],
        )

        self.assertEqual(manager.aria2.unpaused, [])
        self.assertIn("a-1", manager._disk_gate_paused_gids)
        self.assertTrue(manager._disk_protection_active)

    async def test_recovery_releases_waiting_one_per_cycle_with_budget_check(self):
        # free=20G，两个等待任务各需 2G：一个周期只放行 1 个
        manager = self.make_manager(free_gb=20, threshold_gb=5)
        manager._disk_protection_active = True
        manager._disk_gate_paused_gids = {"w-1": False, "w-2": False}

        waiting = [
            self.item("w-1", "paused", total_gb=2),
            self.item("w-2", "paused", total_gb=2),
        ]
        await manager._sync_disk_space_download_protection(active=[], waiting=waiting)

        self.assertEqual(len(manager.aria2.unpaused), 1)
        self.assertEqual(len(manager._disk_gate_paused_gids), 1)
        self.assertTrue(manager._disk_protection_active)  # 还有持有 → 保护仍激活

    async def test_recovery_waiting_blocked_when_budget_insufficient(self):
        # free=7G ≥ resume 6G，但任务需 5G：7-5=2G < threshold 5G → 不放行
        manager = self.make_manager(free_gb=7, threshold_gb=5)
        manager._disk_protection_active = True
        manager._disk_gate_paused_gids = {"w-1": False}

        await manager._sync_disk_space_download_protection(
            active=[], waiting=[self.item("w-1", "paused", total_gb=5)],
        )

        self.assertEqual(manager.aria2.unpaused, [])
        self.assertIn("w-1", manager._disk_gate_paused_gids)

    # ── 状态可见性 ──

    async def test_disk_gate_held_gid_visible_as_pending(self):
        manager = self.make_manager()
        manager._disk_gate_paused_gids["g-1"] = False
        self.assertEqual(manager._visible_aria2_status("paused", "g-1"), "pending")
        self.assertEqual(manager._visible_aria2_status("paused", "other"), "paused")

    # ── 不再 ratchet 并发数 ──

    async def test_no_max_concurrent_ratchet(self):
        manager = self.make_manager(free_gb=3, threshold_gb=5)

        await manager._sync_disk_space_download_protection(
            active=[self.item("a-1", "active", total_gb=8, completed_gb=1)],
            waiting=[],
        )

        # 旧实现会改 max-concurrent-downloads，新实现完全不碰
        self.assertEqual(manager.aria2.global_option_changes, [])
        self.assertEqual(manager._get_effective_max_concurrent_downloads(), 3)

    # ── 闸门集合自清理 ──

    async def test_stale_gate_gids_are_cleaned(self):
        manager = self.make_manager(free_gb=20, threshold_gb=5)
        manager._disk_gate_paused_gids = {"gone-1": True}

        await manager._sync_disk_space_download_protection(active=[], waiting=[])

        self.assertEqual(manager._disk_gate_paused_gids, {})
        self.assertFalse(manager._disk_protection_active)

    # ── 准入控制 ──

    async def test_should_defer_new_downloads_follows_protection_state(self):
        manager = self.make_manager()
        self.assertFalse(manager.should_defer_new_downloads())
        manager._disk_protection_active = True
        self.assertTrue(manager.should_defer_new_downloads())
        # 串行模式下永不 defer（串行闸门接管）
        serial = self.make_manager(serial=True)
        serial._disk_protection_active = True
        self.assertFalse(serial.should_defer_new_downloads())

    async def test_hold_gids_for_disk_gate_registers_batch(self):
        manager = self.make_manager()
        manager.hold_gids_for_disk_gate(["g-1", "", None, "g-2"])
        self.assertEqual(set(manager._disk_gate_paused_gids), {"g-1", "g-2"})

    # ── 磁盘错误自愈 ──

    async def test_disk_failed_downloads_auto_retry_on_recovery(self):
        manager = self.make_manager(free_gb=10, threshold_gb=5)
        # 恢复真实自愈逻辑（make_manager 默认 stub 掉了）
        manager._auto_retry_disk_failed_downloads = (
            task_manager_module.TaskManager._auto_retry_disk_failed_downloads.__get__(manager)
        )
        manager._disk_protection_active = True  # 上一周期处于保护
        retried = []

        failed_task = {
            "task_id": "t-1", "status": "failed",
            "error": "aria2 错误 [9]: fallocate failed. cause: No space left on device",
        }
        unrelated_task = {
            "task_id": "t-2", "status": "failed",
            "error": "aria2 错误 [1]: connection refused",
        }

        original_get_all = task_manager_module.db.get_all_tasks
        try:
            async def fake_get_all():
                return [failed_task, unrelated_task]

            async def fake_retry(task_id):
                retried.append(task_id)
                return {"success": True}

            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
            manager.retry_task = cast(Any, fake_retry)

            # 空间充足 → 保护解除 → 触发自愈
            await manager._sync_disk_space_download_protection(active=[], waiting=[])
        finally:
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all

        self.assertEqual(retried, ["t-1"])  # 只重试磁盘错误任务
        self.assertEqual(manager._disk_failure_retry_counts["t-1"], 1)

    async def test_disk_failed_retry_capped_at_three(self):
        manager = self.make_manager()
        manager._auto_retry_disk_failed_downloads = (
            task_manager_module.TaskManager._auto_retry_disk_failed_downloads.__get__(manager)
        )
        manager._disk_failure_retry_counts["t-1"] = 3
        retried = []

        original_get_all = task_manager_module.db.get_all_tasks
        try:
            async def fake_get_all():
                return [{
                    "task_id": "t-1", "status": "failed",
                    "error": "No space left on device",
                }]

            async def fake_retry(task_id):
                retried.append(task_id)
                return {"success": True}

            cast(Any, task_manager_module.db).get_all_tasks = fake_get_all
            manager.retry_task = cast(Any, fake_retry)
            await manager._auto_retry_disk_failed_downloads()
        finally:
            cast(Any, task_manager_module.db).get_all_tasks = original_get_all

        self.assertEqual(retried, [])

    # ── serial 模式整体禁用 ──

    async def test_serial_mode_disables_disk_gate(self):
        manager = self.make_manager(free_gb=0, serial=True)

        await manager._sync_disk_space_download_protection(
            active=[self.item("a-1", "active", total_gb=8)],
            waiting=[self.item("w-1", "waiting", total_gb=4)],
        )

        self.assertEqual(manager.aria2.force_paused, [])
        self.assertEqual(manager._disk_gate_paused_gids, {})
        self.assertFalse(manager._disk_protection_active)


if __name__ == "__main__":
    unittest.main()
