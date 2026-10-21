import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.modules.tel2teldrive import routes
from app.modules.tel2teldrive.catalog import CatalogError


class ChannelRouteTests(unittest.TestCase):
    def setUp(self):
        self.catalog = SimpleNamespace(**{
            name: AsyncMock(return_value={}) for name in (
                "list_channels", "add_channel", "update_channel", "remove_channel",
                "list_videos", "scan_channel", "enqueue_videos")})
        self.deps = patch.object(routes, "_get_deps", return_value=(
            SimpleNamespace(video_catalog=self.catalog), None, None, Mock()))
        self.deps.start()
        self.addCleanup(self.deps.stop)
        app = FastAPI()
        app.include_router(routes.router)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def test_add_and_pause_explicit_channel(self):
        self.catalog.add_channel.return_value = {"channel_id": -100123, "title": "Videos"}
        response = self.client.post("/api/t2td/channels", json={"reference": "@videos"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["channel"]["channel_id"], -100123)
        self.catalog.add_channel.assert_awaited_once_with("@videos")
        self.client.patch("/api/t2td/channels/-100123", json={"enabled": False})
        self.catalog.update_channel.assert_awaited_once_with(-100123, False)

    def test_pagination_and_scan_keep_channel_identity(self):
        response = self.client.get("/api/t2td/channels/-100123/videos?before_id=150&limit=20")
        self.assertEqual(response.status_code, 200)
        self.catalog.list_videos.assert_awaited_once_with(-100123, before_id=150, limit=20)
        self.client.post("/api/t2td/channels/-100123/scan", json={"older": True})
        self.catalog.scan_channel.assert_awaited_once_with(-100123, older=True)

    def test_download_only_submitted_ids_and_preserve_partial_result(self):
        expected = {"queued": 1, "failed": 1, "results": [
            {"message_id": 11, "success": True, "job_id": "job"},
            {"message_id": 12, "success": False, "error": "源消息已删除"}]}
        self.catalog.enqueue_videos.return_value = expected
        response = self.client.post("/api/t2td/channels/-100123/download", json={"message_ids": [11, 12]})
        self.assertEqual(response.json(), expected)
        self.catalog.enqueue_videos.assert_awaited_once_with(-100123, [11, 12])

    def test_invalid_bodies_never_reach_catalog(self):
        for body in ({"message_ids": []}, {"message_ids": [True]}, {"message_ids": ["1"]},
                     {"message_ids": [-1]}, {"message_ids": [2**31]},
                     {"message_ids": list(range(1, 102))}, {"message_ids": [1], "delete_source": True}):
            with self.subTest(body=body):
                response = self.client.post("/api/t2td/channels/-100123/download", json=body)
                self.assertEqual(response.status_code, 422)
        self.catalog.enqueue_videos.assert_not_awaited()
        response = self.client.patch("/api/t2td/channels/-100123", json={"enabled": "false"})
        self.assertEqual(response.status_code, 422)
        self.catalog.update_channel.assert_not_awaited()
        for query in ("limit=0", "limit=101", "before_id=-1"):
            self.assertEqual(self.client.get("/api/t2td/channels/-100123/videos?" + query).status_code, 422)
        self.catalog.list_videos.assert_not_awaited()

    def test_cooldown_and_unavailable_are_visible(self):
        self.catalog.scan_channel.side_effect = CatalogError(429, "请稍后重试", retry_after=123)
        response = self.client.post("/api/t2td/channels/-100123/scan", json={})
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers["retry-after"], "123")
        self.assertEqual(response.json()["detail"], "请稍后重试")
        self.catalog.scan_channel.side_effect = CatalogError(503, "请先登录 Telegram")
        self.assertEqual(self.client.post("/api/t2td/channels/-100123/scan", json={}).status_code, 503)

    def test_unexpected_failure_does_not_leak_internal_exception(self):
        self.catalog.list_channels.side_effect = RuntimeError("private credential detail")
        response = self.client.get("/api/t2td/channels")
        self.assertEqual(response.status_code, 500)
        self.assertNotIn("credential", response.text)


if __name__ == "__main__":
    unittest.main()
