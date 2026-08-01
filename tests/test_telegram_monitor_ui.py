import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


class TelegramMonitorUiContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (ROOT / "app" / "static" / "index.html").read_text(encoding="utf-8")
        cls.javascript = (ROOT / "app" / "static" / "app.js").read_text(encoding="utf-8")

    def test_settings_and_wizard_have_one_channel_input(self):
        self.assertEqual(self.html.count('id="cfgTeldriveChannel"'), 1)
        self.assertNotIn('id="cfgTelegramChannelId"', self.html)
        self.assertEqual(self.html.count('id="wTdChannel"'), 1)
        self.assertNotIn('id="wTgChannel"', self.html)

    def test_monitor_exposes_three_stable_modes(self):
        self.assertIn('data-t2td-mode="logs"', self.html)
        self.assertIn('data-t2td-mode="deleted"', self.html)
        self.assertIn('data-t2td-mode="telegram-deletions"', self.html)
        self.assertIn("setT2TDPanelMode('telegram-deletions')", self.html)

    def test_javascript_uses_independent_telegram_audit_api_and_sse(self):
        self.assertIn("/api/t2td/telegram-delete-logs", self.javascript)
        self.assertIn("data.type === 'telegram_delete_audit'", self.javascript)
        self.assertIn("function formatT2TDFullTimestamp", self.javascript)
        self.assertNotIn("cfgTelegramChannelId", self.javascript)
        self.assertNotIn("wTgChannel", self.javascript)


if __name__ == "__main__":
    unittest.main()
