"""TelDriveManager 启动入口"""

import logging
import os
import sys
import json
import runpy
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# 屏蔽底层框架的大量网络请求日志输出，防止刷屏造成误解
logging.getLogger("httpx").setLevel(logging.WARNING)

# 确保项目根目录在 sys.path 中
ROOT_DIR = Path(__file__).parent
UPDATE_PROTOCOL = 2
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main():
    update_lock = ROOT_DIR / ".tdm-update-lock"
    if update_lock.exists():
        transaction = json.loads(update_lock.read_text(encoding="utf-8"))
        worker = Path(transaction["worker"])
        if ROOT_DIR.resolve() not in worker.resolve().parents:
            raise RuntimeError("Invalid update recovery worker")
        runpy.run_path(str(worker))["startup_gate"](ROOT_DIR.resolve())
    else:
        from update_worker import startup_gate
        startup_gate(ROOT_DIR.resolve())

    import uvicorn
    from app.config import load_config

    config = load_config()
    port = config.get("server", {}).get("port", 8888)
    from app.updater import update_manager
    version = update_manager._startup_version

    print(f"""
╔══════════════════════════════════════════════╗
║         TelDriveManager                     ║
║  PikPak + Aria2→TelDrive + Telegram Sync     ║
╚══════════════════════════════════════════════╝

  🌐  http://localhost:{port}
  Version: {version}
""")

    reload_enabled = os.getenv("TELDRIVE_RELOAD", "0").strip().lower() in {"1", "true", "yes", "on"}

    if reload_enabled:
        uvicorn.run(
            "app.main:app",
            host="0.0.0.0",
            port=port,
            log_level="warning",
            reload=True,
            reload_dirs=[str(ROOT_DIR / "app")],
            reload_excludes=[
                "downloads/*",
                "*.db",
                "*.db-*",
                "*.log",
                "*.session",
                "history_*.md",
            ],
        )
    else:
        server = uvicorn.Server(uvicorn.Config(
            "app.main:app",
            host="0.0.0.0",
            port=port,
            log_level="warning",
            reload=False,
        ))
        from app.updater import update_manager
        update_manager.shutdown_callback = lambda: setattr(server, "should_exit", True)
        server.run()


if __name__ == "__main__":
    main()
