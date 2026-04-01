"""TelDriveManager 启动入口"""

import logging
import sys
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# 确保项目根目录在 sys.path 中
ROOT_DIR = Path(__file__).parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def main():
    import uvicorn
    from app.config import load_config

    config = load_config()
    port = config.get("server", {}).get("port", 8888)

    print(f"""
╔══════════════════════════════════════════════╗
║         TelDriveManager v1.0                 ║
║  PikPak + Aria2→TelDrive + Telegram Sync     ║
╚══════════════════════════════════════════════╝

  🌐  http://localhost:{port}
""")

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
