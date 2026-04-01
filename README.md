# TelDriveManager

TelDriveManager 是一个强大的自动化桥接服务管理面板，专为 Aria2、PikPak 与 Telegram / TelDrive 生态打造的高效链路工具。它提供了现代且极简的 Web 配置与管理页面，允许您直观地管理所有的中转、上传和同步逻辑。

## 🌟 核心功能

- **Web 配置向导**：只需在网页端按步骤配置，全程无需修改本地文本配置。
- **健康监控自检**：实时测试所有的底层模块环境 (Aria2, TelDrive API, Telegram, 数据库等)，并支持单点重连自检。
- **Aria2 到 TelDrive 自动推送**：监控 Aria2 的下载队列，并稳定长效地推送到 TelDrive（配合限制并发与错误重试策略）。
- **云端代理中转**：完美无缝地接管 PikPak 离线文件。
- **热重启与并发保护**：完善的后台任务管理方案、安全的释放退出拦截。

## 🚀 快速启动

1. 安装依赖:
   ```bash
   pip install -r requirements.txt
   ```
2. 运行主程序:
   ```bash
   python app/main.py
   ```
3. 在浏览器打开管理端:
   ```
   http://localhost:8888
   ```

若出现配置失败等问题或首次进入系统，系统会自动弹出现代化向导界面帮助您初始化所有的 API 及密码。

## 🔧 技术栈
- 前端：纯净 Vanilla JS + CSS3 (Glassmorphism 拟物设计) + Phosphor 图标库
- 后端：FastAPI + RESTful & WebSocket
- 组件：Telethon, Aiohttp, Psycopg2, Tomli

## 👨‍💻 部署注意及已知支持
* Telegram 免密扫码登录中转 (依托Telethon) 完全支持从网页端拉取。
* 默认采用 Supabase 托管 PostgreSQL 或自建 PG 服务处理历史信息管理。
