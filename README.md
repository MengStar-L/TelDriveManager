# TelDriveManager

TelDriveManager 是一个强大的自动化桥接服务管理面板，专为 Aria2、PikPak 与 Telegram / TelDrive 生态打造的高效链路工具。它提供了现代且极简的 Web 配置与管理页面，允许您直观地管理所有的中转、上传和同步逻辑。

## 🌟 核心功能

- **Web 配置向导**：只需在网页端按步骤配置，全程无需修改本地文本配置。
- **健康监控自检**：实时测试所有的底层模块环境 (Aria2, TelDrive API, Telegram, 数据库等)，并支持单点重连自检。
- **Aria2 到 TelDrive 自动推送**：监控 Aria2 的下载队列，并稳定长效地推送到 TelDrive（配合限制并发与错误重试策略）。
- **云端代理中转**：完美无缝地接管 PikPak 离线文件。
- **热重启与并发保护**：完善的后台任务管理方案、安全的释放退出拦截。

## 🚀 快速部署与安装 (Systemd 服务)

为了确保服务能长期稳定在后台运行，我们推荐将其部署在 `/opt/TelDriveManager` 目录，并使用 Systemd 进行托管。

### 1. 克隆代码并安装依赖
```bash
# 切换到安装目录
cd /opt
# 克隆仓库
git clone https://github.com/MengStar-L/TelDriveManager.git
cd TelDriveManager

# 必须创建虚拟环境以隔离依赖
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置 Systemd 服务
创建服务配置文件：
```bash
sudo nano /etc/systemd/system/teldrive-manager.service
```

在文件中粘贴以下内容：
```ini
[Unit]
Description=TelDriveManager Web Service
After=network.target

[Service]
Type=simple
# 如果有特定非 root 用户，可在此更改
User=root
WorkingDirectory=/opt/TelDriveManager

# 默认使用虚拟环境内的 Python 启动 (推荐)
ExecStart=/opt/TelDriveManager/venv/bin/python main.py

# 如果在全系统安装了相关包，可以换用下方全局解释器启动：
# ExecStart=/usr/bin/python3 main.py

Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 3. 启用并启动服务
```bash
# 重载系统服务守护进程
sudo systemctl daemon-reload
# 设置开机自启
sudo systemctl enable teldrive-manager
# 启动服务
sudo systemctl start teldrive-manager
# 查看运行及错误日志
sudo systemctl status teldrive-manager
```

### 4. 访问面板
打开浏览器，访问你的服务器IP及相关端口：
```
http://服务器IP:8888
```
若出现配置失败等问题或首次进入系统，系统会自动弹出现代化向导界面帮助您初始化所有的 API 及密码。

### 5. 更新升级
当有新版本发布时，在服务器上执行以下命令即可完成更新：
```bash
cd /opt/TelDriveManager
# 拉取最新代码
git pull origin main
# 如有新增依赖，重新安装
source venv/bin/activate
pip install -r requirements.txt
# 重启服务
sudo systemctl restart teldrive-manager
# 确认服务状态
sudo systemctl status teldrive-manager
```

## 🔧 技术栈
- 前端：纯净 Vanilla JS + CSS3 (Glassmorphism 拟物设计) + Phosphor 图标库
- 后端：FastAPI + RESTful & WebSocket
- 组件：Telethon, Aiohttp, Psycopg2, Tomli

## 👨‍💻 部署注意及已知支持
* Telegram 免密扫码登录中转 (依托Telethon) 完全支持从网页端拉取。
* 默认采用 Supabase 托管 PostgreSQL 或自建 PG 服务处理历史信息管理。
