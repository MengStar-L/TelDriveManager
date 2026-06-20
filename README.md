<p align="center">
  <img src="./image.png" width="96" alt="TelDriveManager logo">
</p>

<h1 align="center">TelDriveManager</h1>

<p align="center">
  <strong>把 PikPak、Aria2、Telegram 与 TelDrive 串成一条稳定的自动化链路。</strong>
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white">
  <img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-Web%20Panel-009688?style=for-the-badge&logo=fastapi&logoColor=white">
  <img alt="Aria2" src="https://img.shields.io/badge/Aria2-Download-4B5563?style=for-the-badge">
  <img alt="TelDrive" src="https://img.shields.io/badge/TelDrive-Cloud-0EA5E9?style=for-the-badge">
  <img alt="Telegram" src="https://img.shields.io/badge/Telegram-Sync-26A5E4?style=for-the-badge&logo=telegram&logoColor=white">
</p>

<p align="center">
  <a href="#快速开始">快速开始</a>
  ·
  <a href="#功能特性">功能特性</a>
  ·
  <a href="#配置说明">配置说明</a>
  ·
  <a href="#systemd-部署">Systemd 部署</a>
  ·
  <a href="#技术栈">技术栈</a>
</p>

---

TelDriveManager 是一个面向 TelDrive 生态的 Web 管理面板。它把 PikPak 离线能力、aria2 下载队列、TelDrive 分块上传和 Telegram 监听同步放到同一个界面里，适合长期运行在服务器上，处理磁链、分享链接、RSS 订阅和自动上传任务。

你不需要手动维护复杂脚本。首次启动后，面板会引导你完成 PikPak、aria2、TelDrive、Telegram 和数据库配置；日常使用时，可以在浏览器里解析、推送、监控、重试和清理任务。

## 工作流

| 输入来源 | 处理链路 | 输出结果 |
| --- | --- | --- |
| 磁力链接 | PikPak 解析文件树，选择目标文件后推送到 aria2 | 下载完成后自动上传到 TelDrive |
| PikPak 分享 | 解析分享目录，按需保留目录结构 | 转成可追踪的下载与上传任务 |
| RSS 订阅 | 解析条目并筛选资源 | 一键推送到下载队列 |
| Telegram 频道 | 监听文件消息与删除事件 | 同步维护 TelDrive 文件状态 |

## 功能特性

### Web 配置与自检

- 首次运行自动弹出配置向导，减少手动改 `config.toml` 的步骤。
- 支持 PikPak、aria2、TelDrive API、Telegram、PostgreSQL、远程 aria2 的单项测试。
- 提供全量健康检查，便于定位凭证、网络、数据库和运行时问题。

### PikPak 到 aria2

- 支持磁链解析、PikPak 分享解析和 RSS 订阅解析。
- 支持文件树选择、目录结构保留、目标路径设置。
- 可选远程 aria2 镜像推送：本地下载的同时，把同一链接推送到远程 aria2 下载器。

### Aria2 到 TelDrive

- 内置 aria2 托管能力，支持自动安装或上传压缩包安装。
- 下载完成后自动上传到 TelDrive，并记录任务进度。
- 支持暂停、恢复、取消、重试、批量清理失败或完成任务。
- 支持磁盘保护、串行安全模式、上传重试、自动清理本地缓存。
- 上传分块大小可配置，默认 `250M`，兼顾稳定性与重传成本。

### Telegram 与 TelDrive 同步

- 基于 Telethon 监听 Telegram 频道文件消息。
- 支持扫码登录和二次密码登录流程。
- 可根据 Telegram 缺失或删除事件，辅助清理 TelDrive 中的对应文件。
- 提供 TelDrive 文件夹扫描，便于检查目录结构与文件状态。

## 快速开始

适合本地试运行或首次体验：

```bash
git clone https://github.com/MengStar-L/TelDriveManager.git
cd TelDriveManager

python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows PowerShell
# .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
python main.py
```

启动后访问：

```text
http://localhost:8888
```

首次运行时，如果核心配置尚未填写，系统会自动打开初始化向导。配置会写入项目根目录下的 `config.toml`。

## 配置说明

完整示例见 [`config.example.toml`](./config.example.toml)。常用配置段如下：

| 配置段 | 用途 |
| --- | --- |
| `[server]` | Web 面板端口，默认 `8888` |
| `[auth]` | 面板登录用户名与密码 |
| `[pikpak]` | PikPak 账号、密码或 encoded token |
| `[aria2]` | 本地 aria2 托管、RPC、并发、磁盘保护 |
| `[remote_aria2]` | 远程 aria2 镜像推送 |
| `[teldrive]` | TelDrive API、Token、频道 ID、上传分块 |
| `[upload]` | 自动上传、重试、串行模式、分块并行上传 |
| `[telegram]` | Telegram API、监听频道、同步开关 |
| `[telegram_db]` | TelDrive / Telegram 同步所需 PostgreSQL |
| `[log]` | 面板日志缓冲与日志文件 |

也可以使用环境变量覆盖配置，格式为：

```text
TDM_SECTION_KEY=value
```

例如：

```bash
TDM_SERVER_PORT=8899 python main.py
```

## Systemd 部署

推荐将服务部署到 `/opt/TelDriveManager`，并使用仓库自带的 systemd 单元托管。

### 1. 安装依赖

```bash
cd /opt
git clone https://github.com/MengStar-L/TelDriveManager.git
cd TelDriveManager

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 安装服务

仓库内置服务文件：[`deploy/teldrive-manager.service`](./deploy/teldrive-manager.service)。

如果安装路径就是 `/opt/TelDriveManager`，直接复制：

```bash
sudo cp /opt/TelDriveManager/deploy/teldrive-manager.service /etc/systemd/system/teldrive-manager.service
```

如果安装到了其他目录，请先修改服务文件中的 `WorkingDirectory` 与 `ExecStart`。

### 3. 启动服务

```bash
sudo systemctl daemon-reload
sudo systemctl enable teldrive-manager
sudo systemctl start teldrive-manager
sudo systemctl status teldrive-manager
```

查看实时日志：

```bash
sudo journalctl -u teldrive-manager -f
```

## 更新

```bash
cd /opt/TelDriveManager
git pull origin main

source venv/bin/activate
pip install -r requirements.txt

sudo systemctl restart teldrive-manager
sudo systemctl status teldrive-manager
```

## 目录速览

```text
app/
  modules/
    aria2teldrive/    # aria2 下载与 TelDrive 上传
    pikpak/           # PikPak 磁链、分享、RSS 解析
    tel2teldrive/     # Telegram 监听与 TelDrive 同步
  routes/             # 登录、设置、自检、WebSocket
  static/             # Web 面板
deploy/               # systemd 服务文件
tests/                # 关键链路测试
config.example.toml   # 配置模板
main.py               # 启动入口
```

## 技术栈

| 层级 | 技术 |
| --- | --- |
| 后端 | FastAPI, Uvicorn, WebSocket |
| 前端 | Vanilla JavaScript, CSS, Phosphor Icons |
| 下载 | aria2 RPC, aiohttp, httpx |
| 网盘与同步 | PikPak API, TelDrive API, Telethon |
| 存储 | SQLite 任务库, PostgreSQL 同步数据 |
| 配置 | TOML, 环境变量覆盖 |

## 使用提示

- `config.toml`、`tasks.db`、`*.session` 等运行时文件包含敏感信息，不建议提交到仓库。
- 远程 aria2 推送只负责镜像下载链接，不负责上传到 TelDrive。
- `parallel_chunk_upload` 是试验功能。网络稳定时可以提速，遇到限流或 flood-wait 时建议关闭。
- 串行安全模式会强制下载和上传总占用为 1，适合磁盘空间紧张或希望严格控制资源的环境。

---

<p align="center">
  <sub>Made for a quieter, cleaner TelDrive workflow.</sub>
</p>
