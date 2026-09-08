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
- TelDrive 分片存储、Telegram 监听和自动清理统一使用 `[teldrive].channel_id`；旧版双字段冲突时自动删除会被阻止。
- Telegram 删除决策会记录完整时间、频道、消息 ID、原因及成功、失败或阻止状态。
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

### 自动更新

设置页会定期检查仓库的 GitHub Releases。发现新版本后点击“立即更新”，程序会下载并校验源码，保留配置、任务数据库、下载缓存、aria2 状态和 Telegram 会话。没有发布版本时会显示“暂无发布版本”。请使用 `python main.py` 启动；直接运行 uvicorn 或启用开发热重载时不提供自动更新。

安装前会验证 Python 依赖、源码语法和应用导入。依赖需要变化时，在 `.tdm-runtimes/` 中建立独立虚拟环境，安装新版依赖并运行 `pip check`，不会修改当前环境；依赖下载、兼容性或磁盘检查失败时，原服务继续运行。自动安装只接受当前平台可用的 wheel，缺少 wheel、Python 版本不兼容或更新启动协议不兼容时需要手动升级。启动器按 `.tdm-runtime` 选择环境，原来的启动命令仍然可用。

更新器完整备份后才开始替换文件；新版必须通过带本次更新标识的本机 HTTP 就绪检查，已安装的托管 aria2 也必须运行且 RPC 可用，才会确认成功并清理备份。失败时恢复源码、版本标记和旧环境指针。回滚失败会保留备份和 `.tdm-update-lock`，阻止混合版本启动；修复空间或权限问题后重新启动，启动器会重试恢复。该回滚不撤销远端操作，也不承诺兼容未来的破坏性数据库迁移。

Linux/systemd 自动更新使用独立的临时 systemd 服务执行安装，由它停止、更新、启动原服务，不依赖子进程逃离原控制组。需要 systemd 支持 `systemd-run --collect`，服务以 root 运行且 `MainPID` 是 `main.py` 进程；仓库附带的部署方式符合此要求。权限或服务识别不满足时，更新会在停机前拒绝。安装进程意外退出会重试恢复；整机重启后由启动器读取持久化记录恢复。恢复期间不要手动删除锁或备份。

更新额外预留约 3.6 GiB 的下载、解压和依赖准备预算，并另行检查备份容量。下载、解压和依赖准备期间会检查磁盘保留空间；旧的更新环境仅保留当前及上次使用的环境。软件检查不能替代文件系统配额，其他进程或底层存储异常仍可能耗尽空间。

### 已部署 Linux 服务升级

首次从没有自更新功能的版本升级时，可使用 Release 附带的脚本。它适用于以 root、systemd 和项目内 `venv` 或 `.venv` 部署的服务；准备失败不停止原服务，新版启动失败恢复旧版。配置、任务数据库、下载缓存、Telegram 会话均保留。

```bash
curl -fL --retry 3 https://github.com/MengStar-L/TelDriveManager/releases/download/v1.1.1/update-linux.sh -o /tmp/tdm-update-v1.1.1.sh
sudo bash /tmp/tdm-update-v1.1.1.sh /opt/TelDriveManager teldrive-manager.service v1.1.1
```

按实际部署修改第二行的目录和服务名。脚本校验发布包 SHA-256，需要至少 6 GiB 空闲空间供初始解压；后续安装还会按配置检查磁盘保留值、依赖和备份容量。失败原因可从脚本输出的 `journalctl` 命令查看，恢复记录和备份不要手动删除。脚本按发布包更新源码，不修改 `.git` 元数据；后续推荐使用设置页继续更新。

## 配置说明

Telegram 活动日志按 5 MiB 轮转并保留 3 份备份。后台消息全量核验最短间隔为 5 分钟，查询超时或限流后自动退避；实时消息监听与文件快照同步仍照常运行。aria2 RPC 故障会在面板显示并自动退避重试，避免持续刷日志。部署时还应给 systemd journal 和 Docker 日志设置容量上限，它们不受应用日志轮转控制。

完整示例见 [`config.example.toml`](./config.example.toml)。常用配置段如下：

| 配置段 | 用途 |
| --- | --- |
| `[server]` | Web 面板端口，默认 `8888` |
| `[auth]` | 面板登录用户名与密码 |
| `[pikpak]` | PikPak 账号、密码或 encoded token |
| `[aria2]` | 本地 aria2 托管、RPC、并发、磁盘保护 |
| `[remote_aria2]` | 远程 aria2 镜像推送 |
| `[teldrive]` | TelDrive API、Token、共享 Telegram 存储/监听频道 ID、上传分块 |
| `[upload]` | 自动上传、重试、串行模式、分块并行上传 |
| `[telegram]` | Telegram API、会话与同步开关 |
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

### 磁盘与重试

本地 aria2 下载和 Telegram 回源按实际文件系统共享剩余写入预算，保留 `[aria2].disk_protection_threshold_gb` 指定的空间，默认 5 GiB。串行模式也执行空间检查。任务必须有足够空间容纳剩余内容；未知大小的 HTTP(S) 下载先通过 HEAD 或单字节 Range 请求读取响应头，确认大小后才放行。始终不提供大小的源或尚无大小元数据的其他协议任务保持暂停；已确认的零字节文件可以放行。空间不足时任务等待，已下载文件的上传仍可继续。

多个半成品相互占用空间时，程序会优先完成一个任务：核对 HTTP(S) 单文件的源链接、实际分配磁盘块、缓存归属和停止状态，只回收足以让一个任务完成的其他未完成缓存，并保留源链接、下载参数和任务记录重新排队。已确认完整、正在上传、用户手动暂停、共享路径、符号链接及无法确认可重新下载的缓存不会自动回收。多文件 BT 缓存不参与自动回收。回收前写入 SQLite 操作记录，停止 aria2 并保存会话后才删除文件；异常或重启后继续未完成操作，失败保留记录，可手动取消或删除。

托管 aria2 重启时先暂停恢复的下载。暂停状态尚未读入断点信息时，已有缓存按实际分配空间抵扣后续磁盘增长，不将文件长度当作下载完成证明。独立巡检每 2 秒检查下载盘，跌破保留值时请求暂停；RPC 无法响应时停止本程序托管的 aria2，恢复容量后需重启服务。aria2 新日志使用每份 2 MiB、保留 2 份备份的轮转策略，避免日志无限追加。

回源文件先写入 `.part`，通过长度校验和刷盘后才登记下载完成。下载期间空间不足会清理本次 `.part` 并释放槽位；恢复队列前还会清理已登记、没有活跃下载者的遗留 `.part`。上传完成与源消息、本地缓存清理分别持久化，清理失败重试不会重新上传。失败上传的完整缓存保留用于续传；取消或删除任务时清理其所属缓存，删除失败会保留记录。PikPak 源文件在对应传输任务全部完成后清理，部分选择下载只清理所选文件。

升级会自动增加 SQLite 状态字段和清理队列表。旧目录任务的汇总进度无法证明每个文件已提交，因此首次恢复会重新逐文件核验或上传；旧回源文件没有下载完成标记时会重新下载。已有配置为空或损坏时启动报错，需要恢复有效配置。

使用 `python main.py` 或下方 systemd 配置运行单个程序实例。共享预算在一个进程内协调，不支持多个 worker 共同操作同一下载目录和任务库。远程 aria2 镜像推送不计入本地预算。自动回收不能解决单个文件本身超过容量、上传端持续故障或没有可安全回收数据的情况；这些情况会明确等待。软件巡检不是文件系统配额，不能控制其他程序突然占盘、虚拟磁盘底层超售、容量突变或操作系统故障；需要隔离系统盘时应将下载目录放到独立卷或配置系统级配额。

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
