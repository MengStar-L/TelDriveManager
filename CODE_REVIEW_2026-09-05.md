# TelDriveManager 全项目审查

审查日期：2026-09-05。原始代码基线：`46c38a9`。本轮已落实下述 19 项修复；原始发现保留作为故障背景，原始行号对应基线版本。

## 2026-09-06 再审查修复

针对再审查发现的 7 项问题完成以下修改。这些验证取代之前对自动更新流程的过度结论。

| 再审查问题 | 本次修复 |
| --- | --- |
| 串行暂停、恢复可能误删共享缓存 | 移除这两条路径及串行队列整理中的删除操作；核实 aria2 已停止并保存会话后才脱离旧 GID，保留断点缓存和路径。RPC 失败或状态无法确认则返回失败，保留原任务。 |
| systemd 清理安装子进程 | 通过 `systemd-run` 创建独立临时服务，由安装服务显式停止和启动应用；保留 systemd 默认控制组清理。权限及 MainPID 检查在停机前完成。 |
| 进程存活误当作启动成功 | 导入应用前执行启动门禁；安装期间拒绝加载混合文件，验证阶段允许启动；通过随机事务标识、启动版本和真实本机 HTTP 就绪响应确认成功。 |
| 中断后没有完整回滚日志 | 先备份全部文件，再原子写入并刷盘完整恢复清单，最后替换文件；保存的独立安装器用于启动恢复。强制终止后的重启恢复有真实子进程测试。 |
| 回滚失败删除备份 | 回滚失败保留备份及锁，阻止混合版本启动；缺失恢复清单或旧文件时拒绝假报恢复成功；空间恢复后可重试同一事务。 |
| 抢锁失败误删其他实例锁 | 锁位于各项目内部；系统文件锁协调安装与恢复，以随机 token 验证清理归属，不按文件年龄删除他人锁。记录进程身份，避免 PID 重用误判。 |
| 更新不处理依赖变化 | 安装前检查依赖解析、`pip check`、源码语法及应用导入；依赖有变化时创建独立虚拟环境，不修改原环境，回滚恢复原环境指针。 |

附加修复：更新下载逐块检查空间；解压和哈希在后台线程运行；取消时等待线程退出；更新失败原因显示到设置页；新版服务确认后浏览器刷新资源；正在安装时检查更新不能覆盖安装状态。

本轮最终验证在 `D:\RunTime\Temp\TelDriveManager-updatefix-20260905` 隔离副本运行：Python 共 253 项，252 项通过、1 项真实 aria2 测试因未配置二进制跳过；6 个 JavaScript 测试文件全部通过，`node --check` 和 `git diff --check` 通过。覆盖真实应用子进程健康后提交、新版启动失败后旧版恢复响应、安装进程硬退出后恢复，以及模拟磁盘不足、依赖失败、锁竞争和 systemd 启停顺序。正式配置、数据库、Telegram/PikPak/TelDrive 均未用于故障测试。

Linux/systemd 未在实机运行验证，当前 Windows 主机没有可用 Linux/WSL 环境。独立环境安装仅接受 wheel，不兼容平台或升级启动协议时需手动更新；更新回滚不撤销远端操作及未来的破坏性数据库迁移。磁盘预留与巡检不是系统配额，不能保证其他程序或底层存储故障下绝不耗尽空间。代码尚未发布或部署。

## 磁盘停滞专项补修

用户随后提出的“多个任务各下载一半，没有一个能完成上传”的场景在上一版仍可复现。本次补修针对这一缺口，不将“暂停保护”等同于“能够自动恢复”。以下行为取代前一版未知大小任务采用 8 MiB/s 滚动预算的策略。

- 新增 `app/modules/aria2teldrive/disk_recovery.py`：按剩余需求选择可完成任务，按实际磁盘分配量规划回收，并提前占住为它释放的预算，防止被回源或新下载抢走。
- 回收范围限定为已登记、可重新访问的 HTTP(S) 单文件半成品，必须核对暂停状态、独立归属、源大小、断点控制文件以及文件身份。保护已确认完整的上传缓存、手动暂停任务、共享路径、符号链接、硬链接、目录和 BT 多文件缓存。可回收量仍不足时不盲目删除。
- SQLite 增加 `disk_recovery_json` 保存准备、已停止及文件身份信息。确认 aria2 停止并保存会话后才删除指定文件；失败和重启可继续，旧任务保留源链接、参数、目标路径和大小重新排队。未完成回收的任务不能被普通调度器误放行，用户仍可明确取消或删除。
- 未知大小的 HTTP(S) 任务只查询响应头，大小不明时保持暂停；零字节响应与未知大小区分处理。已知大小下载预留剩余需求。托管 aria2 启动增加 `--pause=true`，重新核算预算后才恢复。
- 真实 aria2 测试发现，重启后的暂停任务尚未加载断点时会暂报总大小或进度为零。现按已有缓存实际分配的磁盘块抵扣未来增长；该统计只用于空间预算，不作为下载完整性依据。
- 等待列表改为完整分页。磁盘错误失败任务会定期以暂停状态重新排队，不再依赖磁盘保护先解除。RPC 暂时无法放行时保留闸门归属供下轮重试。
- 增加独立磁盘巡检：低于保留值或容量查询失败时请求暂停，无法通过 RPC 控制时停止本程序托管的 aria2。停止后需恢复容量并重启服务；不会终止其他来源的进程。
- 回源恢复队列前清理已登记且无活跃下载者的 `.part`，空间不足退出时释放槽位；完整失败上传缓存继续保留。aria2 标准输出改为每份 2 MiB、两份备份的轮转日志，读取错误日志也只读末尾 64 KiB。
- 前端显示具体等待和回收原因，按既有转义规则渲染；内部回收记录及其中下载凭据不返回到任务接口。

验证使用最新隔离副本、临时 SQLite、模拟容量及本地 HTTP 服务。新增 `tests/test_disk_recovery.py` 和可通过 `ARIA2_TEST_BINARY` 启用的 `tests/test_disk_recovery_aria2.py`。Windows 上真实 aria2 1.37.0 测试跑通两个半成品停滞、回收、续传、进程重启保持暂停、完成清理，以及被回收任务重新下载；两份文件逐字节比对通过。没有实际填满磁盘，没有使用正式 Telegram、TelDrive、PikPak 或正式配置和数据库。

本次完整 Python 测试集 **220 项通过**（包含真实 aria2 测试，26.439 秒）；最终缓存归属补强后再运行相关 **38 项回归通过**。JavaScript **5 个测试文件通过**；`compileall`、`node --check`、`git diff --check` 通过。测试日志仍位于 `D:\RunTime\Temp\TelDriveManager-fixes-20260905\python-tests.log`。下文“修复验证”的 194 项是前一轮 19 项修复的历史结果。

仍有明确边界：自动回收不能解决单文件超过磁盘容量、源站不提供大小、远端持续无法上传或没有可安全回收缓存的情况；BT 多文件缓存不自动回收。单进程预算及定时巡检不是系统级配额，不能保证其他程序、底层存储超售或操作系统故障下磁盘绝不写满。隔离系统盘需要独立卷或系统级配额。修改尚未部署。

## 修复状态

| 编号 | 状态 | 实现与回归验证 |
| --- | --- | --- |
| F01 | 已修复 | 分页和子目录查询失败抛出异常，完整快照才参与删除；确认周期只计一次；删除前重新查询并保护仍被引用的分块，删除失败保留待清理映射。 |
| F02 | 已修复 | `.part` 文件、逐区间字节计数、短写重试、最终长度与刷盘校验；原子改名后保存下载标记及指纹，预扩展旧文件不再被直接上传。 |
| F03 | 已修复 | SQLite 保存逐文件完成记录和当前文件身份；完成分块不等于完成文件记录；累计分块不重复计算，空文件实际上传，目录变化阻止最终完成。 |
| F04 | 已修复 | 新消息仅按已登记的消息 ID 去重；同名不同来源继续处理，不按文件名删除或补到旧映射。 |
| F05 | 已修复 | PostgreSQL 权威映射或频道归属查询失败时暂停删除；已配置数据库但没有该文件权威 parts 时不使用旧映射删除。 |
| F06 | 已修复 | 实时事件、同步入口及实际删除边界检查开关；运行中关闭同步后，旧配置对象不能继续执行同步删除。回源成功后的源消息清理仍属于回源流程。 |
| F07 | 已修复 | 删除同名同大小即成功的捷径；完成幂等需要匹配分块身份。旧版本只在新文件记录成功创建且能确认分块不重叠时清理；上传失败或分块归属不明时保留旧版本。 |
| F08 | 已修复 | 明确大小不匹配的远端分块只能作为孤儿；最终校验数量、编号和可获取的大小、总大小，不创建错误记录。 |
| F09 | 已修复 | Telegram 日志使用 `textContent`；JS 测试禁止使用 HTML sink，并覆盖 message、level、time 三个字段。 |
| F10 | 已修复 | 每次验证令牌签名、当前凭据和有效期；进程内缓存不再绕过过期或改密检查。 |
| F11 | 已修复 | 配置同目录临时写入、刷盘、验证及原子替换；失败保留原配置与缓存，空配置拒绝启动；映射文件也采用原子保存。 |
| F12 | 已修复 | 新增 `app/disk_budget.py`，本地 aria2 与回源按文件系统共享剩余写入预留；保留系统空间，串行也生效；空间不足退出当前回源槽位等待，已下载任务优先恢复。 |
| F13 | 已修复 | 持久化账号、源文件 ID 和传输完成依赖；推送失败、部分推送、仅入队均不删除源；完成记录在任务记录清除后仍可验证。磁链部分选择仅清理所选文件。 |
| F14 | 已修复 | 取消、清理失败任务和定期缓存清理核对托管路径及其他任务占用；失败保留任务与错误，不能只删除记录留下无主缓存。 |
| F15 | 已修复 | 并行上传 finally 取消并等待所有分块协程；任务重试先等待旧上传退出；旧协程只移除自身注册，等待上传槽时取消也释放会话。 |
| F16 | 已修复 | 上传提交结果单独落库，清理可独立恢复；本地清理失败不能标记完成，源删除失败不阻止本地释放空间；旧失败清理任务按已有文件 ID 迁移。 |
| F17 | 已修复 | 一个动态限流器维护实际活跃数；热改并发唤醒原等待队列，不创建第二套 semaphore。 |
| F18 | 已修复 | 重试等待旧任务时明确处理 `CancelledError`，保留注册直至旧任务退出并复核完成状态，再重置调度。 |
| F19 | 已修复 | 根目录与递归子目录消费全部分页；保留原分享定位参数，重复游标、重复节点和目录环引用报列表不完整。 |

## 修复验证

- 在 `D:\RunTime\Temp\TelDriveManager-fixes-20260905` 的最新源码副本上运行测试。没有复制或使用正式配置、数据库、Telegram session；所有外部删除、上传均为模拟调用。
- Python 完整测试集 194 项全部通过，覆盖原有 157 项及新增 37 项故障回归。最终结果见该目录 `python-tests.log`；JavaScript 5 个测试文件全部通过，包含原 4 个测试文件和新增日志注入回归。
- Python `compileall`、`node --check app/static/app.js` 和 `git diff --check` 通过。正式 `config.toml`、`tasks.db` 的修改时间仍分别为 2026-08-03、2026-08-02；未修改正式会话或执行远端删除。
- 上传集成测试使用本地 aiohttp 模拟 TelDrive 服务，实际接收并逐字节比对上传内容；覆盖错误分块大小、同名旧版本、不完整文件和取消。
- SQLite 使用临时数据库验证逐文件续传、旧清理状态升级、清理依赖跨重启及删除任务记录后的保留。磁盘测试模拟零空间、共享卷超额预留、不同卷及串行恢复；没有实际填满磁盘。
- 新增 `tests/test_review_regressions.py`、`tests/test_telegram_log_escape.js`；更新了旧测试中“串行关闭磁盘保护”“推送后立即删源”的不安全预期。

## 上线边界

- 代码已修改，尚未部署或重启正式服务。下次正常启动自动增加 SQLite 状态字段和清理队列表；旧目录总进度不再作为跳过文件的依据，旧回源缓存没有完成标记时会重新下载。
- 失败上传的完整缓存保留用于重试，占用会进入实际可用空间计算；不足以容纳新文件时等待，不继续积累到零空间。取消和清理任务可回收其所属缓存；清理失败会保留记录。
- 预算按仓库默认单进程运行方式协调。多个 worker、其他程序突然占盘、网络文件系统容量变化或 aria2 RPC 失联无法暂停，需要服务器层面的容量监控。远程 aria2 镜像任务不受本地预算控制。
- 真实 Telegram、TelDrive、PikPak、PostgreSQL 和生产故障日志未接入；真实 TelDrive 版本的同名创建策略、PikPak 长时间排队后的直链有效期仍需部署环境验证。远端拒绝提交时任务失败并保留源文件，不报告成功。

## 原始审查

## 结论

发现 19 组需要修复的问题，其中 13 组 P1、6 组 P2。最高风险是源文件误删、上传不完整却标记成功、磁盘空间保护缺失，以及后台权限和配置失效。P1 表示应优先修复的文件安全、权限或服务可用性问题，P2 表示影响部分流程正确性与长期运行的问题。

现有 157 项 Python 测试和 4 个 JavaScript 测试文件全部通过，不能据此认为程序没有问题。审查另外构造了 19 个 Python 故障场景和 1 个 JavaScript 渲染检查，均复现了报告中的缺陷；部分相关场景合并为一组问题。

## P1 问题

### F01：目录查询失败被当成文件删除，可能误删 Telegram 原始分块

- 位置：[service.py:1475](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:1475)、[service.py:2209](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:2209)、[service.py:2275](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:2275)。
- `list_teldrive_dir()` 遇到 HTTP 错误、超时或解析失败时返回已收集的部分结果，第一页失败时返回空列表。调用方无法区分完整快照和失败快照，把缺少的文件当作真实消失。
- 复现：先返回一个正常文件，之后连续两次模拟目录读取失败；Telegram 查询仍确认原消息存在。配置 `confirm_cycles=3` 时，第二次失败后就调用了 Telegram 删除。这也证实首次发现消失的周期被重复计数。
- 影响：TelDrive 短时不可用或一个子目录持续读取失败，可能导致真实文件的 Telegram 数据被删除。现有“Telegram 消息大比例缺失”保护不覆盖这个反向同步路径。
- 修复方向：任何分页或子目录失败都必须令整个快照不可用于删除判断；独立记录完整性；删除前重新验证，并修正确认周期计数。

### F02：回源不完整文件被当成完整文件上传，随后删除源消息

- 位置：[relay.py:201](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:201)、[relay.py:222](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:222)、[relay.py:609](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:609)。
- 多 bot 下载先 `truncate(file_size)`，文件逻辑长度立即等于目标长度；恢复任务时只以文件是否存在、长度是否相同决定跳过下载，未使用可靠的下载完成标记。worker 也不校验每个区间实际收到的字节数，队列空就返回成功。
- 复现一：4096 字节文件只写入 1024 字节后预扩展，状态仍为 downloading。恢复时下载次数为 0，直接上传含 3072 个零字节的文件，并调用源消息删除，任务完成。
- 复现二：bot 迭代器正常提前结束，只返回 1024/4096 字节，下载函数仍返回成功。
- 修复方向：使用独立 `.part` 文件和持久化完成标记，逐区间确认长度，成功后原子改名；不得仅靠 `st_size` 判完整。上传前也应校验期望大小与已验证的下载完成状态。

### F03：目录上传续传重复累计分块，重试会跳过未上传文件并删本地目录

- 位置：[task_manager.py:2909](/D:/Code/TelDriveManager/app/modules/aria2teldrive/task_manager.py:2909)、[task_manager.py:2927](/D:/Code/TelDriveManager/app/modules/aria2teldrive/task_manager.py:2927)、[task_manager.py:3058](/D:/Code/TelDriveManager/app/modules/aria2teldrive/task_manager.py:3058)。
- `confirmed_chunks_total` 已从历史总基线开始，恢复时跳过已完成文件或完成当前部分上传文件后，又累计该文件的全部分块。由此把历史分块重复计算为新进度。
- 复现：目录中 a.bin 为两块、b.bin 为一块，a 已确认一块。补完 a 后 b 上传失败，数据库却记录已确认 3 块，而实际只有 a 的 2 块。再次重试没有调用任何上传，直接标记 completed，并自动删除整个本地目录。
- 修复方向：分别记录已完成文件和当前文件分块检查点，累计进度只计算新增确认块；文件记录创建成功才允许跳过该文件。

### F04：仅凭文件名相同就删除新 Telegram 消息

- 位置：[service.py:2745](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:2745)、[service.py:2752](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:2752)。
- 重复判断使用整个 TelDrive 映射中的名称集合，没有比较大小、文件内容或来源消息身份。
- 复现：已存在 same.mp4、大小 1024，新消息同名但大小 8192，仍调用删除新消息的接口。
- 影响：同名不同版本、不同目录的同名视频会直接丢失新源消息，尚未执行回源下载或上传。
- 修复方向：名称冲突不能自动等同于重复；删除须依赖可验证的同一文件身份或内容证据。

### F05：数据库复核失败时仍使用旧映射删除 TelDrive 文件

- 位置：[service.py:1580](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:1580)、[service.py:1610](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:1610)、[service.py:1903](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:1903)。
- 权威 parts 查询失败返回 `{}`，外频道查询失败返回空集合，与“查询成功但没有数据”混淆。删除函数在没有权威数据时继续依据本地旧消息 ID 删除。
- 复现：两个 PostgreSQL 查询都模拟连接失败，本地映射中的旧消息已缺失，仍调用 TelDrive 文件删除。
- 修复方向：查询失败返回明确的不可确认状态；已配置数据库时，无法完成权威复核必须暂停删除。

### F06：关闭“删除同步”仍会响应 Telegram 删除事件

- 位置：[service.py:2478](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:2478)、[service.py:2700](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:2700)。
- `sync_enabled` 只控制轮询任务，实时 `MessageDeleted` 处理器无条件注册，事件回调和下游删除函数都没有检查该开关。
- 复现：`sync_enabled=False` 时触发模拟 Telegram 删除事件，仍进入 TelDrive 删除处理。
- 修复方向：在实际执行同步删除的入口统一检查运行时开关，保证保存关闭配置后所有相关路径停止。

### F07：上传续传仅凭同名同大小就误报成功

- 位置：[teldrive_client.py:1126](/D:/Code/TelDriveManager/app/modules/aria2teldrive/teldrive_client.py:1126)。
- 只要存在任何历史已确认分块，且目标目录找到同名同大小文件，就把它当成本任务上次完成的结果；没有验证 upload_id、parts 或内容归属。
- 复现：传入一个未完成上传的检查点，目标返回一个无关的同名同大小文件。函数直接 success，parts 核验调用次数为 0。
- 影响：普通任务可能删除本地新版本，回源任务可能删除 Telegram 新源消息，但远端实际仍是其他内容。
- 修复方向：完成幂等判断必须绑定本次上传的文件 ID、分块集合或可靠内容身份。

### F08：最终远端分块校验仍会接受明确错误的分块大小

- 位置：[teldrive_client.py:277](/D:/Code/TelDriveManager/app/modules/aria2teldrive/teldrive_client.py:277)、[teldrive_client.py:779](/D:/Code/TelDriveManager/app/modules/aria2teldrive/teldrive_client.py:779)。
- 去重时没有符合期望大小的候选，就退回全部候选并选一个；最终校验只检查数量和编号，不验证分块大小。
- 复现：期望 4096 字节的一块，远端明确返回 size=1024，仍创建文件记录并返回 success。
- 修复方向：已知大小不匹配的候选不得保留为有效分块；创建文件记录前验证各块大小及总字节数。

### F09：Telegram 日志存在脚本注入入口

- 位置：[service.py:2726](/D:/Code/TelDriveManager/app/modules/tel2teldrive/service.py:2726)、[app.js:4952](/D:/Code/TelDriveManager/app/static/app.js:4952)。
- 频道文件名直接进入日志，前端把日志 message、level、time 直接插入 `innerHTML`，未转义。其他模块已有转义函数，但此路径没有使用。
- 验证：带 `<img ... onerror=...>` 的合成文件名原样进入日志 HTML。此检查验证了实际渲染函数的 HTML sink，未在真实管理员页面执行脚本。
- 影响：能向监听频道发送文件的人，可借日志页面在管理员同源上下文运行脚本，读取设置或调用后台操作接口。
- 修复方向：日志纯文本使用 `textContent`；确需拼接 HTML 时逐字段转义。

### F10：已登录会话绕过过期与改密验证

- 位置：[auth.py:103](/D:/Code/TelDriveManager/app/auth.py:103)。
- `verify_token()` 遇到 `_active_tokens` 中的 token 直接返回 True，绕过签名、签发时间及当前账号密码校验。
- 复现：时间超过 7 天后仍通过；随后修改密码，原 token 仍通过。底层签名校验本应拒绝这些 token。
- 修复方向：每次请求都执行当前签名和有效期验证，或采用包含过期时间与凭据版本的受限缓存。

### F11：保存配置失败会破坏原配置，重新加载后可能关闭鉴权

- 位置：[config.py:472](/D:/Code/TelDriveManager/app/config.py:472)、[auth.py:81](/D:/Code/TelDriveManager/app/auth.py:81)。
- 保存先以 `wb` 截断正式配置，再写 TOML。磁盘满、进程退出或写入异常时没有原子替换或回滚。
- 复现：原配置有账号密码，模拟 dump 阶段 ENOSPC 后文件剩余 0 字节。空 TOML 仍能解析，重新加载得到默认空账号密码，鉴权关闭。
- 修复方向：写同目录临时文件，成功刷盘和验证后原子替换；损坏配置不得静默降级为无认证服务。

### F12：回源下载没有磁盘预算，失败文件持续积累

- 位置：[relay.py:539](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:539)、[relay.py:609](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:609)、[task_manager.py:276](/D:/Code/TelDriveManager/app/modules/aria2teldrive/task_manager.py:276)。
- 回源没有下载前的文件体积预算、并发空间预留或下载期间空间保护；aria2 的磁盘闸门和串行模式不控制回源。串行模式同时还会禁用 aria2 自身的磁盘保护。
- 复现：模拟剩余空间 0，三个回源任务仍先后启动下载；磁盘检查调用次数为 0；上传失败后三个本地文件全部保留。
- 影响：单文件超过剩余空间、并发竞争，或连续上传失败均能逐步写满磁盘。把回源并发设为 1 不能解决累积问题。
- 修复方向：按实际所在文件系统统一核算 aria2 和回源空间，原子预留预算，保留必要空间；不足时等待并显示原因。失败缓存应有明确保留、重试与回收策略。

### F13：PikPak 推送失败后仍永久删除源文件

- 位置：[pikpak/routes.py:1595](/D:/Code/TelDriveManager/app/modules/pikpak/routes.py:1595)、[pikpak/routes.py:1599](/D:/Code/TelDriveManager/app/modules/pikpak/routes.py:1599)、[pikpak/client.py:511](/D:/Code/TelDriveManager/app/modules/pikpak/client.py:511)。
- `_aria2_push_only()` 捕获推送异常后仍进入云端删除逻辑；该逻辑也没有等待真实下载完成。`delete_files()` 调用的是 `delete_forever()`。
- 复现：aria2 批量推送直接抛异常，成功创建下载数为 0，但仍调用 PikPak 删除。
- 修复方向：源文件回收应绑定实际下载/上传完成状态，失败或仅入队不能作为成功依据。分享隔离目录的 finally 回收也需要结合排队、长时等待和直链可用性确定生命周期；本次未连接真实 PikPak 验证删除后直链的存活时间。

## P2 问题

### F14：清理失败任务只删除记录，留下无法追踪的本地文件

- 位置：[task_manager.py:3688](/D:/Code/TelDriveManager/app/modules/aria2teldrive/task_manager.py:3688)、[aria2teldrive/routes.py:107](/D:/Code/TelDriveManager/app/modules/aria2teldrive/routes.py:107)。
- 普通任务 `delete_task()` 只对进行中状态调用包含文件清理的 cancel，failed 状态直接删数据库记录。定期清理又只处理数据库中 completed 的任务。
- 复现：删除一个 failed 任务，数据库删除成功、本地 4096 字节文件仍存在。
- 修复方向：在清理任务时依据文件归属处理本地缓存；清理失败保留可追踪记录和可重试状态，避免永久孤儿文件。

### F15：取消并行上传不会取消其分块子任务

- 位置：[teldrive_client.py:1020](/D:/Code/TelDriveManager/app/modules/aria2teldrive/teldrive_client.py:1020)。
- 父任务等待 `asyncio.wait()` 时被取消，finally 只上报并发数 0，没有取消和等待其创建的子任务。
- 复现：取消父上传后，两个分块协程仍存活；放行模拟上传后仍执行两次检查点回调。
- 影响：暂停、重试、超时或退出时，旧任务可能继续执行请求或回调，与新上传竞争。真实请求可能随后因 session 关闭而失败，但并无可靠的子任务回收保证。
- 修复方向：finally 中取消并等待所有未完成子任务，且旧上传清理不得移除新上传的运行状态。

### F16：回源清理阶段失败会重新上传，清理失败可能被当作完成

- 位置：[relay.py:623](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:623)、[relay.py:653](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:653)、[relay.py:996](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:996)。
- 上传成功后的 cleaning 状态没有独立恢复分支；删除源消息失败后，下一次 `_process_job()` 又执行上传。只有源删除成功才尝试清理本地文件，本地删除异常又被吞掉，调用方仍标记 completed。
- 本轮复现：仅模拟第一次源消息删除失败、第二次成功，上传被执行两次。前一轮隔离模拟还验证了本地 PermissionError 后文件仍保留、状态却 completed。
- 修复方向：把下载、上传完成和清理作为独立持久化阶段；cleaning 只重试清理；定期回收 completed 回源的残留文件。

### F17：修改回源并发数会同时运行新旧两套限流器

- 位置：[relay.py:302](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:302)、[relay.py:540](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:540)。
- 热更新直接替换 semaphore，已运行和已排队协程继续持有旧对象，新任务进入新对象。
- 复现：从并发 1 改为 2 后，同时观察到 3 个任务进入处理阶段。
- 修复方向：保留一个稳定的动态调度器，依据实际活跃数发放槽位，或等待旧队列受控退出后再切换。

### F18：重试排队中的回源任务会取消 API 自身

- 位置：[relay.py:430](/D:/Code/TelDriveManager/app/modules/tel2teldrive/relay.py:430)。
- `retry_job()` 取消并 await 旧任务时只抑制 Exception；`CancelledError` 属于 BaseException。排队中的任务在进入 `_run_job()` 内部异常处理前取消，因此异常直接传播，后续重置和调度不会执行。
- 复现：任务等待 semaphore 时点击重试，抛出 CancelledError，任务未重新进入调度表。
- 修复方向：准确处理等待旧任务结束所产生的取消异常，并保证新任务注册不受旧任务回调影响。

### F19：PikPak 分享列表未处理分页

- 位置：[pikpak/client.py:540](/D:/Code/TelDriveManager/app/modules/pikpak/client.py:540)、[pikpak/client.py:578](/D:/Code/TelDriveManager/app/modules/pikpak/client.py:578)。
- 分享根目录只请求 `limit=100` 的第一页；子目录也只调用一次底层目录接口，没有消费 next_page_token。
- 复现：第一页返回 100 项并明确给出下一页 token，函数仍只请求一次并返回这 100 项。
- 修复方向：分享根目录和递归子目录均补齐分页，任何分页失败都应报告列表不完整。

## 验证与范围

- 阅读范围：FastAPI 启动和路由、配置与鉴权、SQLite、aria2 托管与 RPC、任务调度和清理、TelDrive 分块上传、Telegram 监听与删除同步、回源和 bot 下载池、PikPak 分享/磁链/多账号/健康检查、前端日志和任务接口、部署文件及现有测试。
- 代码副本：[source](/D:/RunTime/Temp/TelDriveManager-audit-20260905/source)。来自 `git archive HEAD`，没有复制工作目录中的正式配置、凭据、会话或数据库。
- 审查环境：独立 Python 3.13 环境，依赖按仓库 requirements.txt 安装。原项目 `.venv` 指向不存在的 `D:\python\python.exe`，因此未使用它运行测试。
- 现有测试：[python-tests.log](/D:/RunTime/Temp/TelDriveManager-audit-20260905/python-tests.log)。157 项 Python 测试通过；4 个 JavaScript 测试文件通过。Python 编译检查与 `node --check app/static/app.js` 通过。
- 额外复现：[review_checks.py](/D:/RunTime/Temp/TelDriveManager-audit-20260905/review_checks.py)、[review_checks.js](/D:/RunTime/Temp/TelDriveManager-audit-20260905/review_checks.js)。其中目录续传和配置失败测试使用真实本地数据库/配置逻辑，写入限于临时目录；外部网络、远端删除与上传接口均以模拟替代。
- 结果：[review-results.jsonl](/D:/RunTime/Temp/TelDriveManager-audit-20260905/review-results.jsonl)、[review-js-results.jsonl](/D:/RunTime/Temp/TelDriveManager-audit-20260905/review-js-results.jsonl)。
- 本次没有连接生产服务器、真实 Telegram/TelDrive/PikPak 或 PostgreSQL，没有真实执行删除、回源或上传，没有实际耗尽磁盘，也没有运行完整浏览器端端测试。多 bot 的 POSIX 定位写入在 Windows 下以受控等价写入模拟。
- 本地正式配置的回源开关为关闭，正式任务表为空；实际运行服务器的版本、配置、挂载和故障时日志未验证。报告确认代码存在的触发路径，不声称所有问题都已在生产发生。

## 修复顺序

1. 先修误删和错误完成：F01-F08、F13，并为每条失败路径加入回归测试。
2. 修磁盘预算和缓存生命周期：F12、F14-F17，同时处理配置原子写入 F11，防止磁盘问题扩大成配置丢失。
3. 修权限和页面输入处理：F09-F10，应与前两项同时列为优先修复。
4. 最后处理排队重试 F18、分享分页 F19，并补充真实服务的端到端验证。

原始基线中“关闭删除同步”不能阻止事件删除，见 F06；该行为已在本轮修复，状态和范围以上文为准。
