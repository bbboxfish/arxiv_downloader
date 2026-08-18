# arxiv-downloader 服务器 SQLite 运行与百万篇扩展设计

## 1. 文档范围

本文面向一台不安装 PostgreSQL 的 Linux 服务器，使用 SQLite 运行
`arxiv-downloader`。服务器提供两块持久化存储：

- `/ssd/ltwiki`：低延迟、高 IOPS，负责数据库、SQLite WAL、下载临时文件和运行状态。
- `/hdd/ltwiki`：大容量，负责最终 PDF、发布中间区、隔离文件、清单和数据库备份。

本文同时描述当前 A0 代码的试运行方法，以及扩展到百万篇论文前必须补齐的技术能力。
标记为“目标能力”的内容是设计要求，不代表当前代码已经实现。

## 2. 核心原则

1. SQLite 数据库和它的 `-wal`、`-shm` 文件必须位于同一个 SSD 本地文件系统目录。
2. 下载中的 `.part` 文件放在 SSD，避免 HDD 上产生大量小块随机写入。
3. 最终 PDF、`incoming` 和 `quarantine` 全部放在同一个 HDD 文件系统；这样
   `incoming` 到最终对象的 `os.replace` 才是原子重命名。
4. SSD 到 HDD 是“校验后复制”，不能假设跨文件系统移动具有原子性。
5. 只运行一个 `arxivd` 进程。SQLite 允许多个读取者，但本方案只允许一个任务状态写入者。
6. 下载并发不以填满带宽为目标。必须遵守 arXiv 的访问政策、限速要求和明确的
   `User-Agent` 联系方式。
7. `/hdd/ltwiki` 或 `/ssd/ltwiki` 未正确挂载时，服务必须拒绝启动，避免写入系统盘。

## 3. SSD 与 HDD 的职责

### 3.1 SSD：活跃状态和临时工作区

建议目录：

```text
/ssd/ltwiki/arxiv-downloader/
  db/
    arxiv.db
    arxiv.db-wal
    arxiv.db-shm
  staging/
    {task_id}/
      {paper_id}.part
      {paper_id}.part.meta.json       # 目标能力：断点信息
  spool/                              # 目标能力：分块导入文件
  maintenance/
  .mount_sentinel
```

SSD 负责：

- SQLite 主数据库、WAL 和共享内存文件。
- 尚未完成的 HTTP 下载文件。
- 断点续传所需的 ETag、Last-Modified、已下载字节数等状态。
- 大批量 ID 导入的临时分块文件。
- SQLite 临时排序和维护操作的可控工作空间。

SSD 不负责长期保存 PDF。下载完成并发布到 HDD 后，应删除 SSD 上对应的 `.part` 和
断点元数据。

### 3.2 HDD：不可变对象和可恢复副本

建议目录：

```text
/hdd/ltwiki/arxiv/
  .mount_sentinel
  incoming/
    {task_id}/
      {paper_id}.part
  objects/
    pdf/
      submitted/
        YYYY/
          MM/
            DD/
              {normalized_arxiv_id_with_version}.pdf
  manifests/
    daily/
    batches/
  quarantine/
  backups/
    sqlite/
  reports/
```

HDD 负责：

- 已校验完成的 PDF 对象。
- 与最终对象位于同一文件系统的 `incoming` 发布目录。
- 文件存在但校验不通过时的隔离区。
- 可独立于 SQLite 检查的批次清单和每日清单。
- SQLite 在线备份文件和恢复演练报告。

HDD 上的 `objects` 应按“写入后不可变”处理。任何替换都必须经过重新下载、校验、
写入 `incoming`、原子发布和数据库事务记录。

## 4. 容量规划

百万篇 PDF 的空间不能按论文数量直接估算，应先用样本计算真实平均值和 P95 大小：

```text
HDD 原始需求 = 论文数量 × 平均 PDF 大小
HDD 建议容量 = 原始需求 × 1.25 至 1.40
SSD staging 需求 = 最大并发下载数 × max_file_size × 3 + 数据库/WAL/维护余量
```

示例：平均每篇 3 MiB 时，100 万篇约为 2.86 TiB；按 30% 余量规划至少约 3.72 TiB。
如果平均值是 8 MiB，则原始 PDF 已接近 7.63 TiB。上线前必须以实际样本重新计算。

SQLite 容量也要单独压测。若每篇论文的任务、索引和原始元数据平均占 5 至 20 KiB，
100 万篇可能占用约 5 至 20 GiB，维护和迁移时还需要额外空间。SSD 应至少预留数据库
实际大小的 3 倍，用于 WAL、在线备份临时开销、索引构建和恢复演练。

建议保护水位：

- HDD 可用空间低于 `max(总容量的 10%, 500 GiB)` 时停止领取新任务。
- SSD 可用空间低于 `max(总容量的 15%, 100 GiB)` 时停止领取新任务。
- 达到保护水位后允许正在发布的任务完成，但不能开始新下载。
- 监控 inode 使用率；inode 可用比例低于 10% 时同样停止领取任务。

以上低水位保护属于百万篇目标能力，当前 A0 代码尚未实现。

## 5. 安装

以下命令假定代码部署到 `/opt/arxiv-downloader`，服务账户为 `arxivd`：

```bash
sudo useradd --system --home /var/lib/arxiv-downloader --shell /usr/sbin/nologin arxivd

cd /opt/arxiv-downloader
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

确认 Python 和 SQLite 版本：

```bash
/opt/arxiv-downloader/.venv/bin/python --version
/opt/arxiv-downloader/.venv/bin/python -c 'import sqlite3; print(sqlite3.sqlite_version)'
```

建议 SQLite 版本不低于 3.35。服务器不需要安装或启动 PostgreSQL。

## 6. 挂载与目录初始化

先确认两个路径确实是预期的持久化挂载：

```bash
findmnt /ssd/ltwiki
findmnt /hdd/ltwiki
findmnt -no SOURCE,FSTYPE,OPTIONS /ssd/ltwiki
findmnt -no SOURCE,FSTYPE,OPTIONS /hdd/ltwiki
mountpoint -q /ssd/ltwiki
mountpoint -q /hdd/ltwiki
df -hT /ssd/ltwiki /hdd/ltwiki
df -ih /ssd/ltwiki /hdd/ltwiki
```

SQLite 数据库应位于本机 ext4 或 XFS 等具有可靠 POSIX 锁和 `fsync` 语义的文件系统。
如果 `/ssd/ltwiki` 实际是 NFS、CIFS、对象存储 FUSE 或其他网络文件系统，不应把 SQLite
数据库放在该路径。HDD 文件系统也必须确认同一挂载内的原子重命名和 `fsync` 语义；否则
需要重新设计发布协议。

只有确认挂载正确后才能创建哨兵文件：

```bash
sudo install -d -o arxivd -g arxivd -m 0750 \
  /ssd/ltwiki/arxiv-downloader/db \
  /ssd/ltwiki/arxiv-downloader/staging \
  /ssd/ltwiki/arxiv-downloader/spool \
  /ssd/ltwiki/arxiv-downloader/maintenance

sudo install -d -o arxivd -g arxivd -m 0750 \
  /hdd/ltwiki/arxiv/incoming \
  /hdd/ltwiki/arxiv/objects/pdf/submitted \
  /hdd/ltwiki/arxiv/manifests/daily \
  /hdd/ltwiki/arxiv/manifests/batches \
  /hdd/ltwiki/arxiv/quarantine \
  /hdd/ltwiki/arxiv/backups/sqlite \
  /hdd/ltwiki/arxiv/reports

sudo -u arxivd touch /ssd/ltwiki/arxiv-downloader/.mount_sentinel
sudo -u arxivd touch /hdd/ltwiki/arxiv/.mount_sentinel
```

不要让服务在启动过程中自动创建哨兵。哨兵由管理员在确认挂载后创建。

## 7. 服务器配置文件

创建配置目录：

```bash
sudo install -d -o root -g arxivd -m 0750 /etc/arxiv-downloader
```

创建 `/etc/arxiv-downloader/config.sqlite3.toml`：

```toml
[server]
host = "127.0.0.1"
port = 8765

[database]
dsn_env = "ARXIV_DATABASE_URL"
url = "sqlite+aiosqlite:////ssd/ltwiki/arxiv-downloader/db/arxiv.db"

[download]
concurrency = 2
request_timeout_seconds = 180
max_attempts = 4
max_file_size_mb = 300
min_request_interval_seconds = 3
user_agent = "arxiv-downloader/0.1 admin@example.com"

[storage]
mode = "mounted"
mount_root = "/hdd/ltwiki"
data_root = "/hdd/ltwiki/arxiv"
staging_root = "/ssd/ltwiki/arxiv-downloader/staging"
sentinel_file = "/hdd/ltwiki/arxiv/.mount_sentinel"
```

必须把 `admin@example.com` 改成真实、可联系的运维邮箱。

```bash
sudo chown root:arxivd /etc/arxiv-downloader/config.sqlite3.toml
sudo chmod 0640 /etc/arxiv-downloader/config.sqlite3.toml
```

当前配置模型只能对一个 `mount_root` 执行挂载保护，所以它会保护 HDD。systemd 的
`RequiresMountsFor` 同时保护 SSD 和 HDD；SSD 哨兵和独立应用级检查属于后续代码增强项。

SQLite URL 中绝对路径前有四个斜杠：

```text
sqlite+aiosqlite:////ssd/ltwiki/arxiv-downloader/db/arxiv.db
```

## 8. 代理配置

arXiv 元数据和 PDF 请求需要经过现有代理。CLI 与本机 `arxivd` 的通信必须绕过代理，
因此要设置 `NO_PROXY`。

创建 `/etc/arxiv-downloader/arxivd.env`：

```bash
HTTP_PROXY=http://127.0.0.1:17890
HTTPS_PROXY=http://127.0.0.1:17890
ALL_PROXY=socks5://127.0.0.1:17890
http_proxy=http://127.0.0.1:17890
https_proxy=http://127.0.0.1:17890
all_proxy=socks5://127.0.0.1:17890
NO_PROXY=127.0.0.1,localhost
no_proxy=127.0.0.1,localhost
```

```bash
sudo chown root:arxivd /etc/arxiv-downloader/arxivd.env
sudo chmod 0640 /etc/arxiv-downloader/arxivd.env
```

`httpx` 默认读取环境代理。HTTPS 请求优先使用 `HTTPS_PROXY`。如果实际需要通过
`ALL_PROXY` 的 SOCKS5 连接，必须额外安装 SOCKS 支持：

```bash
source /opt/arxiv-downloader/.venv/bin/activate
python -m pip install 'httpx[socks]'
```

只使用现有 HTTP/HTTPS 代理时，不需要 SOCKS 额外依赖。验证代理时不要下载 PDF：

```bash
set -a
source /etc/arxiv-downloader/arxivd.env
set +a
curl --fail --silent --show-error --max-time 30 --output /dev/null \
  'https://export.arxiv.org/api/query?search_query=id:1706.03762'
curl --fail --head --max-time 30 'https://arxiv.org/'
```

## 9. 初始化 SQLite

不要在 shell 中设置 PostgreSQL URL。显式选择服务器配置：

```bash
unset ARXIV_DATABASE_URL
export ARXIV_DOWNLOADER_CONFIG=/etc/arxiv-downloader/config.sqlite3.toml
cd /opt/arxiv-downloader
sudo -u arxivd env -u ARXIV_DATABASE_URL \
  ARXIV_DOWNLOADER_CONFIG=/etc/arxiv-downloader/config.sqlite3.toml \
  /opt/arxiv-downloader/.venv/bin/alembic upgrade head
sudo -u arxivd env -u ARXIV_DATABASE_URL \
  ARXIV_DOWNLOADER_CONFIG=/etc/arxiv-downloader/config.sqlite3.toml \
  /opt/arxiv-downloader/.venv/bin/alembic current
```

检查数据库和表：

```bash
sqlite3 /ssd/ltwiki/arxiv-downloader/db/arxiv.db '.tables'
sqlite3 /ssd/ltwiki/arxiv-downloader/db/arxiv.db 'PRAGMA integrity_check;'
sqlite3 /ssd/ltwiki/arxiv-downloader/db/arxiv.db 'PRAGMA foreign_key_check;'
```

当前程序建立 SQLite 连接时会启用：

```sql
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=30000;
PRAGMA journal_mode=WAL;
```

百万篇场景建议保持 `synchronous=FULL` 以优先保证任务状态持久性。若未来改为
`synchronous=NORMAL`，必须先完成断电恢复和文件/数据库对账测试，不能仅为了提高写入速度
直接修改。

## 10. systemd 服务

创建 `/etc/systemd/system/arxivd.service`：

```ini
[Unit]
Description=arXiv SQLite download daemon
Wants=network-online.target
After=network-online.target
RequiresMountsFor=/ssd/ltwiki /hdd/ltwiki

[Service]
Type=simple
User=arxivd
Group=arxivd
WorkingDirectory=/opt/arxiv-downloader
EnvironmentFile=/etc/arxiv-downloader/arxivd.env
Environment=ARXIV_DOWNLOADER_CONFIG=/etc/arxiv-downloader/config.sqlite3.toml
Environment=SQLITE_TMPDIR=/ssd/ltwiki/arxiv-downloader/maintenance
ExecStartPre=/usr/bin/mountpoint -q /ssd/ltwiki
ExecStartPre=/usr/bin/mountpoint -q /hdd/ltwiki
ExecStartPre=/usr/bin/test -r /ssd/ltwiki/arxiv-downloader/.mount_sentinel
ExecStartPre=/usr/bin/test -r /hdd/ltwiki/arxiv/.mount_sentinel
ExecStart=/opt/arxiv-downloader/.venv/bin/arxivd
Restart=on-failure
RestartSec=10s
TimeoutStopSec=120s
KillSignal=SIGINT
UMask=0027
LimitNOFILE=65536
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/ssd/ltwiki/arxiv-downloader /hdd/ltwiki/arxiv

[Install]
WantedBy=multi-user.target
```

加载并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now arxivd
sudo systemctl status arxivd
sudo journalctl -u arxivd -f
```

检查本机控制接口是否绕过代理：

```bash
export NO_PROXY=127.0.0.1,localhost
export no_proxy="$NO_PROXY"
curl --fail http://127.0.0.1:8765/health
```

## 11. 试运行命令

```bash
cd /opt/arxiv-downloader
source .venv/bin/activate
export ARXIV_DOWNLOADER_CONFIG=/etc/arxiv-downloader/config.sqlite3.toml
export NO_PROXY=127.0.0.1,localhost
export no_proxy="$NO_PROXY"

arxivctl daemon status
arxivctl task create --name server-smoke --input demo/smoke_ids.txt
arxivctl task show BATCH_ID
arxivctl task progress BATCH_ID --watch --interval 2
arxivctl dataset verify BATCH_ID
```

检查文件落盘位置：

```bash
find /hdd/ltwiki/arxiv/objects/pdf/submitted -type f -name '*.pdf' | head
du -sh /ssd/ltwiki/arxiv-downloader/db
du -sh /ssd/ltwiki/arxiv-downloader/staging
du -sh /hdd/ltwiki/arxiv/objects
```

## 12. 当前 A0 的中断恢复行为

当前代码已经具备以下恢复语义：

- 任务状态存储在 SQLite 中，CLI 退出不影响后台下载。
- `arxivd` 启动时把遗留的 `RUNNING` 任务重新置为 `PENDING`。
- `resume_downloads = true` 时保留 staging 中遗留的 `.part` 和 sidecar 检查点。
- 重启后使用 ETag/Last-Modified 和 HTTP Range 尝试续传；服务忽略 Range 时安全地从头下载。
- `resume_downloads = false` 时恢复旧 A0 行为，启动时清理 `.part` 文件。
- 网络失败会按照 `next_attempt_at` 和指数退避重新排队；404、超大文件和非 PDF 不重试。
- 配置了低水位阈值后，调度器会停止领取新任务并在 `/health` 返回低容量状态。
- 最终文件校验成功后才写入 COMPLETE artifact。
- 已存在且大小、SHA-256 与数据库一致的 artifact 可以直接复用。
- 最终文件存在但 artifact 缺失时，可以校验并补建记录。

这适合服务器试运行和较长任务，但完整的百万篇对账、分块导入和自动维护仍需要后续实现。

## 13. 百万篇目标：可恢复下载

### 13.1 持久化下载检查点

每个 SSD `.part` 文件应有对应的持久化检查点，至少记录：

```text
task_id
paper_id
pdf_url
bytes_downloaded
expected_total_bytes
etag
last_modified
last_checkpoint_at
```

当前实现使用与 `.part` 同目录的原子替换 sidecar 文件。
不要每写一个网络 chunk 就提交 SQLite；建议每 8 至 32 MiB 或每 10 至 30 秒更新一次，
以减少单写者争用。

### 13.2 HTTP Range 恢复

恢复时执行：

1. 检查 `.part` 实际大小是否与检查点一致；不一致时以较小值为准并截断。
2. 使用 `Range: bytes={size}-` 和 `If-Range: {etag 或 last_modified}` 请求。
3. 只有服务返回 `206` 且 `Content-Range` 起点完全匹配时才追加；完整检查点会先进行本地校验。
4. 返回 `200`、ETag 改变、长度不匹配或服务器不支持 Range 时，删除旧断点并从头下载。
5. 恢复后重新从头读取 `.part` 计算 SHA-256；不要依赖无法可靠持久化的内存哈希状态。
6. 完成后校验 `%PDF-`、最大大小、最终长度和 SHA-256，再进入 HDD 发布阶段。

### 13.3 写入和发布的崩溃一致性

目标发布顺序：

1. 在 SSD 下载 `.part`。
2. 对 SSD 文件完成 PDF、长度和 SHA-256 校验。
3. 复制到 `/hdd/ltwiki/arxiv/incoming/{task_id}/...part`。
4. 对 HDD incoming 文件再次校验大小和 SHA-256。
5. `fsync` incoming 文件，再 `fsync` incoming 目录。
6. 在 HDD 内使用 `os.replace` 原子发布到最终路径。
7. `fsync` 最终文件所在目录。
8. 在一个短 SQLite 事务中写入 artifact 并把任务标记为 `SUCCEEDED`。
9. 删除 SSD staging 和无用的 sidecar。

数据库事务不能跨越网络下载或 SSD 到 HDD 的大文件复制。

### 13.4 启动对账矩阵

启动恢复必须处理以下组合：

| 数据库状态 | SSD `.part` | HDD incoming | HDD final | 恢复动作 |
|---|---:|---:|---:|---|
| RUNNING | 有 | 无 | 无 | 校验断点并 Range 恢复 |
| RUNNING | 无 | 无 | 无 | 重新置为 PENDING |
| RUNNING | 任意 | 有 | 无 | 校验 incoming，完成发布或删除重来 |
| RUNNING | 任意 | 任意 | 有 | 校验 final，补写 artifact 后成功 |
| SUCCEEDED | 无 | 无 | 有 | 抽样或按策略复核 |
| SUCCEEDED | 任意 | 任意 | 无 | 标记不一致，重新排队并告警 |
| FAILED | 有 | 任意 | 无 | 按保留策略续传或清理 |
| 任意 | 任意 | 任意 | 校验失败 | 移入 quarantine，禁止覆盖证据 |

对账应分批、可暂停，并使用基于主键的游标分页，不能一次把百万条记录装入内存。

## 14. 百万篇目标：任务模型与调度

当前 A0 每批最多 50 个输入、最多 2 个 Worker。百万篇不能直接作为一个 HTTP 请求或一个
超大数据库事务提交。目标设计应包括：

- 导入文件按 5,000 至 20,000 个 ID 分块，记录导入游标和文件 SHA-256。
- 每个分块使用短事务执行去重和任务创建；失败只回滚当前分块。
- 使用 keyset pagination，例如 `(state, updated_at, task_id)`，禁止深层 `OFFSET`。
- 将元数据抓取、PDF 下载、校验和发布拆为持久化阶段。
- 增加 `RETRY_WAIT`、`VERIFYING`、`PUBLISHING` 等状态，以及 `next_attempt_at`。
- 对 HTTP 429、5xx、代理断开和超时使用带随机抖动的指数退避。
- 正确解析 `Retry-After`，并设置全局熔断，避免代理或 arXiv 故障时形成请求风暴。
- 所有任务领取必须有状态条件更新；任何任务完成操作都必须是幂等的。
- 只保留一个 SQLite 写入者。下载可以并发，但状态写入应小批量、短事务化。
- 进度统计维护增量计数或汇总表，避免每次 UI 查询扫描百万行。

SQLite 适合单机、单写者的百万行任务元数据，但不适合多个 `arxivd` 进程共享数据库。
如果未来需要多机 Worker、主从、高可用或跨节点抢占任务，应重新评估数据库和队列架构。

## 15. arXiv 请求速率与代理故障

按当前 3 秒最小请求间隔，单纯发起 100 万次请求的理论下限约为 34.7 天。若每篇分别需要
一次元数据请求和一次 PDF 请求，理论下限至少翻倍，且没有计入下载时间、重试和维护窗口。

因此百万篇计划必须：

- 先确认 arXiv 当前的 API 和批量数据访问政策。
- 评估官方 bulk data、公开对象存储或数据快照，而不是对网页端做百万级逐篇抓取。
- 使用真实联系邮箱的 User-Agent。
- 代理不可用时暂停领取新任务，而不是立即循环重试。
- 区分目标站点 429、目标站点 5xx、HTTP 代理错误和 SOCKS 代理错误。
- 记录代理失败率、请求延迟、下载吞吐和连续失败次数。
- 为代理恢复设置半开探测，单次探测成功后再逐步恢复任务领取。

任何提高并发或缩短间隔的修改都应先获得数据源政策允许，而不是仅依据服务器带宽决定。

## 16. SQLite 索引与维护

百万篇目标至少需要检查以下索引：

```text
papers(arxiv_id, version) UNIQUE
download_tasks(batch_id, paper_id) UNIQUE
download_tasks(state, updated_at, task_id)
download_tasks(next_attempt_at, state)          # 目标能力
artifacts(paper_id, kind) UNIQUE
artifacts(status, created_at)                   # 目标能力
```

维护原则：

- 不在下载高峰执行 `VACUUM`。
- 删除大量记录后，在维护窗口评估 `VACUUM`；正常运行只做 `PRAGMA optimize`。
- 定期执行 `ANALYZE` 或 `PRAGMA optimize`，让查询规划器获得最新统计。
- 监控 `arxiv.db-wal` 大小；异常增长说明长读事务或 checkpoint 受阻。
- 备份前不要求强制停止服务，使用 SQLite online backup API。
- 完整 `integrity_check` 成本较高；日常可执行 `quick_check`，完整检查放在恢复副本上。

建议维护命令：

```bash
sqlite3 /ssd/ltwiki/arxiv-downloader/db/arxiv.db 'PRAGMA quick_check;'
sqlite3 /ssd/ltwiki/arxiv-downloader/db/arxiv.db 'PRAGMA foreign_key_check;'
sqlite3 /ssd/ltwiki/arxiv-downloader/db/arxiv.db 'PRAGMA optimize;'
sqlite3 /ssd/ltwiki/arxiv-downloader/db/arxiv.db 'PRAGMA wal_checkpoint(PASSIVE);'
```

不要在 `arxivd` 正在运行时手工复制 `arxiv.db` 单文件作为备份，因为未合并的 WAL 可能使
副本不一致。

## 17. 备份与恢复演练

创建一致性在线备份：

```bash
export ARXIV_DOWNLOADER_CONFIG=/etc/arxiv-downloader/config.sqlite3.toml
/opt/arxiv-downloader/.venv/bin/arxivctl database export \
  --output "/hdd/ltwiki/arxiv/backups/sqlite/arxiv-$(date +%Y%m%d-%H%M%S).sqlite3"
```

校验最新备份：

```bash
BACKUP=/hdd/ltwiki/arxiv/backups/sqlite/arxiv-YYYYMMDD-HHMMSS.sqlite3
sqlite3 "$BACKUP" 'PRAGMA integrity_check;'
sqlite3 "$BACKUP" 'PRAGMA foreign_key_check;'
```

恢复演练必须在独立路径进行：

```bash
install -d -o arxivd -g arxivd -m 0750 /ssd/ltwiki/arxiv-downloader/restore-test
cp "$BACKUP" /ssd/ltwiki/arxiv-downloader/restore-test/arxiv.db
sqlite3 /ssd/ltwiki/arxiv-downloader/restore-test/arxiv.db 'PRAGMA integrity_check;'
sqlite3 /ssd/ltwiki/arxiv-downloader/restore-test/arxiv.db \
  'SELECT state, COUNT(*) FROM download_tasks GROUP BY state;'
```

数据库备份不等于数据集备份。还必须保留或重新生成 HDD 文件清单，至少包含：

```text
object_key
arxiv_id
version
size_bytes
sha256
created_at
```

## 18. 监控与告警

至少监控：

- SSD/HDD 可用字节、inode 和挂载状态。
- SQLite 主文件和 WAL 大小。
- PENDING、RUNNING、RETRY_WAIT、FAILED、SUCCEEDED 数量。
- 最老 PENDING 任务等待时间。
- 每分钟成功数、失败数和重试数。
- HTTP 429、5xx、超时、代理错误和非 PDF 响应。
- SSD 到 HDD 发布耗时和校验耗时。
- staging、incoming、quarantine 文件数量和总大小。
- 最近一次数据库备份时间、大小和校验结果。

当前数据库可以用以下只读查询初步检查：

```bash
DB=/ssd/ltwiki/arxiv-downloader/db/arxiv.db
sqlite3 "$DB" 'SELECT state, COUNT(*) FROM download_tasks GROUP BY state;'
sqlite3 "$DB" 'SELECT state, COUNT(*) FROM batches GROUP BY state;'
sqlite3 "$DB" 'SELECT status, COUNT(*), SUM(size_bytes) FROM artifacts GROUP BY status;'
```

## 19. 故障演练

正式扩大规模前至少完成：

1. 下载过程中 `systemctl stop arxivd`，确认重启后任务可以恢复。
2. 下载过程中终止进程，确认 SQLite `quick_check` 和外键检查通过。
3. 暂停代理，确认服务限速退避且不会高速循环失败。
4. 让代理恢复，确认任务可以继续执行。
5. 临时卸载 HDD，确认 systemd 和挂载保护拒绝启动或发布。
6. 模拟 SSD/HDD 低空间，确认停止领取新任务。
7. 在 SSD `.part`、HDD incoming、HDD final 三个阶段分别中断并执行启动对账。
8. 构造“数据库成功但文件缺失”和“文件存在但数据库缺失”，验证修复策略。
9. 从 HDD 备份恢复一份 SQLite 到独立 SSD 目录并执行完整校验。
10. 对随机抽样 PDF 重新计算 SHA-256，与数据库和 manifest 比对。

其中退避熔断、完整启动对账、分块导入和自动维护仍属于目标能力，需代码实现后再验收。

## 20. 当前代码与百万篇目标的差距

当前代码可以用于 20 至 50 篇的服务器 SQLite 试运行，但在扩大到百万篇之前必须处理：

- 当前单批输入上限为 50，尚未实现百万行导入游标、分块提交和增量进度汇总。
- 当前有 HTTP Range 续传和 `.part.meta.json` 检查点，但尚未实现完整启动对账矩阵。
- 当前有 `next_attempt_at` 和指数退避，但尚未实现随机抖动、`Retry-After` 和代理熔断。
- 当前有 SSD/HDD 字节和百分比低水位配置，但尚未检查 inode，也未将 SSD 挂载检查集成到应用层。
- 当前应用配置只显式检查 HDD `mount_root`，SSD 主要依靠 systemd 保护。
- 当前没有自动的文件系统/数据库全量对账任务。
- 当前没有自动备份调度、备份保留策略和恢复演练自动化。
- 当前只适合一个 `arxivd` 进程，不能通过增加进程扩展 SQLite 写入能力。

建议扩容路径：

```text
20-50 篇冒烟测试
  -> 1,000 篇中断与代理故障测试
  -> 10,000 篇容量、WAL 和恢复测试
  -> 100,000 篇分块导入与长稳测试
  -> 完成所有目标能力和 arXiv 数据源合规确认
  -> 百万篇任务
```

在 10,000 篇以上测试前，应先实现断点续传、分块导入、退避、低水位保护和启动对账，
不能仅通过增大批次上限进入百万篇规模。
