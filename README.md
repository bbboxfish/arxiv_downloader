# arxiv-downloader A0

`arxiv-downloader` is the runnable A0 implementation described in
[`A0.md`](A0.md). A short-lived `arxivctl` client talks to the persistent
`arxivd` service, which stores task state in SQLite by default (with optional
PostgreSQL support) and publishes verified PDFs to protected storage.

- macOS development uses repository-local storage under `.local/`.
- Linux deployment uses a mounted volume under `/mnt/arxiv`.
- The control API listens only on `127.0.0.1:8765`.

## Run locally on macOS

### 1. Prerequisites

You need Python 3.11 or newer and internet access to arXiv. The commands below
use Homebrew Python 3.14, matching the tested macOS environment.

Install Python if it is not already installed:

```bash
brew install python@3.14
```

SQLite is included with Python, so no database server is required for the
default setup.

### 2. Install the Python project

From the repository root:

```bash
cd /path/to/arxiv_downloader
python3.14 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

If `.venv` already exists, only activate it:

```bash
source .venv/bin/activate
```

### 3. Configure SQLite and local storage

The included `config/config.dev.toml` selects explicit local-development
storage. It skips Linux-only mount detection but retains sentinel, path,
checksum, quarantine, and publication checks.

```bash
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.dev.toml"

mkdir -p .local/storage-volume/arxiv .local/cache
touch .local/storage-volume/arxiv/.mount_sentinel
```

Before larger downloads, edit `config/config.dev.toml` and put a real contact
address in the `download.user_agent` value.

The development configuration stores SQLite at `.local/arxiv.db`. The
`.local/` directory is ignored by Git.

### 4. Create or update the database schema

```bash
alembic upgrade head
alembic current
```

The expected current revision is:

```text
0005_batch_rate_limit_metrics (head)
```

You can inspect the SQLite tables with:

```bash
sqlite3 .local/arxiv.db '.tables'
```

### 5. Start the daemon

Keep the following command running in the first terminal:

```bash
cd /path/to/arxiv_downloader
source .venv/bin/activate
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.dev.toml"
arxivd
```

For verbose daemon, scheduler, HTTP, and database diagnostics, start it with:

```bash
arxivd --debug
```

Normal INFO logging includes a concise line after each newly downloaded PDF is
published. The path is relative to the configured `storage.data_root`:

```json
{"event":"file_saved","filename":"2403.05530v5.pdf","size":"2.5 MiB",
 "size_bytes":2621440,"object_key":"objects/pdf/submitted/2024/03/05/2403.05530v5.pdf"}
```

Successful `HTTP/1.1 2xx` request lines and high-frequency successful metadata
event JSON are suppressed from normal output. Successful HTTP request lines
remain suppressed with `--debug`; failed HTTP lines and diagnostic events remain
available for troubleshooting.

When a batch completes, `arxivd` emits one `batch_finished` JSON event with
`queue_duration_ms`, `duration_ms`, `total`, `succeeded`, `failed`,
`cancelled`, `metadata_failed`, and `metadata_failed_ids`. Queue duration covers
creation until the first worker claim; `duration_ms` covers first claim until
completion. Download task counts exclude metadata failures, which are reported
separately with their IDs.

The event and `task show`/`task progress` also report `metadata_worker_count`,
`download_worker_count`, `metadata_rate_limit_wait_ms`,
`download_rate_limit_wait_ms`, and their sum `rate_limit_wait_ms`. Wait metrics
are accumulated across worker attempts and persisted in the database. Since
workers can wait concurrently, this worker-time sum can overlap itself and must
not be subtracted from wall-clock `duration_ms` to infer pure transfer time.

`task show` and `task progress` also list the arXiv IDs currently held by the
metadata worker and download workers. In `--watch` mode, errors are labeled as
historical; they are not evidence that the daemon is repeatedly downloading the
same paper. Multiple workers produce multiple active-ID lines. Old persisted
errors with an empty message display a fallback directing operators to
`arxivd --debug`; newly recorded HTTP exceptions include their exception type
or diagnostic detail. Metadata failures remain persisted in `batch_inputs` and
can be retrieved later with `task show <batch-id>` or `task progress <batch-id>`;
the CLI prints each one as `Metadata failed ID` without relying on retained logs.
Historical error lines include the last attempt's execution time when both
`started_at` and `finished_at` were recorded; queue time is excluded.

A successful startup ends with:

```text
Uvicorn running on http://127.0.0.1:8765
```

Press `Ctrl+C` to stop the daemon cleanly.

### 6. Use `arxivctl` from another terminal

Activate the project environment in the second terminal:

```bash
cd /path/to/arxiv_downloader
source .venv/bin/activate
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.dev.toml"
```

Check daemon, database, and storage health:

```bash
arxivctl daemon status
```

Expected output:

```text
Status:   ok
Database: ok
Storage:  ok
```

Run the included one-paper smoke test:

```bash
arxivctl task submit \
  --name mac-smoke-test \
  --input demo/smoke_ids.txt
```

The command returns a batch ID:

```text
Batch created: cf6c57d9-e7a2-4350-acbb-22890bd65326
Papers queued: 1
Duplicates skipped: 0
Invalid IDs: 0
```

Use the returned value in place of `BATCH_ID` below.

Show the current batch state once:

```bash
arxivctl task show BATCH_ID
```

Watch progress until the batch completes or is cancelled:

```bash
arxivctl task progress BATCH_ID --watch --interval 1
```

Verify the published PDF size and SHA-256:

```bash
arxivctl dataset verify BATCH_ID
```

Expected successful verification:

```text
Expected: 1
Checked: 1
Valid: 1
Missing/corrupt: 0
```

Submit the larger demonstration list:

```bash
arxivctl task submit \
  --name mac-demo \
  --input demo/demo_ids.txt
```

Generate an ID file for papers first submitted during an inclusive UTC date
range. This command queries arXiv directly and does not contact `arxivd`:

```bash
arxivctl task create \
  --start-date 2020-01-01 \
  --end-date 2020-12-31 \
  --output .local/arxiv_ids_2020.txt
```

The command first partitions cross-month ranges by calendar month. If arXiv
reports `totalResults >= 10000` for any window, it recursively bisects that
window at a UTC date boundary before paging it. This avoids arXiv's observed
HTTP 500 response when the pagination cursor reaches `start=10000`.

The requested output remains the deduplicated aggregate file used by
`task submit`. Auditable leaf-window files are written under a sibling parts
directory with names that state their exact inclusive date range, for example:

```text
.local/arxiv_ids_2020.txt
.local/arxiv_ids_2020.parts/arxiv_ids_2020-01-01_2020-01-16.txt
.local/arxiv_ids_2020.parts/arxiv_ids_2020-01-17_2020-01-31.txt
```

The actual split dates depend on the result count. If one UTC day alone still
contains at least 10000 results, `task create` exits with
`RESULT_WINDOW_TOO_LARGE` instead of writing an incomplete aggregate.

Add `--debug` when diagnosing a slow query or an arXiv API error. It prints
each date window, page offset, request URL, response status and duration,
pagination counts, and a response-body preview for failed responses:

```bash
arxivctl task create \
  --start-date 2020-01-01 \
  --end-date 2020-01-31 \
  --output .local/arxiv_ids_2020-01.txt \
  --debug
```

Submit the generated file to `arxivd` as a download batch:

```bash
arxivctl task submit \
  --name submitted-2020 \
  --input .local/arxiv_ids_2020.txt
```

`task submit` returns as soon as the ID list is durably queued. Metadata
resolution and PDF downloads continue inside `arxivd`; use the returned batch
ID with `task show` or `task progress --watch` to monitor both metadata import
and download progress.

`task create` follows arXiv API pagination, orders results by submission date,
and writes one normalized current-version ID per paper. Set `ARXIV_USER_AGENT`
to a value containing a real contact address before a large query.

Cancel an active batch:

```bash
arxivctl task cancel BATCH_ID
```

Requeue tasks that reached `FAILED` after exhausting their attempts:

```bash
arxivctl task retry-failed BATCH_ID
```

Cancelled tasks are not retried by `retry-failed`; run `task submit` again if
you want to create a new batch from a cancelled input list.

Create a consistent SQLite backup:

```bash
arxivctl database export --output .local/exports/arxiv-before.sqlite3
```

The downloaded development PDFs are stored below:

```text
.local/storage-volume/arxiv/objects/pdf/submitted/YYYY/MM/DD/
```

For example:

```bash
find .local/storage-volume/arxiv/objects -type f -name '*.pdf'
```

## macOS troubleshooting

### SQLite reports `unable to open database file`

Create the `.local` directory and make sure it is writable by the user running
`arxivd`:

```bash
mkdir -p .local
test -w .local
```

### The daemon reports `MOUNT_NOT_AVAILABLE`

Confirm that the development configuration is selected and recreate the local
sentinel if necessary:

```bash
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.dev.toml"
mkdir -p .local/storage-volume/arxiv .local/cache
touch .local/storage-volume/arxiv/.mount_sentinel
```

Do not change the Linux production configuration to local mode.

### Port 8765 is already in use

Find the process that owns the daemon port:

```bash
lsof -nP -iTCP:8765 -sTCP:LISTEN
```

Stop the previous `arxivd` process before starting another one.

## Linux mounted-storage deployment

Linux production uses `storage.mode = "mounted"` from
`config/config.example.toml`. That configuration defaults to SQLite at
`/var/lib/arxiv-downloader/arxiv.db`:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'

cp config/config.example.toml config/config.toml
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.toml"

alembic upgrade head
arxivd
```

The production daemon refuses to start unless `/mnt` is a real mount point and
`/mnt/arxiv/.mount_sentinel` already exists. An administrator—not the daemon—
must create the sentinel after verifying the mounted volume. A hardened example
systemd unit is available at `deploy/arxivd.service`.

To use PostgreSQL instead, install PostgreSQL/asyncpg and set the environment
variable before migrations and daemon startup. The environment value overrides
the SQLite URL in the TOML file:

```bash
export ARXIV_DATABASE_URL='postgresql+asyncpg://arxiv:password@127.0.0.1/arxiv'
alembic upgrade head
arxivd
```

For systemd, put `ARXIV_DATABASE_URL=...` in
`/etc/arxiv-downloader/arxivd.env`. SQLite backups are regular `.sqlite3`
files created through the online backup API; PostgreSQL exports remain custom
format `pg_dump` files.

## Request rate limiting

`arxivd` uses one process-wide `AsyncRateLimiter` for both arXiv metadata
requests and PDF downloads. Sharing one limiter prevents the metadata client
and two download workers from each independently using the full request rate.

Configure the minimum interval between HTTP request starts in either
`config/config.dev.toml` or the production configuration:

```toml
[download]
concurrency = 2
min_request_interval_seconds = 3
```

With a three-second interval, the daemon starts at most approximately 20 arXiv
requests per minute in total:

```text
60 seconds / 3 seconds = 20 request starts per minute
```

Two PDFs can still be downloading concurrently when a transfer lasts longer
than three seconds, but their requests cannot start less than three seconds
apart. The limiter controls request cadence rather than bandwidth in bytes per
second.

The implementation is in `src/arxiv_downloader/downloader/rate_limit.py`:

```python
class AsyncRateLimiter:
    """Enforce a process-wide minimum interval between request starts."""

    def __init__(self, minimum_interval_seconds: float) -> None:
        self.minimum_interval = minimum_interval_seconds
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            delay = self._next_allowed - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_allowed = loop.time() + self.minimum_interval
```

The daemon creates a single instance and passes it to both clients in
`src/arxiv_downloader/daemon/app.py`:

```python
limiter = AsyncRateLimiter(
    resolved_settings.download.min_request_interval_seconds
)
metadata = ArxivMetadataClient(resolved_settings.download, limiter)
downloader = PDFDownloader(
    resolved_settings.download,
    resolved_settings.storage.staging_root,
    limiter,
)
```

Each client waits immediately before starting its HTTP request:

```python
# Metadata request
await self._limiter.wait()
response = await self._client.get(...)

# PDF request
await self._limiter.wait()
async with self._client.stream("GET", pdf_url) as response:
    ...
```

To reduce traffic further, increase the interval and restart `arxivd`:

```toml
min_request_interval_seconds = 5  # approximately 12 starts per minute
```

Current A0 limitations:

- The limiter controls request starts, not transfer bandwidth.
- HTTP 429 responses are classified, but `Retry-After` is not yet honored.
- PDF attempts are retried without exponential backoff.
- Metadata requests are not automatically retried after HTTP 5xx failures.
- An interval of `0` disables pacing and should only be used with mocked tests.

## Tests

The unit suite uses temporary SQLite databases, paths, and mocked HTTP/mount
checks; PostgreSQL, arXiv, and `/mnt` are not required:

```bash
source .venv/bin/activate
pytest
ruff check src tests migrations
ruff format --check src tests migrations
```

## Configuration notes

- Configuration defaults to `/etc/arxiv-downloader/config.toml`; override it
  with `ARXIV_DOWNLOADER_CONFIG`.
- The database URL comes from `database.url` by default. The environment
  variable named by `database.dsn_env`, normally `ARXIV_DATABASE_URL`, takes
  precedence and is redacted in logs.
- Supported URLs are `sqlite+aiosqlite:///...` and
  `postgresql+asyncpg://...`; the driver suffix can be omitted and is added
  automatically.
- SQLite is configured with foreign keys, WAL, and a 30-second busy timeout.
  A0 still runs exactly one `arxivd` process.
- The control API is restricted to `127.0.0.1:8765`.
- Apply Alembic migrations before starting the daemon.
- `storage.mode = "local"` is only for local development.
- `storage.mode = "mounted"` is the production default and enforces both
  `mountpoint` and `findmnt` checks before writing final files.
