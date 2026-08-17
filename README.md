# arxiv-downloader A0

`arxiv-downloader` is the runnable A0 implementation described in
[`A0.md`](A0.md). A short-lived `arxivctl` client talks to the persistent
`arxivd` service, which stores task state in PostgreSQL and publishes verified
PDFs to protected storage.

- macOS development uses repository-local storage under `.local/`.
- Linux deployment uses a mounted volume under `/mnt/arxiv`.
- The control API listens only on `127.0.0.1:8765`.

## Run locally on macOS

### 1. Prerequisites

You need Python 3.11 or newer, Homebrew, PostgreSQL, and internet access to
arXiv. The commands below use Homebrew Python 3.14, matching the tested macOS
environment.

Install Python and PostgreSQL if they are not already installed, then start
PostgreSQL:

```bash
brew install python@3.14
brew install postgresql@18
brew services start postgresql@18
export PATH="$(brew --prefix postgresql@18)/bin:$PATH"
```

Confirm that PostgreSQL accepts TCP connections:

```bash
pg_isready -h 127.0.0.1 -p 5432
```

If a Docker PostgreSQL container is already using port 5432, stop it first:

```bash
docker stop arxiv-postgres
```

Stopping the container does not delete its data.

### 2. Create the local database

Homebrew PostgreSQL normally creates a database role matching your macOS user.
Create the application database with that role:

```bash
createdb arxiv
psql -d arxiv -c 'SELECT current_user, current_database();'
```

If `createdb` reports that `arxiv` already exists, continue with the existing
database.

### 3. Install the Python project

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

### 4. Configure the database and local storage

The included `config/config.dev.toml` selects explicit local-development
storage. It skips Linux-only mount detection but retains sentinel, path,
checksum, quarantine, and publication checks.

```bash
export ARXIV_DATABASE_URL="postgresql+asyncpg://${USER}@127.0.0.1:5432/arxiv"
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.dev.toml"

mkdir -p .local/storage-volume/arxiv .local/cache
touch .local/storage-volume/arxiv/.mount_sentinel
```

Before larger downloads, edit `config/config.dev.toml` and put a real contact
address in the `download.user_agent` value.

The `.local/` directory is ignored by Git.

### 5. Create or update the database schema

```bash
alembic upgrade head
alembic current
```

The expected current revision is:

```text
0001_a0_schema (head)
```

You can inspect the tables with:

```bash
psql -d arxiv -c '\dt'
```

### 6. Start the daemon

Keep the following command running in the first terminal:

```bash
cd /path/to/arxiv_downloader
source .venv/bin/activate
export ARXIV_DATABASE_URL="postgresql+asyncpg://${USER}@127.0.0.1:5432/arxiv"
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.dev.toml"
arxivd
```

A successful startup ends with:

```text
Uvicorn running on http://127.0.0.1:8765
```

Press `Ctrl+C` to stop the daemon cleanly.

### 7. Use `arxivctl` from another terminal

Activate the project environment in the second terminal:

```bash
cd /path/to/arxiv_downloader
source .venv/bin/activate
export ARXIV_DATABASE_URL="postgresql+asyncpg://${USER}@127.0.0.1:5432/arxiv"
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
arxivctl task create \
  --name mac-smoke-test \
  --input demo/smoke_ids.txt
```

The command returns a batch ID:

```text
Batch created: cf6c57d9-e7a2-4350-acbb-22890bd65326
Papers accepted: 1
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
arxivctl task create \
  --name mac-demo \
  --input demo/demo_ids.txt
```

Cancel an active batch:

```bash
arxivctl task cancel BATCH_ID
```

Requeue tasks that reached `FAILED` after exhausting their attempts:

```bash
arxivctl task retry-failed BATCH_ID
```

Cancelled tasks are not retried by `retry-failed`; create a new batch if you
want to resubmit a cancelled input list.

Create a manual PostgreSQL backup:

```bash
arxivctl database export --output .local/exports/arxiv-before.dump
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

### PostgreSQL is not accepting connections

```bash
brew services list
brew services restart postgresql@18
pg_isready -h 127.0.0.1 -p 5432
```

### PostgreSQL reports `role "arxiv" does not exist`

The local Homebrew setup uses your macOS user rather than a Docker role named
`arxiv`. Reset the URL in the terminal that starts `arxivd`:

```bash
export ARXIV_DATABASE_URL="postgresql+asyncpg://${USER}@127.0.0.1:5432/arxiv"
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
`config/config.example.toml`:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'

export ARXIV_DATABASE_URL='postgresql+asyncpg://arxiv:password@127.0.0.1/arxiv'
cp config/config.example.toml config/config.toml
export ARXIV_DOWNLOADER_CONFIG="$PWD/config/config.toml"

alembic upgrade head
arxivd
```

The production daemon refuses to start unless `/mnt` is a real mount point and
`/mnt/arxiv/.mount_sentinel` already exists. An administrator—not the daemon—
must create the sentinel after verifying the mounted volume. A hardened example
systemd unit is available at `deploy/arxivd.service`.

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

The unit suite uses temporary paths and mocked HTTP/mount checks; PostgreSQL,
arXiv, and `/mnt` are not required:

```bash
source .venv/bin/activate
pytest
ruff check src tests migrations
ruff format --check src tests migrations
```

## Configuration notes

- Configuration defaults to `/etc/arxiv-downloader/config.toml`; override it
  with `ARXIV_DOWNLOADER_CONFIG`.
- The database URL is read only from the environment variable named by
  `database.dsn_env`, normally `ARXIV_DATABASE_URL`, and is redacted in logs.
- The control API is restricted to `127.0.0.1:8765`.
- Apply Alembic migrations before starting the daemon.
- `storage.mode = "local"` is only for local development.
- `storage.mode = "mounted"` is the production default and enforces both
  `mountpoint` and `findmnt` checks before writing final files.
