from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import typer
from sqlalchemy.engine import make_url

from arxiv_downloader.config import load_settings
from arxiv_downloader.errors import MetadataError
from arxiv_downloader.ids import ArxivId
from arxiv_downloader.metadata.query import DEFAULT_API_URL, fetch_submitted_ids

app = typer.Typer(help="Control the local arxivd service.", no_args_is_help=True)
daemon_app = typer.Typer(help="Inspect the daemon.")
task_app = typer.Typer(help="Generate ID files and manage download batches.")
dataset_app = typer.Typer(help="Verify downloaded datasets.")
database_app = typer.Typer(help="Perform manual database operations.")
app.add_typer(daemon_app, name="daemon")
app.add_typer(task_app, name="task")
app.add_typer(dataset_app, name="dataset")
app.add_typer(database_app, name="database")


def _base_url() -> str:
    return os.environ.get("ARXIVD_URL", "http://127.0.0.1:8765").rstrip("/")


def _request(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    timeout = kwargs.pop("timeout", 130)
    try:
        response = httpx.request(method, f"{_base_url()}{path}", timeout=timeout, **kwargs)
    except httpx.HTTPError as exc:
        typer.echo(f"Could not contact arxivd: {exc}", err=True)
        raise typer.Exit(1) from exc
    if response.is_error:
        try:
            detail = response.json().get("detail", response.text)
        except (ValueError, AttributeError):
            detail = response.text
        typer.echo(f"arxivd returned HTTP {response.status_code}: {detail}", err=True)
        raise typer.Exit(1)
    return response.json()


def _print_progress(data: dict[str, Any]) -> None:
    typer.echo(f"Batch:      {data['batch_id']}")
    typer.echo(f"Name:       {data['name']}")
    typer.echo(f"State:      {data['state']}")
    typer.echo(f"Total:      {data['total']}")
    typer.echo(f"Metadata pending:  {data.get('metadata_pending', 0)}")
    typer.echo(f"Metadata running:  {data.get('metadata_running', 0)}")
    typer.echo(f"Metadata imported: {data.get('metadata_succeeded', 0)}")
    typer.echo(f"Metadata failed:   {data.get('metadata_failed', 0)}")
    typer.echo(f"Pending:    {data['pending']}")
    typer.echo(f"Running:    {data['running']}")
    typer.echo(f"Succeeded:  {data['succeeded']}")
    typer.echo(f"Failed:     {data['failed']}")
    typer.echo(f"Cancelled:  {data['cancelled']}")
    typer.echo(f"Progress:   {data['progress_percent']}%")
    for error in data.get("errors", []):
        typer.echo(f"Error:      {error['arxiv_id']} [{error['code']}] {error['message']}")


@daemon_app.command("status")
def daemon_status() -> None:
    """Check database and storage health."""
    data = _request("GET", "/health")
    typer.echo(f"Status:   {data['status']}")
    typer.echo(f"Database: {data['database']}")
    typer.echo(f"Storage:  {data['storage']}")
    if data["status"] != "ok":
        raise typer.Exit(1)


def _read_identifiers(input_file: Path) -> list[str]:
    return [
        line.strip()
        for line in input_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def _parse_date(value: str, option_name: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        typer.echo(f"{option_name} must use YYYY-MM-DD format.", err=True)
        raise typer.Exit(2) from exc
    if parsed.isoformat() != value:
        typer.echo(f"{option_name} must use YYYY-MM-DD format.", err=True)
        raise typer.Exit(2)
    return parsed


def _write_identifiers(output: Path, identifiers: list[str]) -> None:
    output = output.expanduser().resolve(strict=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write("".join(f"{identifier}\n" for identifier in identifiers))
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, output)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _write_identifier_parts(
    output: Path, windows: list[tuple[date, date, tuple[ArxivId, ...]]]
) -> tuple[Path, list[tuple[Path, date, date, int]]]:
    output = output.expanduser().resolve(strict=False)
    parts_directory = output.with_name(f"{output.stem}.parts")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_directory = Path(
        tempfile.mkdtemp(dir=output.parent, prefix=f".{parts_directory.name}.")
    )
    backup_directory: Path | None = None
    written: list[tuple[Path, date, date, int]] = []
    try:
        for window_start, window_end, identifiers in windows:
            filename = f"arxiv_ids_{window_start}_{window_end}.txt"
            part_path = temporary_directory / filename
            _write_identifiers(part_path, [identifier.full_id for identifier in identifiers])
            written.append(
                (parts_directory / filename, window_start, window_end, len(identifiers))
            )

        if parts_directory.exists():
            backup_directory = Path(
                tempfile.mkdtemp(dir=output.parent, prefix=f".{parts_directory.name}.backup.")
            )
            backup_directory.rmdir()
            os.replace(parts_directory, backup_directory)
        os.replace(temporary_directory, parts_directory)
        if backup_directory is not None:
            shutil.rmtree(backup_directory)
    except Exception:
        if backup_directory is not None and backup_directory.exists():
            if parts_directory.exists():
                shutil.rmtree(parts_directory)
            os.replace(backup_directory, parts_directory)
        raise
    finally:
        if temporary_directory.exists():
            shutil.rmtree(temporary_directory)
    return parts_directory, written


@task_app.command("create")
def task_create(
    start_date: str = typer.Option(
        ..., "--start-date", "--submitted-from", "--start", help="First submission date (YYYY-MM-DD)."
    ),
    end_date: str = typer.Option(
        ..., "--end-date", "--submitted-to", "--end", help="Last submission date, inclusive (YYYY-MM-DD)."
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o", dir_okay=False, help="Generated ID file path."
    ),
    page_size: int = typer.Option(200, min=1, max=2000, help="arXiv results per page."),
    request_interval: float = typer.Option(
        3, min=0, help="Delay between arXiv API pages in seconds."
    ),
    debug: bool = typer.Option(
        False, "--debug", help="Print arXiv request, response, and pagination diagnostics."
    ),
    api_url: str = typer.Option(DEFAULT_API_URL, hidden=True),
) -> None:
    """Generate an arXiv ID file for an inclusive submission-date range."""
    start = _parse_date(start_date, "--start-date")
    end = _parse_date(end_date, "--end-date")
    if start > end:
        typer.echo("--start-date must not be after --end-date.", err=True)
        raise typer.Exit(2)
    destination = output or Path(f"arxiv_ids_{start}_{end}.txt")
    query_windows: list[tuple[date, date, tuple[ArxivId, ...]]] = []
    debug_callback = (
        lambda message: typer.echo(f"[task create debug] {message}", err=True)
    ) if debug else None
    try:
        identifiers = fetch_submitted_ids(
            start,
            end,
            page_size=page_size,
            request_interval_seconds=request_interval,
            user_agent=os.environ.get(
                "ARXIV_USER_AGENT", "arxiv-downloader-a0/0.1 contact@example.com"
            ),
            api_url=api_url,
            debug=debug_callback,
            window_callback=lambda window_start, window_end, window_ids: query_windows.append(
                (window_start, window_end, window_ids)
            ),
        )
    except MetadataError as exc:
        typer.echo(f"Could not query arXiv [{exc.code}]: {exc}", err=True)
        raise typer.Exit(1) from exc
    parts_directory: Path | None = None
    part_files: list[tuple[Path, date, date, int]] = []
    if query_windows:
        parts_directory, part_files = _write_identifier_parts(destination, query_windows)
    _write_identifiers(destination, [identifier.full_id for identifier in identifiers])
    typer.echo(f"ID file created: {destination.expanduser().resolve(strict=False)}")
    typer.echo(f"Papers: {len(identifiers)}")
    typer.echo(f"Submission dates (UTC): {start} to {end} inclusive")
    if parts_directory is not None:
        typer.echo(f"Query parts: {parts_directory}")
        for part_path, window_start, window_end, count in part_files:
            typer.echo(
                f"  {part_path.name}: {window_start} to {window_end} inclusive, "
                f"{count} papers"
            )


@task_app.command("submit")
def task_submit(
    name: str = typer.Option(..., help="Human-readable batch name."),
    input_file: Path = typer.Option(
        ..., "--input", exists=True, dir_okay=False, readable=True, help="One arXiv ID per line."
    ),
) -> None:
    """Submit an arXiv ID file to arxivd as a download batch."""
    identifiers = _read_identifiers(input_file)
    if not identifiers:
        typer.echo("Input file contains no arXiv IDs.", err=True)
        raise typer.Exit(2)
    data = _request(
        "POST",
        "/v1/batches",
        json={"name": name, "arxiv_ids": identifiers},
        timeout=None,
    )
    typer.echo(f"Batch created: {data['batch_id']}")
    typer.echo(f"Papers queued: {data.get('queued_count', data['papers_accepted'])}")
    if "metadata_pending" in data:
        typer.echo(f"Metadata pending: {data['metadata_pending']}")
    typer.echo(f"Duplicates skipped: {data['duplicates_skipped']}")
    typer.echo(f"Invalid IDs: {len(data['invalid_ids'])}")
    for invalid in data["invalid_ids"]:
        typer.echo(f"  INVALID_ID: {invalid}")
    for error in data["metadata_errors"]:
        typer.echo(f"  {error['code']}: {error['arxiv_id']} - {error['message']}")


@task_app.command("show")
def task_show(batch_id: str) -> None:
    """Show the current state of a batch."""
    _print_progress(_request("GET", f"/v1/batches/{batch_id}"))


@task_app.command("progress")
def task_progress(
    batch_id: str,
    watch: bool = typer.Option(False, "--watch", help="Poll until the batch is terminal."),
    interval: float = typer.Option(2.0, min=0.25, help="Watch polling interval in seconds."),
) -> None:
    """Display batch counts, optionally until completion."""
    while True:
        data = _request("GET", f"/v1/batches/{batch_id}")
        _print_progress(data)
        if not watch or data["state"] in {"COMPLETED", "CANCELLED"}:
            return
        typer.echo("")
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            return


@task_app.command("cancel")
def task_cancel(batch_id: str) -> None:
    """Cancel pending and active tasks in a batch."""
    data = _request("POST", f"/v1/batches/{batch_id}/cancel")
    typer.echo(f"Batch state: {data['state']}")
    typer.echo(f"Tasks cancelled: {data['affected_tasks']}")


@task_app.command("retry-failed")
def task_retry_failed(batch_id: str) -> None:
    """Reset failed tasks so workers execute them again."""
    data = _request("POST", f"/v1/batches/{batch_id}/retry-failed")
    typer.echo(f"Batch state: {data['state']}")
    typer.echo(f"Tasks requeued: {data['affected_tasks']}")


@dataset_app.command("verify")
def dataset_verify(batch_id: str) -> None:
    """Recompute size and SHA-256 checks for a batch."""
    data = _request("POST", f"/v1/batches/{batch_id}/verify")
    typer.echo(f"Expected: {data['expected']}")
    typer.echo(f"Checked: {data['checked']}")
    typer.echo(f"Valid: {data['valid']}")
    typer.echo(f"Missing/corrupt: {data['missing_or_corrupt']}")
    for issue in data["issues"]:
        typer.echo(f"  {issue['arxiv_id']} [{issue['code']}] {issue['message']}")
    if data["missing_or_corrupt"]:
        raise typer.Exit(1)


@database_app.command("export")
def database_export(
    output: Path = typer.Option(..., dir_okay=False, help="Destination database backup file."),
) -> None:
    """Create a one-off SQLite backup or PostgreSQL custom-format dump."""
    settings = load_settings()
    url = make_url(settings.database.url)
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if url.get_backend_name() == "sqlite":
        if not url.database or url.database == ":memory:":
            typer.echo("An in-memory SQLite database cannot be exported.", err=True)
            raise typer.Exit(2)
        source = Path(url.database).resolve()
        if source == output:
            typer.echo("SQLite backup output must differ from the live database.", err=True)
            raise typer.Exit(2)
        if not source.is_file():
            typer.echo(f"SQLite database does not exist: {source}", err=True)
            raise typer.Exit(1)
        try:
            with sqlite3.connect(source) as source_connection:
                with sqlite3.connect(output) as output_connection:
                    source_connection.backup(output_connection)
        except sqlite3.Error as exc:
            typer.echo(f"SQLite backup failed: {exc}", err=True)
            raise typer.Exit(1) from exc
        typer.echo(f"Database exported: {output}")
        return
    if url.get_backend_name() != "postgresql":
        typer.echo("Database export supports only SQLite and PostgreSQL.", err=True)
        raise typer.Exit(2)
    environment = os.environ.copy()
    if url.host:
        environment["PGHOST"] = url.host
    if url.port:
        environment["PGPORT"] = str(url.port)
    if url.username:
        environment["PGUSER"] = url.username
    if url.password:
        environment["PGPASSWORD"] = url.password
    if url.database:
        environment["PGDATABASE"] = url.database
    sslmode = url.query.get("sslmode")
    if sslmode:
        environment["PGSSLMODE"] = sslmode
    try:
        result = subprocess.run(
            ["pg_dump", "--format=custom", "--file", str(output)],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        typer.echo("pg_dump is not installed or is not on PATH.", err=True)
        raise typer.Exit(1) from exc
    if result.returncode != 0:
        typer.echo(f"pg_dump failed: {result.stderr.strip()}", err=True)
        raise typer.Exit(result.returncode)
    typer.echo(f"Database exported: {output}")


if __name__ == "__main__":
    app()
