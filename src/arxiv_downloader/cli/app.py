from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx
import typer
from sqlalchemy.engine import make_url

from arxiv_downloader.config import load_settings

app = typer.Typer(help="Control the local arxivd service.", no_args_is_help=True)
daemon_app = typer.Typer(help="Inspect the daemon.")
task_app = typer.Typer(help="Create and manage download batches.")
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


@task_app.command("create")
def task_create(
    name: str = typer.Option(..., help="Human-readable batch name."),
    input_file: Path = typer.Option(
        ..., "--input", exists=True, dir_okay=False, readable=True, help="One arXiv ID per line."
    ),
) -> None:
    """Create a batch from a text file."""
    identifiers = [
        line.strip()
        for line in input_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not identifiers:
        typer.echo("Input file contains no arXiv IDs.", err=True)
        raise typer.Exit(2)
    if len(identifiers) > 50:
        typer.echo("A0 accepts at most 50 input lines per batch.", err=True)
        raise typer.Exit(2)
    data = _request(
        "POST",
        "/v1/batches",
        json={"name": name, "arxiv_ids": identifiers},
        timeout=600,
    )
    typer.echo(f"Batch created: {data['batch_id']}")
    typer.echo(f"Papers accepted: {data['papers_accepted']}")
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
    output: Path = typer.Option(..., dir_okay=False, help="Destination for a custom pg_dump file."),
) -> None:
    """Create a one-off PostgreSQL custom-format dump."""
    settings = load_settings()
    url = make_url(settings.database.url)
    if url.get_backend_name() != "postgresql":
        typer.echo("Database export requires PostgreSQL.", err=True)
        raise typer.Exit(2)
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
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
