from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from arxiv_downloader.cli import app as cli_module
from arxiv_downloader.ids import ArxivId

runner = CliRunner()


def test_task_create_generates_date_range_id_file(tmp_path, monkeypatch):
    output = tmp_path / "ids" / "2020.txt"
    captured = {}

    def fake_fetch(start, end, **kwargs):
        captured.update(start=start, end=end, kwargs=kwargs)
        return [ArxivId("2001.00001", 1), ArxivId("2001.00002", 3)]

    monkeypatch.setattr(cli_module, "fetch_submitted_ids", fake_fetch)

    result = runner.invoke(
        cli_module.app,
        [
            "task",
            "create",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2020-12-31",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert output.read_text() == "2001.00001v1\n2001.00002v3\n"
    assert captured["start"] == date(2020, 1, 1)
    assert captured["end"] == date(2020, 12, 31)
    assert "Papers: 2" in result.output


def test_task_create_debug_prints_query_diagnostics(tmp_path, monkeypatch):
    output = tmp_path / "ids.txt"

    def fake_fetch(*args, **kwargs):
        kwargs["debug"]("response status=500 elapsed=0.123s")
        return []

    monkeypatch.setattr(cli_module, "fetch_submitted_ids", fake_fetch)

    result = runner.invoke(
        cli_module.app,
        [
            "task",
            "create",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2020-01-01",
            "--output",
            str(output),
            "--debug",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "[task create debug] response status=500 elapsed=0.123s" in result.output


def test_task_create_writes_date_named_query_parts(tmp_path, monkeypatch):
    output = tmp_path / "january.txt"

    def fake_fetch(start, end, **kwargs):
        kwargs["window_callback"](
            date(2020, 1, 1),
            date(2020, 1, 16),
            (ArxivId("2001.00001", 1),),
        )
        kwargs["window_callback"](
            date(2020, 1, 17),
            date(2020, 1, 31),
            (ArxivId("2001.00002", 2),),
        )
        return [ArxivId("2001.00001", 1), ArxivId("2001.00002", 2)]

    monkeypatch.setattr(cli_module, "fetch_submitted_ids", fake_fetch)

    result = runner.invoke(
        cli_module.app,
        [
            "task",
            "create",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2020-01-31",
            "--output",
            str(output),
        ],
    )

    parts = tmp_path / "january.parts"
    assert result.exit_code == 0, result.output
    assert output.read_text() == "2001.00001v1\n2001.00002v2\n"
    assert (parts / "arxiv_ids_2020-01-01_2020-01-16.txt").read_text() == "2001.00001v1\n"
    assert (parts / "arxiv_ids_2020-01-17_2020-01-31.txt").read_text() == "2001.00002v2\n"
    assert "2020-01-01 to 2020-01-16 inclusive, 1 papers" in result.output


def test_task_create_rejects_reversed_dates(monkeypatch):
    def unexpected_fetch(*args, **kwargs):
        raise AssertionError("arXiv must not be queried for an invalid date range")

    monkeypatch.setattr(cli_module, "fetch_submitted_ids", unexpected_fetch)

    result = runner.invoke(
        cli_module.app,
        [
            "task",
            "create",
            "--start-date",
            "2020-02-01",
            "--end-date",
            "2020-01-01",
        ],
    )

    assert result.exit_code == 2
    assert "must not be after" in result.output


def test_task_submit_posts_id_file_to_daemon(tmp_path, monkeypatch):
    input_file = tmp_path / "ids.txt"
    input_file.write_text("# generated\n2001.00001v1\n\n2001.00002v3\n")
    captured = {}

    def fake_request(method, path, **kwargs):
        captured.update(method=method, path=path, kwargs=kwargs)
        return {
            "batch_id": "batch-1",
            "papers_accepted": 2,
            "duplicates_skipped": 0,
            "invalid_ids": [],
            "metadata_errors": [],
        }

    monkeypatch.setattr(cli_module, "_request", fake_request)

    result = runner.invoke(
        cli_module.app,
        ["task", "submit", "--name", "server-demo", "--input", str(input_file)],
    )

    assert result.exit_code == 0, result.output
    assert captured["method"] == "POST"
    assert captured["path"] == "/v1/batches"
    assert captured["kwargs"]["json"] == {
        "name": "server-demo",
        "arxiv_ids": ["2001.00001v1", "2001.00002v3"],
    }
    assert captured["kwargs"]["timeout"] is None
    assert "Batch created: batch-1" in result.output


def test_task_create_default_output_name(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli_module, "fetch_submitted_ids", lambda *args, **kwargs: [ArxivId("2001.00001", 1)]
    )

    result = runner.invoke(
        cli_module.app,
        [
            "task",
            "create",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2020-01-02",
        ],
    )

    assert result.exit_code == 0, result.output
    expected = Path("arxiv_ids_2020-01-01_2020-01-02.txt")
    assert expected.read_text() == "2001.00001v1\n"


def test_task_create_writes_empty_file_when_range_has_no_results(tmp_path, monkeypatch):
    output = tmp_path / "empty.txt"
    monkeypatch.setattr(cli_module, "fetch_submitted_ids", lambda *args, **kwargs: [])

    result = runner.invoke(
        cli_module.app,
        [
            "task",
            "create",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2020-01-02",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert output.read_text() == ""
    assert "Papers: 0" in result.output
