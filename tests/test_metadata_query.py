from datetime import date

import httpx
import pytest

from arxiv_downloader.errors import MetadataError
from arxiv_downloader.metadata.query import fetch_submitted_ids


def atom_page(total: int, identifiers: list[str]) -> bytes:
    entries = "".join(
        f"<entry><id>http://arxiv.org/abs/{identifier}</id></entry>"
        for identifier in identifiers
    )
    return f"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <opensearch:totalResults>{total}</opensearch:totalResults>
  {entries}
</feed>
""".encode()


def test_date_range_query_paginates_and_deduplicates_base_ids():
    requests = []
    pages = {
        "0": atom_page(4, ["2001.00001v1", "2001.00002v2"]),
        "2": atom_page(4, ["2001.00002v3", "2001.00003v1"]),
    }

    def handler(request):
        requests.append(request)
        query = request.url.params["search_query"]
        if query == "submittedDate:[202001010000 TO 202001312359]":
            content = pages[request.url.params["start"]]
        else:
            content = atom_page(0, [])
        return httpx.Response(200, content=content)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        identifiers = fetch_submitted_ids(
            date(2020, 1, 1),
            date(2020, 12, 31),
            page_size=2,
            request_interval_seconds=0,
            client=client,
        )

    assert [identifier.full_id for identifier in identifiers] == [
        "2001.00001v1",
        "2001.00002v3",
        "2001.00003v1",
    ]
    assert [request.url.params["start"] for request in requests[:2]] == ["0", "2"]
    assert requests[0].url.params["search_query"] == (
        "submittedDate:[202001010000 TO 202001312359]"
    )
    assert requests[-1].url.params["search_query"] == (
        "submittedDate:[202012010000 TO 202012312359]"
    )
    assert requests[0].url.params["sortBy"] == "submittedDate"
    assert requests[0].url.params["sortOrder"] == "ascending"


def test_date_range_query_rejects_incomplete_result_set():
    pages = {
        "0": atom_page(3, ["2001.00001v1", "2001.00002v1"]),
        "2": atom_page(3, []),
    }

    def handler(request):
        return httpx.Response(200, content=pages[request.url.params["start"]])

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(MetadataError, match="incomplete"):
            fetch_submitted_ids(
                date(2020, 1, 1),
                date(2020, 1, 1),
                page_size=2,
                request_interval_seconds=0,
                client=client,
            )


def test_date_range_query_debug_reports_pagination_and_error_response():
    messages = []

    def handler(request):
        return httpx.Response(
            500,
            headers={"Content-Type": "text/plain", "Retry-After": "30"},
            content=b"temporary upstream failure",
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(MetadataError, match="returned 500"):
            fetch_submitted_ids(
                date(2020, 1, 1),
                date(2020, 1, 1),
                request_interval_seconds=0,
                client=client,
                debug=messages.append,
            )

    output = "\n".join(messages)
    assert "window=2020-01-01..2020-01-01" in output
    assert "request start=0 max_results=200" in output
    assert "response status=500" in output
    assert "retry-after='30'" in output
    assert "temporary upstream failure" in output


def test_date_range_query_splits_windows_at_arxiv_pagination_limit():
    requests = []
    windows = []

    def handler(request):
        requests.append(request)
        query = request.url.params["search_query"]
        if query == "submittedDate:[202001010000 TO 202001042359]":
            return httpx.Response(200, content=atom_page(10_000, ["2001.00001v1"]))
        if query == "submittedDate:[202001010000 TO 202001022359]":
            return httpx.Response(200, content=atom_page(1, ["2001.00001v1"]))
        if query == "submittedDate:[202001030000 TO 202001042359]":
            return httpx.Response(200, content=atom_page(1, ["2001.00002v2"]))
        raise AssertionError(f"unexpected query: {query}")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        identifiers = fetch_submitted_ids(
            date(2020, 1, 1),
            date(2020, 1, 4),
            request_interval_seconds=0,
            client=client,
            window_callback=lambda start, end, ids: windows.append((start, end, ids)),
        )

    assert [identifier.full_id for identifier in identifiers] == [
        "2001.00001v1",
        "2001.00002v2",
    ]
    assert [(start, end) for start, end, _ in windows] == [
        (date(2020, 1, 1), date(2020, 1, 2)),
        (date(2020, 1, 3), date(2020, 1, 4)),
    ]
    assert len(requests) == 3


def test_date_range_query_rejects_single_day_above_pagination_limit():
    def handler(request):
        return httpx.Response(200, content=atom_page(10_000, ["2001.00001v1"]))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(MetadataError) as error:
            fetch_submitted_ids(
                date(2020, 1, 1),
                date(2020, 1, 1),
                request_interval_seconds=0,
                client=client,
            )

    assert error.value.code == "RESULT_WINDOW_TOO_LARGE"
    assert "single-day window" in str(error.value)
    assert "start=10000" in str(error.value)
