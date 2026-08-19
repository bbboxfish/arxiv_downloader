from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import date, timedelta

import httpx

from arxiv_downloader.errors import InvalidArxivId, MetadataError
from arxiv_downloader.ids import ArxivId, normalize_arxiv_id

ATOM = "{http://www.w3.org/2005/Atom}"
OPENSEARCH = "{http://a9.com/-/spec/opensearch/1.1/}"
DEFAULT_API_URL = "https://export.arxiv.org/api/query"
MAX_PAGINATION_OFFSET = 10_000
DebugCallback = Callable[[str], None]
WindowCallback = Callable[[date, date, tuple[ArxivId, ...]], None]


def fetch_submitted_ids(
    start_date: date,
    end_date: date,
    *,
    page_size: int = 200,
    request_interval_seconds: float = 3,
    request_timeout_seconds: float = 120,
    user_agent: str = "arxiv-downloader-a0/0.1 contact@example.com",
    api_url: str = DEFAULT_API_URL,
    client: httpx.Client | None = None,
    debug: DebugCallback | None = None,
    window_callback: WindowCallback | None = None,
) -> list[ArxivId]:
    if start_date > end_date:
        raise ValueError("start date must not be after end date")
    if not 1 <= page_size <= 2000:
        raise ValueError("page size must be between 1 and 2000")
    if request_interval_seconds < 0:
        raise ValueError("request interval cannot be negative")

    owns_client = client is None
    http_client = client or httpx.Client(
        timeout=request_timeout_seconds,
        headers={"User-Agent": user_agent},
        follow_redirects=True,
    )
    identifiers: list[ArxivId] = []
    positions: dict[str, int] = {}
    request_count = 0
    def request_page(query: str, offset: int) -> httpx.Response:
        nonlocal request_count
        if request_count and request_interval_seconds:
            _debug(debug, f"sleeping {request_interval_seconds:g}s before next request")
            time.sleep(request_interval_seconds)
        started_at = time.perf_counter()
        _debug(
            debug,
            f"request start={offset} max_results={page_size} endpoint={api_url}",
        )
        try:
            response = http_client.get(
                api_url,
                params={
                    "search_query": query,
                    "sortBy": "submittedDate",
                    "sortOrder": "ascending",
                    "start": str(offset),
                    "max_results": str(page_size),
                },
            )
            request_count += 1
        except httpx.TimeoutException as exc:
            _debug(
                debug,
                f"request timed out after {time.perf_counter() - started_at:.3f}s: "
                f"{type(exc).__name__}: {exc}",
            )
            raise MetadataError(
                "arXiv date-range query timed out", code="DOWNLOAD_TIMEOUT"
            ) from exc
        except httpx.HTTPError as exc:
            _debug(
                debug,
                f"request failed after {time.perf_counter() - started_at:.3f}s: "
                f"{type(exc).__name__}: {exc}",
            )
            raise MetadataError(f"arXiv date-range query failed: {exc}") from exc

        _debug(
            debug,
            f"response status={response.status_code} "
            f"elapsed={time.perf_counter() - started_at:.3f}s url={response.request.url}",
        )
        if response.status_code == 429:
            _debug_error_response(debug, response)
            raise MetadataError(
                "arXiv metadata API rate limited the request", code="HTTP_429"
            )
        if response.status_code >= 500:
            _debug_error_response(debug, response)
            raise MetadataError(
                f"arXiv metadata API returned {response.status_code}", code="HTTP_5XX"
            )
        if response.status_code != 200:
            _debug_error_response(debug, response)
            raise MetadataError(
                f"arXiv metadata API returned {response.status_code}",
                code=f"HTTP_{response.status_code}",
            )
        return response

    def fetch_window(window_start: date, window_end: date) -> None:
        query = (
            f"submittedDate:[{window_start:%Y%m%d}0000 TO "
            f"{window_end:%Y%m%d}2359]"
        )
        _debug(debug, f"window={window_start}..{window_end} query={query}")
        window_identifiers: list[ArxivId] = []
        window_positions: dict[str, int] = {}
        offset = 0
        while True:
            response = request_page(query, offset)
            try:
                root = ET.fromstring(response.content)
                total_results = _total_results(root)
                entries = root.findall(f"{ATOM}entry")
                page_ids = [
                    normalize_arxiv_id(_required_entry_id(entry)) for entry in entries
                ]
            except (ET.ParseError, InvalidArxivId, ValueError) as exc:
                _debug(
                    debug,
                    f"response parsing failed: {type(exc).__name__}: {exc}; "
                    f"body={_response_body_preview(response)}",
                )
                raise MetadataError("arXiv date-range response was not valid XML") from exc

            if total_results >= MAX_PAGINATION_OFFSET:
                if window_start == window_end:
                    raise MetadataError(
                        f"arXiv returned {total_results} results for the single-day window "
                        f"{window_start}; it cannot be paged safely beyond "
                        f"start={MAX_PAGINATION_OFFSET}",
                        code="RESULT_WINDOW_TOO_LARGE",
                    )
                left_end = window_start + (window_end - window_start) // 2
                right_start = left_end + timedelta(days=1)
                _debug(
                    debug,
                    f"splitting window={window_start}..{window_end} "
                    f"total_results={total_results} at pagination limit "
                    f"start={MAX_PAGINATION_OFFSET} into "
                    f"{window_start}..{left_end} and {right_start}..{window_end}",
                )
                fetch_window(window_start, left_end)
                fetch_window(right_start, window_end)
                return

            for identifier in page_ids:
                _merge_identifier(window_identifiers, window_positions, identifier)
                _merge_identifier(identifiers, positions, identifier)

            _debug(
                debug,
                f"page entries={len(entries)} total_results={total_results} "
                f"window_unique={len(window_identifiers)} "
                f"collected_unique={len(identifiers)}",
            )

            offset += len(entries)
            if not entries and offset < total_results:
                raise MetadataError("arXiv date-range query returned an incomplete result set")
            if offset >= total_results:
                _debug(
                    debug,
                    f"window complete consumed={offset} total_results={total_results} "
                    f"window_unique={len(window_identifiers)} "
                    f"collected_unique={len(identifiers)}",
                )
                if window_callback is not None:
                    window_callback(window_start, window_end, tuple(window_identifiers))
                return

    try:
        for window_start, window_end in _month_windows(start_date, end_date):
            fetch_window(window_start, window_end)
    finally:
        if owns_client:
            http_client.close()
    return identifiers


def _merge_identifier(
    identifiers: list[ArxivId], positions: dict[str, int], identifier: ArxivId
) -> None:
    position = positions.get(identifier.arxiv_id)
    if position is not None:
        current = identifiers[position]
        if (identifier.version or 0) > (current.version or 0):
            identifiers[position] = identifier
        return
    positions[identifier.arxiv_id] = len(identifiers)
    identifiers.append(identifier)


def _debug(callback: DebugCallback | None, message: str) -> None:
    if callback is not None:
        callback(message)


def _debug_error_response(callback: DebugCallback | None, response: httpx.Response) -> None:
    if callback is None:
        return
    details = []
    for header in ("content-type", "retry-after", "server", "via", "x-cache"):
        value = response.headers.get(header)
        if value:
            details.append(f"{header}={value!r}")
    _debug(callback, f"response headers: {', '.join(details) if details else '(none selected)'}")
    _debug(callback, f"response body (max 2048 chars): {_response_body_preview(response)}")


def _response_body_preview(response: httpx.Response, limit: int = 2048) -> str:
    response_text = response.text
    text = response_text[:limit]
    if len(response_text) > limit:
        text += "... [truncated]"
    return repr(text)


def _month_windows(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        window_end = min(end_date, next_month - timedelta(days=1))
        yield current, window_end
        current = window_end + timedelta(days=1)


def _total_results(root: ET.Element) -> int:
    element = root.find(f"{OPENSEARCH}totalResults")
    if element is None or not element.text:
        raise ValueError("arXiv response is missing totalResults")
    total = int(element.text)
    if total < 0:
        raise ValueError("arXiv totalResults cannot be negative")
    return total


def _required_entry_id(entry: ET.Element) -> str:
    element = entry.find(f"{ATOM}id")
    if element is None or not element.text or not element.text.strip():
        raise ValueError("arXiv entry is missing id")
    return element.text.strip()
