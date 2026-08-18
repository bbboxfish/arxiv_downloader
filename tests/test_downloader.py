from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from arxiv_downloader.config import DownloadConfig
from arxiv_downloader.downloader.client import PDFDownloader
from arxiv_downloader.downloader.rate_limit import AsyncRateLimiter
from arxiv_downloader.errors import DownloadError


class FailingStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], error: Exception | None = None) -> None:
        self._chunks = chunks
        self._error = error

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk
        if self._error is not None:
            raise self._error


def downloader(tmp_path: Path, response: httpx.Response, **overrides) -> PDFDownloader:
    transport = httpx.MockTransport(lambda request: response)
    client = httpx.AsyncClient(transport=transport)
    config = DownloadConfig(min_request_interval_seconds=0, **overrides)
    return PDFDownloader(config, tmp_path, AsyncRateLimiter(0), client)


@pytest.mark.asyncio
async def test_streams_pdf_and_computes_digest(tmp_path):
    payload = b"%PDF-1.7\nsmall-pdf"
    service = downloader(tmp_path, httpx.Response(200, content=payload))
    result = await service.download(uuid4(), uuid4(), "https://arxiv.org/pdf/test.pdf")
    assert result.path.read_bytes() == payload
    assert result.size_bytes == len(payload)
    assert len(result.sha256) == 64


@pytest.mark.asyncio
async def test_rejects_html_disguised_as_pdf_and_removes_part(tmp_path):
    service = downloader(tmp_path, httpx.Response(200, content=b"<html>no</html>"))
    with pytest.raises(DownloadError) as error:
        await service.download(uuid4(), uuid4(), "https://arxiv.org/pdf/test.pdf")
    assert error.value.code == "NOT_A_PDF"
    assert not list(tmp_path.rglob("*.part"))


@pytest.mark.asyncio
async def test_maps_404_to_stable_error_code(tmp_path):
    service = downloader(tmp_path, httpx.Response(404))
    with pytest.raises(DownloadError) as error:
        await service.download(uuid4(), uuid4(), "https://arxiv.org/pdf/missing.pdf")
    assert error.value.code == "HTTP_404"


@pytest.mark.asyncio
async def test_enforces_announced_file_size(tmp_path):
    service = downloader(
        tmp_path,
        httpx.Response(200, headers={"content-length": str(2 * 1024 * 1024)}),
        max_file_size_mb=1,
    )
    with pytest.raises(DownloadError) as error:
        await service.download(uuid4(), uuid4(), "https://arxiv.org/pdf/large.pdf")
    assert error.value.code == "FILE_TOO_LARGE"


@pytest.mark.asyncio
async def test_resumes_interrupted_download_with_http_range(tmp_path):
    payload = b"%PDF-1.7\nresumable-pdf"
    requests: list[httpx.Request] = []
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        requests.append(request)
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                headers={"content-length": str(len(payload)), "etag": '"v1"'},
                stream=FailingStream([payload[:9]], httpx.ReadTimeout("interrupted")),
                request=request,
            )
        assert request.headers["range"] == "bytes=9-"
        assert request.headers["if-range"] == '"v1"'
        return httpx.Response(
            206,
            headers={
                "content-length": str(len(payload) - 9),
                "content-range": f"bytes 9-{len(payload) - 1}/{len(payload)}",
                "etag": '"v1"',
            },
            content=payload[9:],
            request=request,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = DownloadConfig(
        min_request_interval_seconds=0,
        checkpoint_interval_bytes=1,
        checkpoint_interval_seconds=60,
    )
    service = PDFDownloader(config, tmp_path, AsyncRateLimiter(0), client)
    task_id = uuid4()
    paper_id = uuid4()

    with pytest.raises(DownloadError, match="timed out"):
        await service.download(task_id, paper_id, "https://arxiv.org/pdf/resume.pdf")
    part_path = next(tmp_path.rglob("*.part"))
    assert part_path.read_bytes() == payload[:9]
    assert next(tmp_path.rglob("*.part.meta.json")).is_file()

    result = await service.download(task_id, paper_id, "https://arxiv.org/pdf/resume.pdf")

    assert result.path.read_bytes() == payload
    assert result.size_bytes == len(payload)
    assert not list(tmp_path.rglob("*.part.meta.json"))
    await service.close()


@pytest.mark.asyncio
async def test_range_ignorance_restarts_from_full_response(tmp_path):
    payload = b"%PDF-1.7\nfull-response"
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                headers={"content-length": str(len(payload))},
                stream=FailingStream([payload[:5]], httpx.ReadTimeout("interrupted")),
                request=request,
            )
        assert request.headers["range"] == "bytes=5-"
        return httpx.Response(200, content=payload, request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = DownloadConfig(
        min_request_interval_seconds=0,
        checkpoint_interval_bytes=1,
        checkpoint_interval_seconds=60,
    )
    service = PDFDownloader(config, tmp_path, AsyncRateLimiter(0), client)
    task_id = uuid4()
    paper_id = uuid4()

    with pytest.raises(DownloadError):
        await service.download(task_id, paper_id, "https://arxiv.org/pdf/restart.pdf")
    result = await service.download(task_id, paper_id, "https://arxiv.org/pdf/restart.pdf")

    assert result.path.read_bytes() == payload
    await service.close()
