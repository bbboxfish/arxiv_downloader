from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from arxiv_downloader.config import DownloadConfig
from arxiv_downloader.downloader.client import PDFDownloader
from arxiv_downloader.downloader.rate_limit import AsyncRateLimiter
from arxiv_downloader.errors import DownloadError


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
