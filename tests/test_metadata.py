import httpx
import pytest

from arxiv_downloader.config import DownloadConfig
from arxiv_downloader.downloader.rate_limit import AsyncRateLimiter
from arxiv_downloader.ids import ArxivId
from arxiv_downloader.metadata.client import ArxivMetadataClient

ATOM_RESPONSE = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2401.01234v2</id>
    <updated>2024-01-15T12:00:00Z</updated>
    <published>2024-01-01T12:00:00Z</published>
    <title> A useful\n paper </title>
    <summary> Test summary. </summary>
    <author><name>Alice Example</name></author>
    <link title="pdf" href="http://arxiv.org/pdf/2401.01234v2" type="application/pdf"/>
  </entry>
</feed>
"""


@pytest.mark.asyncio
async def test_metadata_parsing_uses_first_version_submission_date():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=ATOM_RESPONSE))
    http_client = httpx.AsyncClient(transport=transport)
    client = ArxivMetadataClient(
        DownloadConfig(min_request_interval_seconds=0), AsyncRateLimiter(0), http_client
    )
    record = await client.fetch(ArxivId("2401.01234", None))
    assert record.version == 2
    assert record.title == "A useful paper"
    assert record.submitted_at.day == 1
    assert record.pdf_url.startswith("https://")
    assert record.raw["authors"] == ["Alice Example"]
    assert record.raw["updated"] == "2024-01-15T12:00:00Z"


@pytest.mark.asyncio
async def test_metadata_batch_fetch_preserves_multiple_versions():
    entry = ATOM_RESPONSE.split(b"<entry>", 1)[1].split(b"</entry>", 1)[0]
    v1_entry = entry.replace(b"2401.01234v2", b"2401.01234v1")
    response = ATOM_RESPONSE.replace(
        b"<entry>" + entry + b"</entry>",
        b"<entry>" + v1_entry + b"</entry><entry>" + entry + b"</entry>",
    )
    requested_url = None

    def handler(request):
        nonlocal requested_url
        requested_url = request.url
        return httpx.Response(200, content=response)

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ArxivMetadataClient(
        DownloadConfig(min_request_interval_seconds=0), AsyncRateLimiter(0), http_client
    )

    records = await client.fetch_many(
        [ArxivId("2401.01234", 1), ArxivId("2401.01234", 2)]
    )

    assert [record.version for record in records] == [1, 2]
    assert all(record.submitted_at.day == 1 for record in records)
    assert requested_url is not None
    assert requested_url.params["id_list"] == "2401.01234v1,2401.01234v2"
