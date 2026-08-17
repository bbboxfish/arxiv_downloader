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
async def test_metadata_parsing_resolves_version_and_submission_date():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=ATOM_RESPONSE))
    http_client = httpx.AsyncClient(transport=transport)
    client = ArxivMetadataClient(
        DownloadConfig(min_request_interval_seconds=0), AsyncRateLimiter(0), http_client
    )
    record = await client.fetch(ArxivId("2401.01234", None))
    assert record.version == 2
    assert record.title == "A useful paper"
    assert record.submitted_at.day == 15
    assert record.pdf_url.startswith("https://")
    assert record.raw["authors"] == ["Alice Example"]
