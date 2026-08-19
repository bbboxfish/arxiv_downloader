from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

import httpx

from arxiv_downloader.config import DownloadConfig
from arxiv_downloader.downloader.rate_limit import AsyncRateLimiter
from arxiv_downloader.errors import MetadataError
from arxiv_downloader.ids import ArxivId, normalize_arxiv_id

ATOM = "{http://www.w3.org/2005/Atom}"


@dataclass(frozen=True, slots=True)
class MetadataRecord:
    arxiv_id: str
    version: int
    title: str
    submitted_at: datetime
    pdf_url: str
    raw: dict[str, Any]
    rate_limit_wait_ms: int = 0

    @property
    def normalized(self) -> ArxivId:
        return ArxivId(self.arxiv_id, self.version)


class ArxivMetadataClient:
    def __init__(
        self,
        config: DownloadConfig,
        limiter: AsyncRateLimiter,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._limiter = limiter
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=config.request_timeout_seconds,
            headers={"User-Agent": config.user_agent},
            follow_redirects=True,
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def fetch(self, requested: ArxivId) -> MetadataRecord:
        response, rate_limit_wait_ms = await self._query(
            {"id_list": requested.full_id, "max_results": "1"}
        )

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as exc:
            raise MetadataError(
                "arXiv metadata response was not valid XML",
                rate_limit_wait_ms=rate_limit_wait_ms,
            ) from exc
        entry = root.find(f"{ATOM}entry")
        if entry is None:
            raise MetadataError(
                f"metadata not found for {requested.full_id}",
                rate_limit_wait_ms=rate_limit_wait_ms,
            )
        try:
            return _parse_entry(entry, requested, rate_limit_wait_ms=rate_limit_wait_ms)
        except MetadataError as exc:
            exc.rate_limit_wait_ms = rate_limit_wait_ms
            raise

    async def fetch_many(self, requested: list[ArxivId]) -> list[MetadataRecord]:
        if not requested:
            return []
        response, rate_limit_wait_ms = await self._query(
            {
                "id_list": ",".join(identifier.full_id for identifier in requested),
                "max_results": str(len(requested)),
            }
        )

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as exc:
            raise MetadataError(
                "arXiv metadata response was not valid XML",
                rate_limit_wait_ms=rate_limit_wait_ms,
            ) from exc
        remaining = list(enumerate(requested))
        records_by_index: dict[int, MetadataRecord] = {}
        for entry in root.findall(f"{ATOM}entry"):
            entry_id = normalize_arxiv_id(_required_text(entry, "id"))
            matched = next(
                (
                    (position, original)
                    for position, original in remaining
                    if original.arxiv_id == entry_id.arxiv_id
                    and original.version == entry_id.version
                ),
                None,
            )
            if matched is None:
                matched = next(
                    (
                        (position, original)
                        for position, original in remaining
                        if original.arxiv_id == entry_id.arxiv_id
                        and original.version is None
                    ),
                    None,
                )
            if matched is None:
                raise MetadataError(
                    f"arXiv returned unexpected ID {entry_id.full_id}",
                    rate_limit_wait_ms=rate_limit_wait_ms,
                )
            position, original = matched
            records_by_index[position] = _parse_entry(entry, original)
            remaining.remove(matched)
        if remaining:
            missing = [
                identifier.full_id
                for _, identifier in remaining
            ]
            raise MetadataError(
                f"metadata not found for: {', '.join(missing)}",
                rate_limit_wait_ms=rate_limit_wait_ms,
            )
        records = [records_by_index[position] for position in range(len(requested))]
        if records:
            records[0] = replace(records[0], rate_limit_wait_ms=rate_limit_wait_ms)
        return records

    async def _query(self, params: dict[str, str]) -> tuple[httpx.Response, int]:
        rate_limit_wait_ms = await self._limiter.wait()
        try:
            response = await self._client.get(
                "https://export.arxiv.org/api/query",
                params=params,
            )
        except httpx.TimeoutException as exc:
            raise MetadataError(
                "arXiv metadata request timed out",
                code="DOWNLOAD_TIMEOUT",
                rate_limit_wait_ms=rate_limit_wait_ms,
            ) from exc
        except httpx.HTTPError as exc:
            detail = str(exc) or type(exc).__name__
            raise MetadataError(
                f"arXiv metadata request failed ({detail})",
                rate_limit_wait_ms=rate_limit_wait_ms,
            ) from exc

        if response.status_code == 429:
            raise MetadataError(
                "arXiv metadata API rate limited the request",
                code="HTTP_429",
                rate_limit_wait_ms=rate_limit_wait_ms,
            )
        if response.status_code >= 500:
            raise MetadataError(
                f"arXiv metadata API returned {response.status_code}",
                code="HTTP_5XX",
                rate_limit_wait_ms=rate_limit_wait_ms,
            )
        if response.status_code != 200:
            raise MetadataError(
                f"arXiv metadata API returned {response.status_code}",
                code=f"HTTP_{response.status_code}",
                rate_limit_wait_ms=rate_limit_wait_ms,
            )
        return response, rate_limit_wait_ms


def _required_text(entry: ET.Element, tag: str) -> str:
    element = entry.find(f"{ATOM}{tag}")
    if element is None or not element.text or not element.text.strip():
        raise MetadataError(f"arXiv metadata is missing {tag}")
    return element.text.strip()


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MetadataError(f"invalid arXiv metadata timestamp: {value}") from exc


def _parse_entry(
    entry: ET.Element, requested: ArxivId, *, rate_limit_wait_ms: int = 0
) -> MetadataRecord:
    entry_id = normalize_arxiv_id(_required_text(entry, "id"))
    if entry_id.arxiv_id != requested.arxiv_id:
        raise MetadataError(
            f"arXiv returned {entry_id.arxiv_id} for requested ID {requested.arxiv_id}"
        )
    version = entry_id.version or requested.version or 1
    if requested.version is not None and version != requested.version:
        raise MetadataError(f"arXiv returned v{version} for requested {requested.full_id}")

    published_text = _required_text(entry, "published")
    updated_text = _required_text(entry, "updated")
    submitted_at = _parse_datetime(published_text)
    title = " ".join(_required_text(entry, "title").split())
    authors = [
        " ".join(author.text.split())
        for author in entry.findall(f"{ATOM}author/{ATOM}name")
        if author.text
    ]
    pdf_url = ""
    for link in entry.findall(f"{ATOM}link"):
        if link.attrib.get("type") == "application/pdf" or link.attrib.get("title") == "pdf":
            pdf_url = link.attrib.get("href", "")
            break
    if not pdf_url:
        pdf_url = f"https://arxiv.org/pdf/{entry_id.arxiv_id}v{version}.pdf"
    elif pdf_url.startswith("http://arxiv.org/"):
        pdf_url = pdf_url.replace("http://arxiv.org/", "https://arxiv.org/", 1)

    raw = {
        "id": entry_id.full_id,
        "title": title,
        "published": published_text,
        "updated": updated_text,
        "authors": authors,
        "summary": " ".join(_required_text(entry, "summary").split()),
        "pdf_url": pdf_url,
    }
    return MetadataRecord(
        arxiv_id=entry_id.arxiv_id,
        version=version,
        title=title,
        submitted_at=submitted_at,
        pdf_url=pdf_url,
        raw=raw,
        rate_limit_wait_ms=rate_limit_wait_ms,
    )
