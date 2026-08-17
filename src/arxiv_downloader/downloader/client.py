from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import httpx

from arxiv_downloader.config import DownloadConfig
from arxiv_downloader.downloader.rate_limit import AsyncRateLimiter
from arxiv_downloader.errors import DownloadError


@dataclass(frozen=True, slots=True)
class DownloadResult:
    path: Path
    size_bytes: int
    sha256: str


class PDFDownloader:
    def __init__(
        self,
        config: DownloadConfig,
        staging_root: Path,
        limiter: AsyncRateLimiter,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._staging_root = staging_root
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

    async def download(self, task_id: UUID, paper_id: UUID, pdf_url: str) -> DownloadResult:
        task_directory = self._staging_root / str(task_id)
        part_path = task_directory / f"{paper_id}.part"
        task_directory.mkdir(parents=True, exist_ok=True)
        part_path.unlink(missing_ok=True)
        await self._limiter.wait()

        size = 0
        digest = hashlib.sha256()
        prefix = bytearray()
        try:
            async with self._client.stream("GET", pdf_url) as response:
                _raise_for_status(response.status_code)
                content_length = response.headers.get("content-length")
                if content_length:
                    try:
                        announced_size = int(content_length)
                    except ValueError:
                        announced_size = 0
                    if announced_size > self._config.max_file_size_bytes:
                        raise DownloadError(
                            "PDF exceeds configured size limit", code="FILE_TOO_LARGE"
                        )

                with part_path.open("wb") as output:
                    async for chunk in response.aiter_bytes():
                        if not chunk:
                            continue
                        size += len(chunk)
                        if size > self._config.max_file_size_bytes:
                            raise DownloadError(
                                "PDF exceeds configured size limit", code="FILE_TOO_LARGE"
                            )
                        if len(prefix) < 5:
                            prefix.extend(chunk[: 5 - len(prefix)])
                        digest.update(chunk)
                        output.write(chunk)
                    output.flush()

            if bytes(prefix) != b"%PDF-":
                raise DownloadError("response does not start with %PDF-", code="NOT_A_PDF")
            return DownloadResult(path=part_path, size_bytes=size, sha256=digest.hexdigest())
        except httpx.TimeoutException as exc:
            part_path.unlink(missing_ok=True)
            raise DownloadError("PDF download timed out", code="DOWNLOAD_TIMEOUT") from exc
        except httpx.HTTPError as exc:
            part_path.unlink(missing_ok=True)
            raise DownloadError(f"PDF request failed: {exc}", code="HTTP_ERROR") from exc
        except DownloadError:
            part_path.unlink(missing_ok=True)
            raise
        except OSError as exc:
            part_path.unlink(missing_ok=True)
            raise DownloadError(
                f"could not write staging file: {exc}", code="STORAGE_WRITE_FAILED"
            ) from exc


def _raise_for_status(status_code: int) -> None:
    if status_code == 404:
        raise DownloadError("PDF was not found", code="HTTP_404")
    if status_code == 429:
        raise DownloadError("arXiv rate limited the PDF request", code="HTTP_429")
    if status_code >= 500:
        raise DownloadError(f"arXiv returned HTTP {status_code}", code="HTTP_5XX")
    if status_code < 200 or status_code >= 300:
        raise DownloadError(f"unexpected HTTP status {status_code}", code=f"HTTP_{status_code}")
