from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
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


@dataclass(frozen=True, slots=True)
class Checkpoint:
    size_bytes: int
    etag: str | None
    last_modified: str | None
    expected_size: int | None


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
        checkpoint_path = _checkpoint_path(part_path)
        task_directory.mkdir(parents=True, exist_ok=True)

        checkpoint = self._load_resume_checkpoint(part_path, checkpoint_path, pdf_url)
        offset = checkpoint.size_bytes if checkpoint is not None else 0
        if checkpoint is not None and checkpoint.expected_size == offset:
            size, prefix, digest = await asyncio.to_thread(_hash_existing, part_path)
            if size == offset and bytes(prefix) == b"%PDF-":
                checkpoint_path.unlink(missing_ok=True)
                return DownloadResult(path=part_path, size_bytes=size, sha256=digest.hexdigest())
        request_headers: dict[str, str] = {}
        if checkpoint is not None and offset:
            request_headers["Range"] = f"bytes={offset}-"
            validator = checkpoint.etag or checkpoint.last_modified
            if validator:
                request_headers["If-Range"] = validator

        await self._limiter.wait()
        size = offset
        expected_size = checkpoint.expected_size if checkpoint else None
        digest = hashlib.sha256()
        prefix = bytearray()
        preserve_on_error = False
        try:
            if offset:
                size, prefix, digest = await asyncio.to_thread(_hash_existing, part_path)

            async with self._client.stream("GET", pdf_url, headers=request_headers) as response:
                status_code = response.status_code
                if offset and status_code == 206:
                    range_start, range_total = _parse_content_range(
                        response.headers.get("content-range")
                    )
                    if range_start != offset:
                        _remove_checkpoint(part_path, checkpoint_path)
                        raise DownloadError(
                            "server returned an invalid content range", code="HTTP_RANGE_INVALID"
                        )
                    expected_size = range_total or expected_size
                    mode = "ab"
                    preserve_on_error = True
                elif status_code == 200:
                    if offset:
                        _remove_checkpoint(part_path, checkpoint_path)
                    mode = "wb"
                    size = 0
                    expected_size = _content_length(response)
                    digest = hashlib.sha256()
                    prefix = bytearray()
                    preserve_on_error = True
                else:
                    _raise_for_status(status_code)
                    raise DownloadError(
                        f"unexpected HTTP status {status_code}", code="HTTP_RANGE_INVALID"
                    )

                if expected_size is not None and expected_size > self._config.max_file_size_bytes:
                    raise DownloadError("PDF exceeds configured size limit", code="FILE_TOO_LARGE")

                checkpoint = Checkpoint(
                    size_bytes=size,
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                    expected_size=expected_size,
                )
                await asyncio.to_thread(_write_checkpoint, checkpoint_path, checkpoint, pdf_url)

                last_checkpoint_size = size
                last_checkpoint_time = time.monotonic()
                with part_path.open(mode) as output:
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
                        now = time.monotonic()
                        if (
                            size - last_checkpoint_size >= self._config.checkpoint_interval_bytes
                            or now - last_checkpoint_time
                            >= self._config.checkpoint_interval_seconds
                        ):
                            output.flush()
                            os.fsync(output.fileno())
                            checkpoint = Checkpoint(
                                size_bytes=size,
                                etag=response.headers.get("etag"),
                                last_modified=response.headers.get("last-modified"),
                                expected_size=expected_size,
                            )
                            await asyncio.to_thread(
                                _write_checkpoint, checkpoint_path, checkpoint, pdf_url
                            )
                            last_checkpoint_size = size
                            last_checkpoint_time = now
                    output.flush()
                    os.fsync(output.fileno())

            if expected_size is not None and size != expected_size:
                raise DownloadError(
                    f"PDF ended at {size} bytes, expected {expected_size}",
                    code="DOWNLOAD_INCOMPLETE",
                )
            if bytes(prefix) != b"%PDF-":
                raise DownloadError("response does not start with %PDF-", code="NOT_A_PDF")
            checkpoint_path.unlink(missing_ok=True)
            return DownloadResult(path=part_path, size_bytes=size, sha256=digest.hexdigest())
        except asyncio.CancelledError:
            if self._config.resume_downloads and part_path.exists() and size:
                await self._checkpoint_current(
                    part_path, checkpoint_path, size, checkpoint, pdf_url
                )
            raise
        except httpx.TimeoutException as exc:
            if self._config.resume_downloads and part_path.exists() and size:
                await self._checkpoint_current(
                    part_path, checkpoint_path, size, checkpoint, pdf_url
                )
            raise DownloadError("PDF download timed out", code="DOWNLOAD_TIMEOUT") from exc
        except httpx.HTTPError as exc:
            if self._config.resume_downloads and part_path.exists() and size:
                await self._checkpoint_current(
                    part_path, checkpoint_path, size, checkpoint, pdf_url
                )
            raise DownloadError(f"PDF request failed: {exc}", code="HTTP_ERROR") from exc
        except DownloadError as exc:
            resumable = (
                self._config.resume_downloads
                and exc.code
                not in {
                    "HTTP_404",
                    "FILE_TOO_LARGE",
                    "NOT_A_PDF",
                }
                and (
                    preserve_on_error
                    or exc.code in {"HTTP_429", "HTTP_5XX", "HTTP_ERROR", "DOWNLOAD_INCOMPLETE"}
                )
            )
            if resumable and part_path.exists() and size:
                await self._checkpoint_current(
                    part_path, checkpoint_path, size, checkpoint, pdf_url
                )
            else:
                _remove_checkpoint(part_path, checkpoint_path)
            raise
        except OSError as exc:
            _remove_checkpoint(part_path, checkpoint_path)
            raise DownloadError(
                f"could not write staging file: {exc}", code="STORAGE_WRITE_FAILED"
            ) from exc

    def _load_resume_checkpoint(
        self, part_path: Path, checkpoint_path: Path, pdf_url: str
    ) -> Checkpoint | None:
        if not self._config.resume_downloads or not part_path.is_file():
            _remove_checkpoint(part_path, checkpoint_path)
            return None
        try:
            data = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            if data.get("pdf_url") != pdf_url:
                raise ValueError("URL changed")
            size = int(data["size_bytes"])
            if size < 1 or part_path.stat().st_size < size:
                raise ValueError("checkpoint is larger than the part file")
            if part_path.stat().st_size != size:
                with part_path.open("r+b") as part:
                    part.truncate(size)
            return Checkpoint(
                size_bytes=size,
                etag=data.get("etag"),
                last_modified=data.get("last_modified"),
                expected_size=data.get("expected_size"),
            )
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            _remove_checkpoint(part_path, checkpoint_path)
            return None

    async def _checkpoint_current(
        self,
        part_path: Path,
        checkpoint_path: Path,
        size: int,
        checkpoint: Checkpoint | None,
        pdf_url: str,
    ) -> None:
        current = Checkpoint(
            size_bytes=size,
            etag=checkpoint.etag if checkpoint else None,
            last_modified=checkpoint.last_modified if checkpoint else None,
            expected_size=checkpoint.expected_size if checkpoint else None,
        )
        await asyncio.to_thread(_write_checkpoint, checkpoint_path, current, pdf_url)


def _checkpoint_path(part_path: Path) -> Path:
    return part_path.with_name(f"{part_path.name}.meta.json")


def _content_length(response: httpx.Response) -> int | None:
    value = response.headers.get("content-length")
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _parse_content_range(value: str | None) -> tuple[int, int | None]:
    if not value:
        raise DownloadError("missing Content-Range for resumed download", code="HTTP_RANGE_INVALID")
    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+|\*)", value.strip())
    if match is None:
        raise DownloadError("invalid Content-Range for resumed download", code="HTTP_RANGE_INVALID")
    start_text, end_text, total_text = match.groups()
    start = int(start_text)
    end = int(end_text)
    if end < start:
        raise DownloadError("invalid Content-Range bounds", code="HTTP_RANGE_INVALID")
    if total_text != "*" and int(total_text) <= end:
        raise DownloadError("invalid Content-Range total", code="HTTP_RANGE_INVALID")
    return start, None if total_text == "*" else int(total_text)


def _hash_existing(path: Path) -> tuple[int, bytearray, hashlib._Hash]:
    size = 0
    prefix = bytearray()
    digest = hashlib.sha256()
    with path.open("rb") as existing:
        for chunk in iter(lambda: existing.read(1024 * 1024), b""):
            size += len(chunk)
            if len(prefix) < 5:
                prefix.extend(chunk[: 5 - len(prefix)])
            digest.update(chunk)
    return size, prefix, digest


def _write_checkpoint(path: Path, checkpoint: Checkpoint, pdf_url: str) -> None:
    payload = {
        "pdf_url": pdf_url,
        "size_bytes": checkpoint.size_bytes,
        "etag": checkpoint.etag,
        "last_modified": checkpoint.last_modified,
        "expected_size": checkpoint.expected_size,
    }
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    with temporary.open("rb") as metadata:
        os.fsync(metadata.fileno())
    os.replace(temporary, path)


def _remove_checkpoint(part_path: Path, checkpoint_path: Path) -> None:
    part_path.unlink(missing_ok=True)
    checkpoint_path.unlink(missing_ok=True)


def _raise_for_status(status_code: int) -> None:
    if status_code == 404:
        raise DownloadError("PDF was not found", code="HTTP_404")
    if status_code == 429:
        raise DownloadError("arXiv rate limited the PDF request", code="HTTP_429")
    if status_code >= 500:
        raise DownloadError(f"arXiv returned HTTP {status_code}", code="HTTP_5XX")
    if status_code < 200 or status_code >= 300:
        raise DownloadError(f"unexpected HTTP status {status_code}", code=f"HTTP_{status_code}")
