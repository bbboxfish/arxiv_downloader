"""Parsing and stable filename rules for modern and legacy arXiv IDs."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from urllib.parse import unquote, urlsplit

from arxiv_downloader.errors import InvalidArxivId

_MODERN_ID = re.compile(r"^(?P<base>\d{4}\.\d{4,5})(?:v(?P<version>[1-9]\d*))?$")
_LEGACY_ID = re.compile(
    r"^(?P<base>[a-z][a-z0-9.-]*/\d{7})(?:v(?P<version>[1-9]\d*))?$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class ArxivId:
    arxiv_id: str
    version: int | None

    @property
    def full_id(self) -> str:
        if self.version is None:
            return self.arxiv_id
        return f"{self.arxiv_id}v{self.version}"

    @property
    def filename(self) -> str:
        if self.version is None:
            raise InvalidArxivId("a version is required before generating a filename")
        return f"{self.arxiv_id.replace('/', '__')}v{self.version}.pdf"

    def with_version(self, version: int) -> ArxivId:
        if version < 1:
            raise InvalidArxivId("arXiv version must be positive")
        return replace(self, version=version)


def normalize_arxiv_id(raw_value: str) -> ArxivId:
    value = raw_value.strip()
    if not value:
        raise InvalidArxivId("arXiv ID is empty")

    parsed_url = urlsplit(value)
    if parsed_url.scheme or parsed_url.netloc:
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc.lower().endswith(
            "arxiv.org"
        ):
            raise InvalidArxivId(f"unsupported arXiv URL: {raw_value}")
        value = unquote(parsed_url.path).strip("/")
        for prefix in ("abs/", "pdf/"):
            if value.startswith(prefix):
                value = value[len(prefix) :]
                break
    else:
        value = value.split("?", 1)[0].split("#", 1)[0]

    if value.lower().endswith(".pdf"):
        value = value[:-4]
    value = value.strip("/")

    match = _MODERN_ID.fullmatch(value) or _LEGACY_ID.fullmatch(value)
    if match is None:
        raise InvalidArxivId(f"invalid arXiv ID: {raw_value}")
    base = match.group("base")
    if "/" in base:
        base = base.lower()
    version_text = match.group("version")
    return ArxivId(base, int(version_text) if version_text else None)


def normalize_unique(values: list[str]) -> tuple[list[ArxivId], int, list[str]]:
    unique: list[ArxivId] = []
    seen: set[str] = set()
    duplicates = 0
    invalid: list[str] = []
    for value in values:
        try:
            normalized = normalize_arxiv_id(value)
        except InvalidArxivId:
            invalid.append(value)
            continue
        if normalized.full_id in seen:
            duplicates += 1
            continue
        seen.add(normalized.full_id)
        unique.append(normalized)
    return unique, duplicates, invalid
