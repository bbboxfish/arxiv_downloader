from __future__ import annotations

import json
import logging
import re
from typing import Any

_SUCCESSFUL_HTTP_REQUEST = re.compile(r'HTTP/[0-9.]+ 2\d\d\b')


class _SuccessfulHttpFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return _SUCCESSFUL_HTTP_REQUEST.search(record.getMessage()) is None


def configure_logging(*, debug: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(message)s",
        force=True,
    )
    for handler in logging.getLogger().handlers:
        handler.addFilter(_SuccessfulHttpFilter())
    logging.getLogger("httpx").setLevel(logging.DEBUG if debug else logging.INFO)
    # httpcore DEBUG output is connection-level noise and cannot be cleanly
    # filtered by final response status. httpx still reports non-2xx requests.
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def log_event(logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    payload = {"event": event, **{key: value for key, value in fields.items() if value is not None}}
    logger.log(level, json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True))
