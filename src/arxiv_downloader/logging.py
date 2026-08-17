from __future__ import annotations

import json
import logging
from typing import Any


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    payload = {"event": event, **{key: value for key, value in fields.items() if value is not None}}
    logger.info(json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True))
