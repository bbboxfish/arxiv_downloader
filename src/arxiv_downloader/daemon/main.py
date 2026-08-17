from __future__ import annotations

import uvicorn

from arxiv_downloader.config import load_settings
from arxiv_downloader.daemon.app import create_app


def main() -> None:
    settings = load_settings()
    uvicorn.run(
        create_app(settings),
        host=settings.server.host,
        port=settings.server.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
