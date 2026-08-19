from __future__ import annotations

import argparse
from collections.abc import Sequence

import uvicorn

from arxiv_downloader.config import load_settings
from arxiv_downloader.daemon.app import create_app


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="arxivd",
        description="Run the persistent arXiv download daemon.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="enable verbose daemon, scheduler, HTTP, and database diagnostics",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    settings = load_settings()
    uvicorn.run(
        create_app(settings, debug=args.debug),
        host=settings.server.host,
        port=settings.server.port,
        log_config=None,
        log_level="debug" if args.debug else "info",
        access_log=False,
    )


if __name__ == "__main__":
    main()
