import logging

from arxiv_downloader.logging import _SuccessfulHttpFilter, log_event


def test_successful_http_filter_suppresses_only_2xx_request_lines():
    http_ok = logging.LogRecord(
        "httpx", logging.INFO, __file__, 1, 'HTTP Request: GET / "HTTP/1.1 200 OK"', (), None
    )
    http_error = logging.LogRecord(
        "httpx", logging.INFO, __file__, 1, 'HTTP Request: GET / "HTTP/1.1 500 Internal"', (), None
    )
    http2_ok = logging.LogRecord(
        "httpx", logging.INFO, __file__, 1, 'HTTP Request: GET / "HTTP/2 204 No Content"', (), None
    )
    assert _SuccessfulHttpFilter().filter(http_ok) is False
    assert _SuccessfulHttpFilter().filter(http2_ok) is False
    assert _SuccessfulHttpFilter().filter(http_error) is True


def test_log_event_honors_debug_level(caplog):
    logger = logging.getLogger("arxivd.test")
    with caplog.at_level(logging.DEBUG, logger="arxivd.test"):
        log_event(logger, "metadata_imported", level=logging.DEBUG, arxiv_id="2403.05530v5")

    assert caplog.records[-1].levelno == logging.DEBUG
    assert '"event": "metadata_imported"' in caplog.records[-1].message
