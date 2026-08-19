import pytest

from arxiv_downloader.downloader.rate_limit import AsyncRateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_reports_wait_time_for_contended_request():
    limiter = AsyncRateLimiter(0.05)
    assert await limiter.wait() >= 0
    assert await limiter.wait() >= 25
