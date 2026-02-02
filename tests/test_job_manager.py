
import pytest

from engine import JobManager


@pytest.mark.asyncio
async def test_job_lifecycle_and_cancel():
    jm = JobManager()
    job = await jm.create("session-1")
    assert job.job_id
    assert job.status == "idle"

    # cancel returns True and marks status
    ok = await jm.cancel(job.job_id)
    assert ok
    assert job.status == "cancelling"
    assert job.cancel_event.is_set()


@pytest.mark.asyncio
async def test_job_cache_helpers():
    jm = JobManager()
    payload = {"value": 123}
    jm.cache_set("key", payload)
    assert jm.cache_get("key", ttl_sec=10) == payload

    # simulate expiry by using a negative ttl
    assert jm.cache_get("key", ttl_sec=0) is None
