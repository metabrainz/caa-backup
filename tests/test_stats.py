"""Tests for CAADownloader stats and rate calculation."""

import os
import time

import pytest

from caa_downloader import CAADownloader
from store import CAABackup, CAABackupDataStore, CoverStatus, ImportTimestamp, db

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


@pytest.fixture
def downloader(tmp_path):
    if not db.is_closed():
        db.close()

    db_path = str(tmp_path / "test.db")
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds = CAABackupDataStore(db_path=db_path)
    if db.is_closed():
        db.connect()
    db.create_tables([CAABackup, ImportTimestamp])

    ds.bulk_add(
        [
            {"caa_id": 1, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
            {"caa_id": 2, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )
    ds.update(caa_id=1, release_mbid=MBID, new_status=CoverStatus.DOWNLOADED)

    dl = CAADownloader(db_path=db_path, images_dir=images_dir, download_threads=1)

    yield dl

    if not db.is_closed():
        db.close()


def test_get_download_rate_empty(downloader):
    """Rate is 0 with no download times recorded."""
    assert downloader.get_download_rate() == 0.0


def test_get_download_rate_single(downloader):
    """Rate is 0 with only one download time."""
    downloader.download_times.append(time.time())
    assert downloader.get_download_rate() == 0.0


def test_get_download_rate_multiple(downloader):
    """Rate is calculated correctly with multiple times."""
    now = time.time()
    downloader.download_times.append(now - 10)
    downloader.download_times.append(now - 5)
    downloader.download_times.append(now)

    rate = downloader.get_download_rate()
    # 2 downloads in 10 seconds = 0.2/sec
    assert abs(rate - 0.2) < 0.01


def test_stats_returns_expected_keys(downloader):
    """stats() returns all expected keys."""
    stats = downloader.stats()
    expected_keys = {
        "total_to_download",
        "downloaded",
        "download_errors",
        "download_rate",
        "disk_total_bytes",
        "disk_free_bytes",
        "disk_used_percent",
        "seconds_before_full",
        "seconds_before_completed",
    }
    assert set(stats.keys()) == expected_keys


def test_stats_values_reflect_state(downloader):
    """stats() reflects the downloader's current state."""
    downloader.total = 100
    downloader.downloaded = 75
    downloader.errors = 5

    stats = downloader.stats()
    assert stats["total_to_download"] == 100
    assert stats["downloaded"] == 75
    assert stats["download_errors"] == 5


def test_estimate_seconds_before_completed(downloader):
    """Estimate completion time based on rate."""
    downloader.total = 1000
    downloader.downloaded = 500
    # Simulate a rate of 10/sec
    now = time.time()
    for i in range(10):
        downloader.download_times.append(now - 9 + i)

    stats = downloader.stats()
    # 500 remaining at ~1/sec = ~500 seconds
    assert stats["seconds_before_completed"] is not None
    assert 400 < stats["seconds_before_completed"] < 600
