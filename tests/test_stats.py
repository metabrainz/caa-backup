"""Tests for CAADownloader stats and rate calculation."""

import os
import time

from caa_downloader import CAADownloader
from store import CoverStatus

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


def _make_downloader(db_setup, tmp_path):
    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)
    ds.bulk_add(
        [
            {"caa_id": 1, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
            {"caa_id": 2, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )
    ds.update(caa_id=1, release_mbid=MBID, new_status=CoverStatus.DOWNLOADED)
    return CAADownloader(db_path=db_path, images_dir=images_dir, download_threads=1)


def test_get_download_rate_empty(db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    assert dl.get_download_rate() == 0.0


def test_get_download_rate_single(db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    dl.download_times.append(time.time())
    assert dl.get_download_rate() == 0.0


def test_get_download_rate_multiple(db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    now = time.time()
    dl.download_times.append(now - 10)
    dl.download_times.append(now - 5)
    dl.download_times.append(now)
    rate = dl.get_download_rate()
    assert abs(rate - 0.2) < 0.01


def test_stats_returns_expected_keys(db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    stats = dl.stats()
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
        "metadata_fetched",
        "integrity_checked",
        "integrity_failures",
    }
    assert set(stats.keys()) == expected_keys


def test_stats_values_reflect_state(db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    dl.total = 100
    dl.downloaded = 75
    dl.errors = 5
    stats = dl.stats()
    assert stats["total_to_download"] == 100
    assert stats["downloaded"] == 75
    assert stats["download_errors"] == 5


def test_estimate_seconds_before_completed(db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    dl.growth_tracker.stop()
    dl.total = 1000
    dl.downloaded = 500
    # Simulate growth tracker samples: 500 downloads over 500 seconds
    now = time.time()
    dl.growth_tracker.download_tracker.samples.clear()
    dl.growth_tracker.download_tracker.record(0, now - 500)
    dl.growth_tracker.download_tracker.record(500, now)
    stats = dl.stats()
    assert stats["seconds_before_completed"] is not None
    assert 400 < stats["seconds_before_completed"] < 600
