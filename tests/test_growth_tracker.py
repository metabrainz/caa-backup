"""Tests for growth_tracker module."""

import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from growth_tracker import DiskGrowthTracker, GrowthTracker


@pytest.fixture
def now():
    return 1000000.0


@pytest.fixture
def fake_downloader():
    return SimpleNamespace(downloaded=50, total=100, permanent_errors=0)


@pytest.fixture
def tracker(tmp_path, fake_downloader):
    return DiskGrowthTracker(path=str(tmp_path), downloader=fake_downloader, sample_interval=1)


# --- GrowthTracker tests ---


@pytest.mark.parametrize(
    "samples,kwargs,expected",
    [
        ([], {}, None),  # no samples
        ([(0, 100)], {}, None),  # single sample
        ([(-10, 0), (0, 100)], {"window_seconds": 60}, 10.0),  # normal rate
        ([(-10, 100), (0, 100)], {}, 0.0),  # zero growth
    ],
)
def test_growth_tracker_rate(samples, kwargs, expected):
    gt = GrowthTracker(**kwargs)
    base = 1000000.0
    for offset, value in samples:
        gt.record(value, base + offset)
    if expected is None:
        assert gt.rate() is None
    else:
        assert abs(gt.rate() - expected) < 0.01


def test_growth_tracker_evicts_old_samples():
    gt = GrowthTracker(window_seconds=10, max_samples=100)
    base = 1000000.0
    gt.record(0, base - 20)  # outside window
    gt.record(50, base - 5)
    gt.record(100, base)
    assert abs(gt.rate() - 10.0) < 0.01


# --- DiskGrowthTracker tests ---


def test_seconds_before_full(tracker, now):
    tracker.disk_tracker.record(1000, now - 10)
    tracker.disk_tracker.record(11000, now)

    with patch("growth_tracker.shutil.disk_usage") as mock_du:
        mock_du.return_value = (100000, 50000, 50000)
        assert abs(tracker.seconds_before_full() - 50.0) < 0.01


@pytest.mark.parametrize(
    "downloaded,total,permanent_errors,expected",
    [
        (500, 1000, 0, 100.0),  # 500 remaining at 5/sec
        (500, 1000, 400, 20.0),  # excludes permanent errors
        (100, 100, 0, 0.0),  # already complete
    ],
)
def test_seconds_before_completed(tracker, fake_downloader, now, downloaded, total, permanent_errors, expected):
    fake_downloader.downloaded = downloaded
    fake_downloader.total = total
    fake_downloader.permanent_errors = permanent_errors

    tracker.download_tracker.record(0, now - 100)
    tracker.download_tracker.record(downloaded, now)
    assert abs(tracker.seconds_before_completed() - expected) < 0.01


def test_no_growth_returns_none(tracker):
    assert tracker.seconds_before_full() is None
    assert tracker.seconds_before_completed() is None


def test_reset(tracker, now):
    tracker.download_tracker.record(0, now - 10)
    tracker.disk_tracker.record(0, now - 10)

    tracker.reset()

    assert len(tracker.download_tracker.samples) == 1
    assert len(tracker.disk_tracker.samples) == 1
    assert tracker.download_tracker.rate() is None


def test_background_sampling(tmp_path, fake_downloader):
    tracker = DiskGrowthTracker(path=str(tmp_path), downloader=fake_downloader, sample_interval=0.05)
    tracker.start()
    try:
        time.sleep(0.2)
        assert len(tracker.download_tracker.samples) >= 2
    finally:
        tracker.stop()


def test_negative_disk_rate(tracker, now):
    tracker.disk_tracker.record(20000, now - 10)
    tracker.disk_tracker.record(10000, now)
    assert tracker.seconds_before_full() is None


def test_oserror_on_sample(fake_downloader):
    tracker = DiskGrowthTracker(path="/nonexistent/path", downloader=fake_downloader, sample_interval=1)
    tracker._record_sample()
    assert len(tracker.disk_tracker.samples) == 0
