"""Tests for CAADownloader.run_downloader orchestration."""

import os
from unittest.mock import MagicMock, patch

import pytest

from caa_downloader import CAADownloader
from store import CAABackup, CAABackupDataStore, CoverStatus, ImportTimestamp, db

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


@pytest.fixture
def setup(tmp_path):
    """Create a downloader with a real SQLite DB."""
    if not db.is_closed():
        db.close()

    db_path = str(tmp_path / "test.db")
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds = CAABackupDataStore(db_path=db_path)
    if db.is_closed():
        db.connect()
    db.create_tables([CAABackup, ImportTimestamp])

    dl = CAADownloader(db_path=db_path, images_dir=images_dir, download_threads=2)

    yield dl, ds

    if not db.is_closed():
        db.close()


def test_run_downloader_nothing_pending(setup):
    """run_downloader returns immediately when nothing to download."""
    dl, ds = setup

    # All records are already downloaded
    ds.bulk_add(
        [
            {"caa_id": 1, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )
    ds.update(caa_id=1, release_mbid=MBID, new_status=CoverStatus.DOWNLOADED)

    dl.run_downloader()

    assert dl.downloaded == 1
    assert dl.total == 1


def test_run_downloader_shutdown_flag(setup):
    """run_downloader stops when _shutdown_requested is set."""
    dl, ds = setup

    ds.bulk_add(
        [
            {
                "caa_id": i,
                "release_mbid": f"{i:08d}-0000-0000-0000-000000000000",
                "mime_type": "image/jpeg",
                "status": CoverStatus.NOT_DOWNLOADED,
            }
            for i in range(10)
        ]
    )

    # Set shutdown before running
    dl._shutdown_requested = True
    dl.run_downloader()

    # Should not have downloaded anything
    counts = ds.get_status_counts()
    assert counts["NOT_DOWNLOADED"] == 10


@patch("caa_downloader.requests.get")
def test_run_downloader_downloads_pending(mock_get, setup):
    """run_downloader processes pending records."""
    dl, ds = setup

    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"image data"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    dl.run_downloader()

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 1
    assert counts.get("NOT_DOWNLOADED", 0) == 0
