"""Tests for CAADownloader._download_and_save_record."""

import os
from unittest.mock import MagicMock, patch

import pytest
import requests

from caa_downloader import CAADownloader
from store import CAABackup, CAABackupDataStore, CoverStatus, ImportTimestamp, db

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


@pytest.fixture
def downloader(tmp_path):
    """Create a downloader with a real SQLite DB and temp images dir."""
    if not db.is_closed():
        db.close()

    db_path = str(tmp_path / "test.db")
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds = CAABackupDataStore(db_path=db_path)
    if db.is_closed():
        db.connect()
    db.create_tables([CAABackup, ImportTimestamp])

    # Insert a test record
    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )

    dl = CAADownloader(db_path=db_path, images_dir=images_dir, download_threads=1)

    yield dl

    if not db.is_closed():
        db.close()


def _make_record():
    """Create a mock record object."""
    record = MagicMock()
    record.release_mbid = MBID
    record.caa_id = 1000
    record.mime_type = "image/jpeg"
    return record


@patch("caa_downloader.requests.get")
def test_successful_download(mock_get, downloader):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"fake image data"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    record = _make_record()
    result = downloader._download_and_save_record(record)

    assert result == (MBID, 1000)

    # File should exist at the correct path
    filepath = os.path.join(downloader.images_dir, "a", "b", f"{MBID}-1000.jpg")
    assert os.path.exists(filepath)
    assert open(filepath, "rb").read() == b"fake image data"

    # No .tmp file should remain
    assert not os.path.exists(filepath + ".tmp")

    # DB should be updated to DOWNLOADED
    with downloader.datastore:
        record_db = downloader.datastore.get(1000)
        assert record_db.status == CoverStatus.DOWNLOADED.value


@patch("caa_downloader.requests.get")
def test_http_404_permanent_error(mock_get, downloader):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    record = _make_record()
    result = downloader._download_and_save_record(record)

    assert result == (None, None)

    # No file should be written
    filepath = os.path.join(downloader.images_dir, "a", "b", f"{MBID}-1000.jpg")
    assert not os.path.exists(filepath)

    # DB should be PERMANENT_ERROR
    with downloader.datastore:
        record_db = downloader.datastore.get(1000)
        assert record_db.status == CoverStatus.PERMANENT_ERROR.value


@patch("caa_downloader.requests.get")
def test_http_500_temp_error(mock_get, downloader):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    record = _make_record()
    result = downloader._download_and_save_record(record)

    assert result == (None, None)

    with downloader.datastore:
        record_db = downloader.datastore.get(1000)
        assert record_db.status == CoverStatus.TEMP_ERROR.value


@patch("caa_downloader.requests.get")
def test_timeout_temp_error(mock_get, downloader):
    mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")

    record = _make_record()
    result = downloader._download_and_save_record(record)

    assert result == (None, None)

    with downloader.datastore:
        record_db = downloader.datastore.get(1000)
        assert record_db.status == CoverStatus.TEMP_ERROR.value


def test_missing_attributes(downloader):
    record = MagicMock(spec=[])  # No attributes
    del record.release_mbid
    result = downloader._download_and_save_record(record)
    assert result == (None, None)
