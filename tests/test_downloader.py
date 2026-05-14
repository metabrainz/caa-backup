"""Tests for CAADownloader._download_and_save_record."""

import os
from unittest.mock import MagicMock, patch

import requests

from caa_downloader import CAADownloader
from store import CoverStatus

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


def _make_downloader(db_setup, tmp_path):
    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)
    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )
    return CAADownloader(db_path=db_path, images_dir=images_dir, download_threads=1)


def _make_record():
    record = MagicMock()
    record.release_mbid = MBID
    record.caa_id = 1000
    record.mime_type = "image/jpeg"
    return record


@patch("caa_downloader.requests.get")
def test_successful_download(mock_get, db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"fake image data"
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    result = dl._download_and_save_record(_make_record())

    assert result == (MBID, 1000)
    filepath = os.path.join(dl.images_dir, "a", "b", f"{MBID}-1000.jpg")
    assert os.path.exists(filepath)
    assert open(filepath, "rb").read() == b"fake image data"
    assert not os.path.exists(filepath + ".tmp")

    with dl.datastore:
        record_db = dl.datastore.get(1000)
        assert record_db.status == CoverStatus.DOWNLOADED.value


@patch("caa_downloader.requests.get")
def test_http_404_permanent_error(mock_get, db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    result = dl._download_and_save_record(_make_record())

    assert result == (None, None)
    with dl.datastore:
        record_db = dl.datastore.get(1000)
        assert record_db.status == CoverStatus.PERMANENT_ERROR.value


@patch("caa_downloader.requests.get")
def test_http_500_temp_error(mock_get, db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    mock_get.return_value = mock_response

    result = dl._download_and_save_record(_make_record())

    assert result == (None, None)
    with dl.datastore:
        record_db = dl.datastore.get(1000)
        assert record_db.status == CoverStatus.TEMP_ERROR.value


@patch("caa_downloader.requests.get")
def test_timeout_temp_error(mock_get, db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")

    result = dl._download_and_save_record(_make_record())

    assert result == (None, None)
    with dl.datastore:
        record_db = dl.datastore.get(1000)
        assert record_db.status == CoverStatus.TEMP_ERROR.value


def test_missing_attributes(db_setup, tmp_path):
    dl = _make_downloader(db_setup, tmp_path)
    record = MagicMock(spec=[])
    del record.release_mbid
    result = dl._download_and_save_record(record)
    assert result == (None, None)


@patch("caa_downloader.requests.get")
def test_download_verifies_against_metadata(mock_get, db_setup, tmp_path):
    """Successful download is verified against IA metadata if available."""
    import gzip
    import json

    dl = _make_downloader(db_setup, tmp_path)

    # Pre-create metadata with expected size
    from metadata_fetcher import metadata_path

    meta_file = metadata_path(dl.images_dir, MBID)
    os.makedirs(os.path.dirname(meta_file), exist_ok=True)
    metadata = {"result": [{"name": f"mbid-{MBID}-1000.jpg", "size": "15", "md5": "abc"}]}
    with gzip.open(meta_file, "wt", encoding="utf-8") as f:
        json.dump(metadata, f)

    # Download returns content matching expected size
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"x" * 15  # matches expected size
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    result = dl._download_and_save_record(_make_record())
    assert result == (MBID, 1000)

    with dl.datastore:
        record_db = dl.datastore.get(1000)
        assert record_db.status == CoverStatus.DOWNLOADED.value


@patch("caa_downloader.requests.get")
def test_download_fails_verification_size_mismatch(mock_get, db_setup, tmp_path):
    """Download marked as TEMP_ERROR if size doesn't match metadata."""
    import gzip
    import json

    dl = _make_downloader(db_setup, tmp_path)

    # Pre-create metadata expecting size 1000
    from metadata_fetcher import metadata_path

    meta_file = metadata_path(dl.images_dir, MBID)
    os.makedirs(os.path.dirname(meta_file), exist_ok=True)
    metadata = {"result": [{"name": f"mbid-{MBID}-1000.jpg", "size": "1000", "md5": "abc"}]}
    with gzip.open(meta_file, "wt", encoding="utf-8") as f:
        json.dump(metadata, f)

    # Download returns content with wrong size
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"short"  # doesn't match expected 1000
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    result = dl._download_and_save_record(_make_record())
    assert result == (MBID, 1000)  # still returns success (file was written)

    with dl.datastore:
        record_db = dl.datastore.get(1000)
        assert record_db.status == CoverStatus.TEMP_ERROR.value
        assert "integrity" in record_db.error
