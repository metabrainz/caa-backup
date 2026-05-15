"""Tests for metadata_fetcher.py."""

import gzip
import json
import os
from unittest.mock import MagicMock, patch

from metadata_fetcher import (
    IntegrityChecker,
    fetch_and_save_metadata,
    get_expected_file_info,
    load_metadata,
    metadata_path,
    verify_file_integrity,
)

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


def test_metadata_path():
    path = metadata_path("/data/images", MBID)
    assert path == f"/data/images/a/b/{MBID}.meta.json.gz"


@patch("metadata_fetcher.requests.get")
def test_fetch_and_save_metadata(mock_get, tmp_path):
    images_dir = str(tmp_path / "images")
    os.makedirs(os.path.join(images_dir, "a", "b"))

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"result": [{"name": "test.jpg", "md5": "abc123", "size": "1000"}]}
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    assert fetch_and_save_metadata(images_dir, MBID) is True

    # File should exist and be valid gzipped JSON
    path = metadata_path(images_dir, MBID)
    assert os.path.exists(path)
    with gzip.open(path, "rt") as f:
        data = json.load(f)
    assert data["result"][0]["md5"] == "abc123"


@patch("metadata_fetcher.requests.get")
def test_fetch_metadata_failure(mock_get, tmp_path):
    images_dir = str(tmp_path / "images")
    os.makedirs(os.path.join(images_dir, "a", "b"))

    import requests

    mock_get.side_effect = requests.exceptions.Timeout("timeout")
    assert fetch_and_save_metadata(images_dir, MBID) is False
    assert not os.path.exists(metadata_path(images_dir, MBID))


def test_load_metadata(tmp_path):
    images_dir = str(tmp_path / "images")
    os.makedirs(os.path.join(images_dir, "a", "b"))

    data = {"result": [{"name": "test.jpg", "size": "500"}]}
    path = metadata_path(images_dir, MBID)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    loaded = load_metadata(images_dir, MBID)
    assert loaded == data


def test_load_metadata_missing(tmp_path):
    assert load_metadata(str(tmp_path), MBID) is None


def test_get_expected_file_info():
    metadata = {
        "result": [
            {"name": "other.jpg", "size": "100"},
            {"name": f"mbid-{MBID}-1000.jpg", "size": "5000", "md5": "abc"},
        ]
    }
    info = get_expected_file_info(metadata, f"mbid-{MBID}-1000.jpg")
    assert info["size"] == "5000"
    assert info["md5"] == "abc"


def test_get_expected_file_info_not_found():
    metadata = {"result": [{"name": "other.jpg"}]}
    assert get_expected_file_info(metadata, "missing.jpg") is None


def test_verify_file_integrity_ok(tmp_path):
    filepath = str(tmp_path / "test.jpg")
    with open(filepath, "wb") as f:
        f.write(b"x" * 100)

    expected = {"size": "100", "md5": "c7b1bfae720853e1ade818fb7e81ad0e"}
    assert verify_file_integrity(filepath, expected, check_md5=False) is None


def test_verify_file_integrity_size_mismatch(tmp_path):
    filepath = str(tmp_path / "test.jpg")
    with open(filepath, "wb") as f:
        f.write(b"x" * 50)

    expected = {"size": "100"}
    result = verify_file_integrity(filepath, expected)
    assert "size mismatch" in result


def test_verify_file_integrity_missing():
    result = verify_file_integrity("/nonexistent/file.jpg", {"size": "100"})
    assert result == "file missing"


def test_verify_file_integrity_md5_check(tmp_path):
    filepath = str(tmp_path / "test.jpg")
    content = b"hello world"
    with open(filepath, "wb") as f:
        f.write(content)

    import hashlib

    expected_md5 = hashlib.md5(content).hexdigest()
    expected = {"size": str(len(content)), "md5": expected_md5}
    assert verify_file_integrity(filepath, expected, check_md5=True) is None


def test_verify_file_integrity_md5_mismatch(tmp_path):
    filepath = str(tmp_path / "test.jpg")
    with open(filepath, "wb") as f:
        f.write(b"corrupted data")

    expected = {"size": "14", "md5": "0000000000000000000000000000dead"}
    result = verify_file_integrity(filepath, expected, check_md5=True)
    assert "md5 mismatch" in result


def test_integrity_checker_finds_issues(tmp_path):
    images_dir = str(tmp_path / "images")
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir)

    # Create a metadata file
    metadata = {
        "result": [
            {"name": f"mbid-{MBID}-1000.jpg", "size": "100", "md5": "abc"},
        ]
    }
    meta_path = os.path.join(prefix_dir, f"{MBID}.meta.json.gz")
    with gzip.open(meta_path, "wt", encoding="utf-8") as f:
        json.dump(metadata, f)

    # Create a file with wrong size
    filepath = os.path.join(prefix_dir, f"{MBID}-1000.jpg")
    with open(filepath, "wb") as f:
        f.write(b"short")

    checker = IntegrityChecker(images_dir=images_dir, check_md5=False, rate_limit=0)
    failures = checker.run()

    assert len(failures) == 1
    assert "size mismatch" in failures[0][1]


def test_integrity_checker_marks_for_redownload(db_setup, tmp_path):
    """Integrity failures mark the record as NOT_DOWNLOADED for re-download."""
    from store import CoverStatus

    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir)

    # Add a record marked as DOWNLOADED
    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )
    ds.update(caa_id=1000, release_mbid=MBID, new_status=CoverStatus.DOWNLOADED)

    # Create metadata with expected size 100
    metadata = {"result": [{"name": f"mbid-{MBID}-1000.jpg", "size": "100", "md5": "abc"}]}
    meta_path = os.path.join(prefix_dir, f"{MBID}.meta.json.gz")
    with gzip.open(meta_path, "wt", encoding="utf-8") as f:
        json.dump(metadata, f)

    # Create a file with wrong size (corrupt)
    filepath = os.path.join(prefix_dir, f"{MBID}-1000.jpg")
    with open(filepath, "wb") as f:
        f.write(b"short")

    checker = IntegrityChecker(images_dir=images_dir, datastore=ds, check_md5=False, rate_limit=0)
    failures = checker.run()

    assert len(failures) == 1

    # Record should be marked NOT_DOWNLOADED for re-download
    record = ds.get(1000)
    assert record.status == CoverStatus.NOT_DOWNLOADED.value
    assert "integrity" in record.error


def test_metadata_fetcher_nothing_to_fetch(db_setup, tmp_path):
    """MetadataFetcher.run() sets fetched=0 when all releases have metadata."""
    from metadata_fetcher import MetadataFetcher
    from store import CoverStatus

    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir)

    # Add a downloaded record
    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )
    ds.update(caa_id=1000, release_mbid=MBID, new_status=CoverStatus.DOWNLOADED)

    # Create the metadata file so it's already fetched
    meta_path = os.path.join(prefix_dir, f"{MBID}.meta.json.gz")
    with gzip.open(meta_path, "wt", encoding="utf-8") as f:
        json.dump({"result": []}, f)

    fetcher = MetadataFetcher(images_dir=images_dir, rate_limit=0)
    fetcher.run()

    assert fetcher.fetched == 0


def test_metadata_fetcher_progress(tmp_path, monkeypatch):
    """MetadataFetcher resumes from saved progress."""
    from metadata_fetcher import MetadataFetcher

    monkeypatch.setenv("DIR_DEPTH", "2")
    images_dir = str(tmp_path / "images")

    # Create releases in two different prefix dirs
    for prefix in ["a/a", "a/b", "f/f"]:
        d = os.path.join(images_dir, *prefix.split("/"))
        os.makedirs(d, exist_ok=True)
        mbid = f"{prefix.replace('/', '')}5245f6-ae8d-49a5-be42-6347f6c0330e"
        with open(os.path.join(d, f"{mbid}-1000.jpg"), "wb") as f:
            f.write(b"img")

    # First run with a very short deadline — should process some and save progress
    from unittest.mock import patch

    call_count = 0

    def mock_fetch(images_dir, release_mbid, timeout=30, **kwargs):
        nonlocal call_count
        call_count += 1
        # Create the metadata file
        path = os.path.join(images_dir, release_mbid[0], release_mbid[1], f"{release_mbid}.meta.json.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt") as f:
            json.dump({"result": []}, f)
        return True

    with patch("metadata_fetcher.fetch_and_save_metadata", side_effect=mock_fetch):
        fetcher = MetadataFetcher(images_dir=images_dir, rate_limit=0)
        fetcher.run()

    assert call_count == 3  # All three releases fetched

    # Progress file should be deleted after a complete pass
    progress_path = os.path.join(images_dir, ".metadata_progress")
    assert not os.path.exists(progress_path)


def test_metadata_fetcher_resumes_after_progress(tmp_path, monkeypatch):
    """MetadataFetcher skips already-processed prefixes."""
    from metadata_fetcher import MetadataFetcher

    monkeypatch.setenv("DIR_DEPTH", "2")
    images_dir = str(tmp_path / "images")

    # Create releases in a/a and f/f
    for prefix, char in [("a/a", "a"), ("f/f", "f")]:
        d = os.path.join(images_dir, *prefix.split("/"))
        os.makedirs(d, exist_ok=True)
        mbid = f"{char}{char}5245f6-ae8d-49a5-be42-6347f6c0330e"
        with open(os.path.join(d, f"{mbid}-1000.jpg"), "wb") as f:
            f.write(b"img")

    # Write progress as if we already processed up to "a/a"
    progress_path = os.path.join(images_dir, ".metadata_progress")
    with open(progress_path, "w") as f:
        f.write("2:a/a")

    from unittest.mock import patch

    fetched_mbids = []

    def mock_fetch(images_dir, release_mbid, timeout=30, **kwargs):
        fetched_mbids.append(release_mbid)
        path = os.path.join(images_dir, release_mbid[0], release_mbid[1], f"{release_mbid}.meta.json.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt") as f:
            json.dump({"result": []}, f)
        return True

    with patch("metadata_fetcher.fetch_and_save_metadata", side_effect=mock_fetch):
        fetcher = MetadataFetcher(images_dir=images_dir, rate_limit=0)
        fetcher.run()

    # Should only fetch f/f release (a/a was already processed)
    assert len(fetched_mbids) == 1
    assert fetched_mbids[0].startswith("ff")


def test_metadata_fetcher_partial_pass_saves_progress(tmp_path, monkeypatch):
    """Progress is saved when interrupted mid-pass (via shutdown flag)."""
    from metadata_fetcher import MetadataFetcher

    monkeypatch.setenv("DIR_DEPTH", "2")
    images_dir = str(tmp_path / "images")

    # Create releases in three prefixes
    for prefix in ["0/0", "0/1", "f/f"]:
        d = os.path.join(images_dir, *prefix.split("/"))
        os.makedirs(d, exist_ok=True)
        mbid = f"{prefix.replace('/', '')}5245f6-ae8d-49a5-be42-6347f6c0330e"
        with open(os.path.join(d, f"{mbid}-1000.jpg"), "wb") as f:
            f.write(b"img")

    from unittest.mock import patch

    fetch_count = [0]

    def mock_fetch(img_dir, release_mbid, timeout=30, **kwargs):
        fetch_count[0] += 1
        # After first fetch, request shutdown
        if fetch_count[0] >= 1:
            fetcher._shutdown_requested = True
        path = os.path.join(img_dir, release_mbid[0], release_mbid[1], f"{release_mbid}.meta.json.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt") as f:
            json.dump({"result": []}, f)
        return True

    with patch("metadata_fetcher.fetch_and_save_metadata", side_effect=mock_fetch):
        fetcher = MetadataFetcher(images_dir=images_dir, rate_limit=0)
        fetcher.run()

    # Should have fetched only 1 (shutdown after first)
    assert fetcher.fetched == 1

    # Progress should be saved (partial pass)
    progress_path = os.path.join(images_dir, ".metadata_progress")
    assert os.path.exists(progress_path)
    with open(progress_path) as f:
        content = f.read()
    assert content == "2:0/0"  # saved at first prefix


def test_metadata_fetcher_full_pass_resets_progress(tmp_path, monkeypatch):
    """Progress file is deleted after a complete pass."""
    from metadata_fetcher import MetadataFetcher

    monkeypatch.setenv("DIR_DEPTH", "2")
    images_dir = str(tmp_path / "images")

    # Create one release
    d = os.path.join(images_dir, "a", "b")
    os.makedirs(d)
    mbid = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"
    with open(os.path.join(d, f"{mbid}-1000.jpg"), "wb") as f:
        f.write(b"img")

    # Write a pre-existing progress file
    progress_path = os.path.join(images_dir, ".metadata_progress")
    with open(progress_path, "w") as f:
        f.write("2:0/0")

    from unittest.mock import patch

    def mock_fetch(img_dir, release_mbid, timeout=30, **kwargs):
        path = os.path.join(img_dir, release_mbid[0], release_mbid[1], f"{release_mbid}.meta.json.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt") as f:
            json.dump({"result": []}, f)
        return True

    with patch("metadata_fetcher.fetch_and_save_metadata", side_effect=mock_fetch):
        fetcher = MetadataFetcher(images_dir=images_dir, rate_limit=0)
        fetcher.run()

    assert fetcher.fetched == 1
    # Progress file should be deleted (full pass complete)
    assert not os.path.exists(progress_path)


def test_metadata_fetcher_deadline_stops_and_saves(tmp_path, monkeypatch):
    """Deadline interruption saves progress at the last completed prefix."""
    import time as time_mod

    from metadata_fetcher import MetadataFetcher

    monkeypatch.setenv("DIR_DEPTH", "2")
    images_dir = str(tmp_path / "images")

    # Create releases in 0/0 and f/f
    for prefix in ["0/0", "f/f"]:
        d = os.path.join(images_dir, *prefix.split("/"))
        os.makedirs(d, exist_ok=True)
        mbid = f"{prefix.replace('/', '')}5245f6-ae8d-49a5-be42-6347f6c0330e"
        with open(os.path.join(d, f"{mbid}-1000.jpg"), "wb") as f:
            f.write(b"img")

    from unittest.mock import patch

    fetch_count = [0]
    fake_time = [time_mod.time()]

    def mock_fetch(img_dir, release_mbid, timeout=30, **kwargs):
        fetch_count[0] += 1
        # Simulate time passing past deadline after first fetch
        fake_time[0] += 9999
        path = os.path.join(img_dir, release_mbid[0], release_mbid[1], f"{release_mbid}.meta.json.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with gzip.open(path, "wt") as f:
            json.dump({"result": []}, f)
        return True

    def mock_time():
        return fake_time[0]

    with (
        patch("metadata_fetcher.fetch_and_save_metadata", side_effect=mock_fetch),
        patch("metadata_fetcher.time.time", side_effect=mock_time),
    ):
        fetcher = MetadataFetcher(images_dir=images_dir, rate_limit=0)
        fetcher.run(deadline=time_mod.time() + 5)

    # Should have fetched only 1 (deadline expired after first)
    assert fetch_count[0] == 1

    # Progress should be saved
    progress_path = os.path.join(images_dir, ".metadata_progress")
    assert os.path.exists(progress_path)
