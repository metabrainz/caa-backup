"""Tests for store.py using in-memory SQLite."""

import pytest

from store import CAABackupDataStore, CoverStatus


@pytest.fixture
def datastore(tmp_path):
    """Create a fresh datastore with tables for each test."""
    from store import CAABackup, ImportTimestamp, db

    # Ensure clean state for the global db singleton
    if not db.is_closed():
        db.close()

    db_path = str(tmp_path / "test.db")
    ds = CAABackupDataStore(db_path=db_path)

    # pragma in __init__ opens the connection implicitly, just create tables
    if db.is_closed():
        db.connect()
    db.create_tables([CAABackup, ImportTimestamp])

    yield ds

    if not db.is_closed():
        db.close()


def test_create_tables(datastore):
    """Tables are created without error."""
    assert datastore.model.table_exists()


def test_bulk_add_and_get(datastore):
    records = [
        {"caa_id": 100, "release_mbid": "aaaa-bbbb", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 200, "release_mbid": "cccc-dddd", "mime_type": "image/png", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    datastore.bulk_add(records)
    record = datastore.get(100)
    assert record is not None
    assert record.release_mbid == "aaaa-bbbb"
    assert record.mime_type == "image/jpeg"
    assert record.status == CoverStatus.NOT_DOWNLOADED.value


def test_get_nonexistent(datastore):
    assert datastore.get(999) is None


def test_update_status(datastore):
    records = [
        {"caa_id": 100, "release_mbid": "aaaa-bbbb", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    datastore.bulk_add(records)
    datastore.update(caa_id=100, release_mbid="aaaa-bbbb", new_status=CoverStatus.DOWNLOADED)
    record = datastore.get(100)
    assert record.status == CoverStatus.DOWNLOADED.value


def test_update_nonexistent_returns(datastore):
    """update() on missing record should return without infinite loop."""
    # Should not hang — just logs and returns
    datastore.update(caa_id=999, release_mbid="xxxx", new_status=CoverStatus.DOWNLOADED)


def test_get_batch(datastore):
    records = [
        {"caa_id": i, "release_mbid": f"mbid-{i:04d}", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED}
        for i in range(10)
    ]
    datastore.bulk_add(records)
    batch = list(datastore.get_batch(count=5))
    assert len(batch) == 5


def test_get_status_counts(datastore):
    records = [
        {"caa_id": 1, "release_mbid": "a", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 2, "release_mbid": "b", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 3, "release_mbid": "c", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    datastore.bulk_add(records)
    datastore.update(caa_id=1, release_mbid="a", new_status=CoverStatus.DOWNLOADED)
    datastore.update(caa_id=2, release_mbid="b", new_status=CoverStatus.TEMP_ERROR, error="timeout")

    counts = datastore.get_status_counts()
    assert counts["DOWNLOADED"] == 1
    assert counts["TEMP_ERROR"] == 1
    assert counts["NOT_DOWNLOADED"] == 1


def test_bulk_update_downloaded_status(datastore):
    records = [
        {"caa_id": i, "release_mbid": f"m-{i}", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED}
        for i in range(5)
    ]
    datastore.bulk_add(records)
    datastore.bulk_update_downloaded_status([0, 1, 2])
    counts = datastore.get_status_counts()
    assert counts["DOWNLOADED"] == 3
    assert counts["NOT_DOWNLOADED"] == 2


def test_mark_all_as_undownloaded(datastore):
    records = [
        {"caa_id": 1, "release_mbid": "a", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    datastore.bulk_add(records)
    datastore.update(caa_id=1, release_mbid="a", new_status=CoverStatus.DOWNLOADED)
    datastore.mark_all_as_undownloaded()
    record = datastore.get(1)
    assert record.status == CoverStatus.NOT_DOWNLOADED.value


def test_get_failed(datastore):
    records = [
        {"caa_id": 1, "release_mbid": "a", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 2, "release_mbid": "b", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 3, "release_mbid": "c", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    datastore.bulk_add(records)
    datastore.update(caa_id=1, release_mbid="a", new_status=CoverStatus.TEMP_ERROR, error="timeout")
    datastore.update(caa_id=2, release_mbid="b", new_status=CoverStatus.PERMANENT_ERROR, error="404")
    failed = list(datastore.get_failed())
    assert len(failed) == 2


def test_import_timestamp(datastore):
    from datetime import datetime, timezone

    ts = datetime(2025, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
    assert datastore.get_last_import_timestamp() is None
    datastore.update_import_timestamp(ts)
    assert datastore.get_last_import_timestamp() == ts
