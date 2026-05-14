"""Tests for store.py using in-memory SQLite."""

from store import CoverStatus


def test_create_tables(db_setup):
    ds, _ = db_setup
    assert ds.model.table_exists()


def test_bulk_add_and_get(db_setup):
    ds, _ = db_setup
    records = [
        {"caa_id": 100, "release_mbid": "aaaa-bbbb", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 200, "release_mbid": "cccc-dddd", "mime_type": "image/png", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)
    record = ds.get(100)
    assert record is not None
    assert record.release_mbid == "aaaa-bbbb"
    assert record.mime_type == "image/jpeg"
    assert record.status == CoverStatus.NOT_DOWNLOADED.value


def test_get_nonexistent(db_setup):
    ds, _ = db_setup
    assert ds.get(999) is None


def test_update_status(db_setup):
    ds, _ = db_setup
    records = [
        {"caa_id": 100, "release_mbid": "aaaa-bbbb", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)
    ds.update(caa_id=100, release_mbid="aaaa-bbbb", new_status=CoverStatus.DOWNLOADED)
    record = ds.get(100)
    assert record.status == CoverStatus.DOWNLOADED.value


def test_update_nonexistent_returns(db_setup):
    """update() on missing record should return without infinite loop."""
    ds, _ = db_setup
    ds.update(caa_id=999, release_mbid="xxxx", new_status=CoverStatus.DOWNLOADED)


def test_get_batch(db_setup):
    ds, _ = db_setup
    records = [
        {"caa_id": i, "release_mbid": f"mbid-{i:04d}", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED}
        for i in range(10)
    ]
    ds.bulk_add(records)
    batch = list(ds.get_batch(count=5))
    assert len(batch) == 5


def test_get_status_counts(db_setup):
    ds, _ = db_setup
    records = [
        {"caa_id": 1, "release_mbid": "a", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 2, "release_mbid": "b", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 3, "release_mbid": "c", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)
    ds.update(caa_id=1, release_mbid="a", new_status=CoverStatus.DOWNLOADED)
    ds.update(caa_id=2, release_mbid="b", new_status=CoverStatus.TEMP_ERROR, error="timeout")

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 1
    assert counts["TEMP_ERROR"] == 1
    assert counts["NOT_DOWNLOADED"] == 1


def test_bulk_update_downloaded_status(db_setup):
    ds, _ = db_setup
    records = [
        {"caa_id": i, "release_mbid": f"m-{i}", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED}
        for i in range(5)
    ]
    ds.bulk_add(records)
    ds.bulk_update_downloaded_status([0, 1, 2])
    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 3
    assert counts["NOT_DOWNLOADED"] == 2


def test_mark_all_as_undownloaded(db_setup):
    ds, _ = db_setup
    records = [
        {"caa_id": 1, "release_mbid": "a", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)
    ds.update(caa_id=1, release_mbid="a", new_status=CoverStatus.DOWNLOADED)
    ds.mark_all_as_undownloaded()
    record = ds.get(1)
    assert record.status == CoverStatus.NOT_DOWNLOADED.value


def test_get_failed(db_setup):
    ds, _ = db_setup
    records = [
        {"caa_id": 1, "release_mbid": "a", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 2, "release_mbid": "b", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 3, "release_mbid": "c", "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)
    ds.update(caa_id=1, release_mbid="a", new_status=CoverStatus.TEMP_ERROR, error="timeout")
    ds.update(caa_id=2, release_mbid="b", new_status=CoverStatus.PERMANENT_ERROR, error="404")
    failed = list(ds.get_failed())
    assert len(failed) == 2


def test_import_timestamp(db_setup):
    from datetime import datetime, timezone

    ds, _ = db_setup
    ts = datetime(2025, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
    assert ds.get_last_import_timestamp() is None
    ds.update_import_timestamp(ts)
    assert ds.get_last_import_timestamp() == ts
