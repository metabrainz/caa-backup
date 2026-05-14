"""Tests for CAAImporter.run_import_incremental."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from caa_importer import CAAImporter
from store import CoverStatus


@patch("caa_importer.psycopg2.connect")
def test_incremental_import_fetches_new_records(mock_connect, db_setup):
    ds, db_path = db_setup
    ts = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    ds.update_import_timestamp(ts)

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    new_ts = datetime(2025, 5, 14, 12, 0, 0, tzinfo=timezone.utc)

    mock_count_cursor = MagicMock()
    mock_count_cursor.fetchone.return_value = (2,)

    mock_data_cursor = MagicMock()
    mock_data_cursor.fetchmany.side_effect = [
        [
            (5000, "aaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "image/jpeg", new_ts),
            (5001, "ffff-1111-2222-3333-444444444444", "image/png", new_ts),
        ],
        [],
    ]

    mock_max_cursor = MagicMock()
    mock_max_cursor.fetchone.return_value = (new_ts,)

    mock_conn.cursor.return_value.__enter__ = MagicMock(
        side_effect=[mock_count_cursor, mock_data_cursor, mock_max_cursor]
    )
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    imp = CAAImporter(pg_conn_string="dbname=test", db_path=db_path, batch_size=100)
    imp.run_import_incremental()

    r1 = ds.get(5000)
    assert r1 is not None
    assert r1.release_mbid == "aaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    assert r1.status == CoverStatus.NOT_DOWNLOADED.value

    r2 = ds.get(5001)
    assert r2 is not None
    assert r2.mime_type == "image/png"


@patch("caa_importer.psycopg2.connect")
def test_incremental_import_no_new_records(mock_connect, db_setup):
    ds, db_path = db_setup
    ts = datetime(2025, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
    ds.update_import_timestamp(ts)

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    mock_count_cursor = MagicMock()
    mock_count_cursor.fetchone.return_value = (0,)

    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_count_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    imp = CAAImporter(pg_conn_string="dbname=test", db_path=db_path, batch_size=100)
    imp.run_import_incremental()

    assert ds.get_last_import_timestamp() == ts


@patch("caa_importer.psycopg2.connect")
def test_incremental_import_connection_failure(mock_connect, db_setup):
    _, db_path = db_setup
    import psycopg2

    mock_connect.side_effect = psycopg2.Error("Connection refused")

    imp = CAAImporter(pg_conn_string="dbname=test", db_path=db_path, batch_size=100)
    imp.run_import_incremental()


@patch("caa_importer.psycopg2.connect")
def test_incremental_import_updates_timestamp_even_with_duplicates(mock_connect, db_setup):
    ds, db_path = db_setup
    old_ts = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    ds.update_import_timestamp(old_ts)

    ds.bulk_add(
        [
            {
                "caa_id": 5000,
                "release_mbid": "aaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "mime_type": "image/jpeg",
                "status": CoverStatus.NOT_DOWNLOADED,
            },
        ]
    )

    new_ts = datetime(2025, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    mock_count_cursor = MagicMock()
    mock_count_cursor.fetchone.return_value = (1,)

    mock_data_cursor = MagicMock()
    mock_data_cursor.fetchmany.side_effect = [
        [(5000, "aaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "image/jpeg", new_ts)],
        [],
    ]

    mock_max_cursor = MagicMock()
    mock_max_cursor.fetchone.return_value = (new_ts,)

    mock_conn.cursor.return_value.__enter__ = MagicMock(
        side_effect=[mock_count_cursor, mock_data_cursor, mock_max_cursor]
    )
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    imp = CAAImporter(pg_conn_string="dbname=test", db_path=db_path, batch_size=100)
    imp.run_import_incremental()

    assert ds.get_last_import_timestamp() == new_ts
