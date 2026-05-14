"""Shared test fixtures for database setup."""

import pytest

from store import CAABackup, CAABackupDataStore, ImportTimestamp


@pytest.fixture
def db_setup(tmp_path):
    """Create a CAABackupDataStore with tables, yield (ds, db_path), then close."""
    db_path = str(tmp_path / "test.db")
    ds = CAABackupDataStore(db_path=db_path)
    ds.db.connect(reuse_if_open=True)
    ds.db.create_tables([CAABackup, ImportTimestamp])

    yield ds, db_path

    if not ds.db.is_closed():
        ds.db.close()
