"""Tests for caa_verify.py."""

import os

import pytest

from caa_verify import CAAVerifier
from store import CAABackup, CAABackupDataStore, CoverStatus, ImportTimestamp, db

MBID_A = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"
MBID_B = "cd1234f6-ae8d-49a5-be42-6347f6c0330e"
MBID_C = "ef9876f6-ae8d-49a5-be42-6347f6c0330e"


@pytest.fixture
def setup(tmp_path):
    """Create a datastore and images directory for testing."""
    if not db.is_closed():
        db.close()

    db_path = str(tmp_path / "test.db")
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds = CAABackupDataStore(db_path=db_path)
    if db.is_closed():
        db.connect()
    db.create_tables([CAABackup, ImportTimestamp])

    yield ds, images_dir, db_path

    if not db.is_closed():
        db.close()


def _create_image_file(images_dir, release_mbid, caa_id, ext="jpg"):
    """Create a fake image file in the expected directory structure."""
    prefix_1 = release_mbid[0]
    prefix_2 = release_mbid[1]
    target_dir = os.path.join(images_dir, prefix_1, prefix_2)
    os.makedirs(target_dir, exist_ok=True)
    filepath = os.path.join(target_dir, f"{release_mbid}-{caa_id}.{ext}")
    with open(filepath, "wb") as f:
        f.write(b"fake image data")


def test_verifier_marks_existing_files_as_downloaded(setup):
    ds, images_dir, db_path = setup

    # Add records to DB
    records = [
        {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 2000, "release_mbid": MBID_B, "mime_type": "image/png", "status": CoverStatus.NOT_DOWNLOADED},
        {"caa_id": 3000, "release_mbid": MBID_C, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)

    # Create files for only 2 of the 3 records
    _create_image_file(images_dir, MBID_A, 1000)
    _create_image_file(images_dir, MBID_B, 2000, "png")

    # Run verifier
    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    # Check results
    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 2
    assert counts["NOT_DOWNLOADED"] == 1


def test_verifier_resets_status_before_scan(setup):
    ds, images_dir, db_path = setup

    # Add a record already marked as DOWNLOADED
    records = [
        {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)
    ds.update(caa_id=1000, release_mbid=MBID_A, new_status=CoverStatus.DOWNLOADED)

    # Don't create the file — verifier should reset to NOT_DOWNLOADED
    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 0
    assert counts["NOT_DOWNLOADED"] == 1


def test_verifier_cleans_tmp_files(setup):
    ds, images_dir, db_path = setup

    records = [
        {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
    ]
    ds.bulk_add(records)

    # Create a .tmp file (should be deleted by verifier)
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir, exist_ok=True)
    tmp_path = os.path.join(prefix_dir, f"{MBID_A}-1000.jpg.tmp")
    with open(tmp_path, "wb") as f:
        f.write(b"incomplete")

    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    assert not os.path.exists(tmp_path)
    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 0
    assert counts["NOT_DOWNLOADED"] == 1
