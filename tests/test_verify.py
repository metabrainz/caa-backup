"""Tests for caa_verify.py."""

import os

from caa_verify import CAAVerifier
from store import CoverStatus

MBID_A = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"
MBID_B = "cd1234f6-ae8d-49a5-be42-6347f6c0330e"
MBID_C = "ef9876f6-ae8d-49a5-be42-6347f6c0330e"


def _create_image_file(images_dir, release_mbid, caa_id, ext="jpg"):
    prefix_1 = release_mbid[0]
    prefix_2 = release_mbid[1]
    target_dir = os.path.join(images_dir, prefix_1, prefix_2)
    os.makedirs(target_dir, exist_ok=True)
    filepath = os.path.join(target_dir, f"{release_mbid}-{caa_id}.{ext}")
    with open(filepath, "wb") as f:
        f.write(b"fake image data")


def test_verifier_marks_existing_files_as_downloaded(db_setup, tmp_path):
    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
            {"caa_id": 2000, "release_mbid": MBID_B, "mime_type": "image/png", "status": CoverStatus.NOT_DOWNLOADED},
            {"caa_id": 3000, "release_mbid": MBID_C, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )

    _create_image_file(images_dir, MBID_A, 1000)
    _create_image_file(images_dir, MBID_B, 2000, "png")

    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 2
    assert counts["NOT_DOWNLOADED"] == 1


def test_verifier_resets_status_before_scan(db_setup, tmp_path):
    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )
    ds.update(caa_id=1000, release_mbid=MBID_A, new_status=CoverStatus.DOWNLOADED)

    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 0
    assert counts["NOT_DOWNLOADED"] == 1


def test_verifier_ignores_tmp_files(db_setup, tmp_path):
    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )

    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir, exist_ok=True)
    tmp_path_file = os.path.join(prefix_dir, f"{MBID_A}-1000.jpg.tmp")
    with open(tmp_path_file, "wb") as f:
        f.write(b"incomplete")

    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 0
    assert counts["NOT_DOWNLOADED"] == 1


def test_verifier_ignores_non_matching_files(db_setup, tmp_path):
    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )

    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir, exist_ok=True)
    for name in ["random.txt", "short-name.jpg", ".hidden", "no-extension"]:
        with open(os.path.join(prefix_dir, name), "wb") as f:
            f.write(b"junk")

    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 0
    assert counts["NOT_DOWNLOADED"] == 1


def test_verifier_handles_multiple_extensions(db_setup, tmp_path):
    ds, db_path = db_setup
    images_dir = str(tmp_path / "images")
    os.makedirs(images_dir)

    ds.bulk_add(
        [
            {"caa_id": 1000, "release_mbid": MBID_A, "mime_type": "image/jpeg", "status": CoverStatus.NOT_DOWNLOADED},
            {"caa_id": 2000, "release_mbid": MBID_B, "mime_type": "image/png", "status": CoverStatus.NOT_DOWNLOADED},
            {"caa_id": 3000, "release_mbid": MBID_C, "mime_type": "image/gif", "status": CoverStatus.NOT_DOWNLOADED},
        ]
    )

    _create_image_file(images_dir, MBID_A, 1000, "jpg")
    _create_image_file(images_dir, MBID_B, 2000, "png")
    _create_image_file(images_dir, MBID_C, 3000, "gif")

    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()

    counts = ds.get_status_counts()
    assert counts["DOWNLOADED"] == 3
    assert counts.get("NOT_DOWNLOADED", 0) == 0
