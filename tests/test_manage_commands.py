"""Tests for manage.py fetch-metadata and check-integrity commands."""

import gzip
import json
import os
from unittest.mock import patch

from click.testing import CliRunner

from manage import cli

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


def test_fetch_metadata_command(tmp_path):
    """fetch-metadata command runs and reports results."""
    images_dir = str(tmp_path / "images")
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir)

    # Create an image file so the fetcher finds a release
    with open(os.path.join(prefix_dir, f"{MBID}-1000.jpg"), "wb") as f:
        f.write(b"image")

    runner = CliRunner()
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        with patch("metadata_fetcher.requests.get") as mock_get:
            from unittest.mock import MagicMock

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"result": [{"name": f"mbid-{MBID}-1000.jpg", "size": "5"}]}
            mock_response.raise_for_status = MagicMock()
            mock_get.return_value = mock_response

            result = runner.invoke(cli, ["fetch-metadata", "--max-fetches", "1"])

    assert result.exit_code == 0
    assert "Fetched metadata for 1 releases" in result.output


def test_check_integrity_command(tmp_path):
    """check-integrity command runs and reports results."""
    images_dir = str(tmp_path / "images")
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir)

    # Create metadata and a matching file
    metadata = {"result": [{"name": f"mbid-{MBID}-1000.jpg", "size": "5", "md5": "abc"}]}
    meta_path = os.path.join(prefix_dir, f"{MBID}.meta.json.gz")
    with gzip.open(meta_path, "wt", encoding="utf-8") as f:
        json.dump(metadata, f)

    with open(os.path.join(prefix_dir, f"{MBID}-1000.jpg"), "wb") as f:
        f.write(b"12345")  # size 5, matches

    runner = CliRunner()
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir, "DB_PATH": ""}):
        result = runner.invoke(cli, ["check-integrity"])

    assert result.exit_code == 0
    assert "Checked 1 files, 0 failures" in result.output


def test_check_integrity_with_md5(tmp_path):
    """check-integrity --check-md5 verifies checksums."""
    import hashlib

    images_dir = str(tmp_path / "images")
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir)

    content = b"hello world"
    md5 = hashlib.md5(content).hexdigest()

    metadata = {"result": [{"name": f"mbid-{MBID}-1000.jpg", "size": str(len(content)), "md5": md5}]}
    meta_path = os.path.join(prefix_dir, f"{MBID}.meta.json.gz")
    with gzip.open(meta_path, "wt", encoding="utf-8") as f:
        json.dump(metadata, f)

    with open(os.path.join(prefix_dir, f"{MBID}-1000.jpg"), "wb") as f:
        f.write(content)

    runner = CliRunner()
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir, "DB_PATH": ""}):
        result = runner.invoke(cli, ["check-integrity", "--check-md5"])

    assert result.exit_code == 0
    assert "Checked 1 files, 0 failures" in result.output


def test_check_integrity_detects_corruption(tmp_path):
    """check-integrity reports size mismatches."""
    images_dir = str(tmp_path / "images")
    prefix_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(prefix_dir)

    metadata = {"result": [{"name": f"mbid-{MBID}-1000.jpg", "size": "1000", "md5": "abc"}]}
    meta_path = os.path.join(prefix_dir, f"{MBID}.meta.json.gz")
    with gzip.open(meta_path, "wt", encoding="utf-8") as f:
        json.dump(metadata, f)

    with open(os.path.join(prefix_dir, f"{MBID}-1000.jpg"), "wb") as f:
        f.write(b"short")  # size 5, expected 1000

    runner = CliRunner()
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir, "DB_PATH": ""}):
        result = runner.invoke(cli, ["check-integrity"])

    assert result.exit_code == 0
    assert "1 failures" in result.output
    assert "size mismatch" in result.output
