"""Tests for configurable directory depth and migration."""

import os
from unittest.mock import patch

from click.testing import CliRunner

from helpers import migrate_release_files, release_dir
from manage import cli

MBID = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"


def test_release_dir_default_depth():
    assert release_dir("/data", MBID) == "/data/a/b"


def test_release_dir_depth_3():
    assert release_dir("/data", MBID, depth=3) == "/data/a/b/5"


def test_release_dir_depth_4():
    assert release_dir("/data", MBID, depth=4) == "/data/a/b/5/2"


def test_release_dir_depth_0():
    assert release_dir("/data", MBID, depth=0) == "/data"


def test_migrate_release_files(tmp_path):
    images_dir = str(tmp_path)
    old_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(old_dir)
    old_path = os.path.join(old_dir, "test.jpg")
    with open(old_path, "w") as f:
        f.write("content")

    assert migrate_release_files(images_dir, MBID, "test.jpg", old_path, new_depth=3) is True

    new_path = os.path.join(images_dir, "a", "b", "5", "test.jpg")
    assert os.path.exists(new_path)
    assert not os.path.exists(old_path)


def test_migrate_release_files_already_in_place(tmp_path):
    images_dir = str(tmp_path)
    correct_dir = os.path.join(images_dir, "a", "b", "5")
    os.makedirs(correct_dir)
    path = os.path.join(correct_dir, "test.jpg")
    with open(path, "w") as f:
        f.write("content")

    assert migrate_release_files(images_dir, MBID, "test.jpg", path, new_depth=3) is False


def test_migrate_dirs_command_dry_run(tmp_path):
    images_dir = str(tmp_path / "images")
    old_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(old_dir)
    with open(os.path.join(old_dir, f"{MBID}-1000.jpg"), "w") as f:
        f.write("content")

    runner = CliRunner()
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3", "--dry-run"])

    assert result.exit_code == 0
    assert "Would move: 1" in result.output
    assert os.path.exists(os.path.join(old_dir, f"{MBID}-1000.jpg"))


def test_migrate_dirs_command_actual(tmp_path):
    images_dir = str(tmp_path / "images")
    old_dir = os.path.join(images_dir, "a", "b")
    os.makedirs(old_dir)
    with open(os.path.join(old_dir, f"{MBID}-1000.jpg"), "w") as f:
        f.write("content")
    with open(os.path.join(old_dir, f"{MBID}.meta.json.gz"), "w") as f:
        f.write("meta")

    runner = CliRunner()
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3"])

    assert result.exit_code == 0
    assert "Moved: 2" in result.output

    # Files should be at new depth
    new_dir = os.path.join(images_dir, "a", "b", "5")
    assert os.path.exists(os.path.join(new_dir, f"{MBID}-1000.jpg"))
    assert os.path.exists(os.path.join(new_dir, f"{MBID}.meta.json.gz"))
    # Old location should be empty
    assert not os.path.exists(os.path.join(old_dir, f"{MBID}-1000.jpg"))


def test_full_migration_workflow(tmp_path):
    """Integration test: create files at depth 2, migrate to 3, verify all moved correctly."""
    images_dir = str(tmp_path / "images")

    # Simulate multiple releases at depth 2
    releases = [
        ("ab5245f6-ae8d-49a5-be42-6347f6c0330e", [1000, 2000]),
        ("cd1234f6-ae8d-49a5-be42-6347f6c0330e", [3000]),
        ("ef9876f6-ae8d-49a5-be42-6347f6c0330e", [4000, 5000, 6000]),
    ]

    # Create files at depth 2
    for mbid, caa_ids in releases:
        d = os.path.join(images_dir, mbid[0], mbid[1])
        os.makedirs(d, exist_ok=True)
        for caa_id in caa_ids:
            with open(os.path.join(d, f"{mbid}-{caa_id}.jpg"), "wb") as f:
                f.write(f"image-{caa_id}".encode())
        # Also create metadata file
        with open(os.path.join(d, f"{mbid}.meta.json.gz"), "wb") as f:
            f.write(b"metadata")

    # Count total files
    total_files = sum(len(ids) + 1 for _, ids in releases)  # +1 for .meta.json.gz

    # Dry run first
    runner = CliRunner()
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3", "--dry-run"])
    assert result.exit_code == 0
    assert f"Found {total_files} files" in result.output
    assert f"Would move: {total_files}" in result.output

    # Verify nothing moved
    for mbid, caa_ids in releases:
        old_dir = os.path.join(images_dir, mbid[0], mbid[1])
        for caa_id in caa_ids:
            assert os.path.exists(os.path.join(old_dir, f"{mbid}-{caa_id}.jpg"))

    # Actual migration
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3"])
    assert result.exit_code == 0
    assert f"Moved: {total_files}" in result.output

    # Verify all files at new depth
    for mbid, caa_ids in releases:
        new_dir = os.path.join(images_dir, mbid[0], mbid[1], mbid[2])
        for caa_id in caa_ids:
            new_path = os.path.join(new_dir, f"{mbid}-{caa_id}.jpg")
            assert os.path.exists(new_path), f"Missing: {new_path}"
            assert open(new_path, "rb").read() == f"image-{caa_id}".encode()
        assert os.path.exists(os.path.join(new_dir, f"{mbid}.meta.json.gz"))

    # Verify old locations are empty
    for mbid, caa_ids in releases:
        old_dir = os.path.join(images_dir, mbid[0], mbid[1])
        for caa_id in caa_ids:
            assert not os.path.exists(os.path.join(old_dir, f"{mbid}-{caa_id}.jpg"))

    # Running again should find nothing to migrate
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3"])
    assert result.exit_code == 0
    assert "Found 0 files" in result.output


def test_migration_with_max_moves(tmp_path):
    """Migration respects --max-moves and can be resumed."""
    images_dir = str(tmp_path / "images")
    mbid = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"

    d = os.path.join(images_dir, "a", "b")
    os.makedirs(d)
    for i in range(5):
        with open(os.path.join(d, f"{mbid}-{i}.jpg"), "w") as f:
            f.write(f"img{i}")

    runner = CliRunner()

    # Move only 2
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3", "--max-moves", "2"])
    assert result.exit_code == 0
    assert "Moved: 2" in result.output

    # 3 remain at old location
    remaining_old = [f for f in os.listdir(d) if f.endswith(".jpg")]
    new_dir = os.path.join(images_dir, "a", "b", "5")
    moved_new = [f for f in os.listdir(new_dir) if f.endswith(".jpg")] if os.path.exists(new_dir) else []
    assert len(remaining_old) == 3
    assert len(moved_new) == 2

    # Resume — move the rest
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3"])
    assert result.exit_code == 0
    assert "Moved: 3" in result.output

    # All should be at new depth now
    assert len(os.listdir(new_dir)) == 5
    assert not any(f.endswith(".jpg") for f in os.listdir(d))


def test_dir_depth_env_var(tmp_path, monkeypatch):
    """DIR_DEPTH env var controls release_dir output."""
    from helpers import get_dir_depth, release_dir

    monkeypatch.setenv("DIR_DEPTH", "3")
    assert get_dir_depth() == 3
    assert release_dir("/data", MBID) == "/data/a/b/5"

    monkeypatch.setenv("DIR_DEPTH", "2")
    assert release_dir("/data", MBID) == "/data/a/b"

    monkeypatch.delenv("DIR_DEPTH")
    assert release_dir("/data", MBID) == "/data/a/b"


def test_migration_round_trip(tmp_path):
    """Migrate depth 2 -> 3 -> 1, verify files intact at each step."""
    images_dir = str(tmp_path / "images")
    mbid = "ab5245f6-ae8d-49a5-be42-6347f6c0330e"

    # Create at depth 2
    d2 = os.path.join(images_dir, "a", "b")
    os.makedirs(d2)
    with open(os.path.join(d2, f"{mbid}-1000.jpg"), "wb") as f:
        f.write(b"original content")
    with open(os.path.join(d2, f"{mbid}.meta.json.gz"), "wb") as f:
        f.write(b"meta content")

    runner = CliRunner()

    # Migrate to depth 3
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "3"])
    assert result.exit_code == 0
    assert "Moved: 2" in result.output

    d3 = os.path.join(images_dir, "a", "b", "5")
    assert os.path.exists(os.path.join(d3, f"{mbid}-1000.jpg"))
    assert not os.path.exists(os.path.join(d2, f"{mbid}-1000.jpg"))

    # Migrate to depth 1
    with patch.dict(os.environ, {"IMAGES_DIR": images_dir}):
        result = runner.invoke(cli, ["migrate-dirs", "--new-depth", "1"])
    assert result.exit_code == 0
    assert "Moved: 2" in result.output

    # Files should be at depth 1 (just first char)
    d1 = os.path.join(images_dir, "a")
    assert os.path.exists(os.path.join(d1, f"{mbid}-1000.jpg"))
    assert open(os.path.join(d1, f"{mbid}-1000.jpg"), "rb").read() == b"original content"
    assert os.path.exists(os.path.join(d1, f"{mbid}.meta.json.gz"))
    assert open(os.path.join(d1, f"{mbid}.meta.json.gz"), "rb").read() == b"meta content"
    assert not os.path.exists(os.path.join(d3, f"{mbid}-1000.jpg"))
