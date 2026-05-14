"""Tests for helpers.py."""

from helpers import build_download_url, build_image_path, extension_from_mime, parse_ia_filename, parse_local_filename


def test_extension_from_mime_jpeg():
    assert extension_from_mime("image/jpeg") == "jpg"


def test_extension_from_mime_png():
    assert extension_from_mime("image/png") == "png"


def test_extension_from_mime_gif():
    assert extension_from_mime("image/gif") == "gif"


def test_extension_from_mime_none():
    assert extension_from_mime(None) == "jpg"


def test_extension_from_mime_empty():
    assert extension_from_mime("") == "jpg"


def test_build_download_url():
    url = build_download_url("ab5245f6-ae8d-49a5-be42-6347f6c0330e", 1347928453932, "jpg")
    assert url == (
        "https://archive.org/download/mbid-ab5245f6-ae8d-49a5-be42-6347f6c0330e"
        "/mbid-ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg"
    )


def test_build_image_path():
    path = build_image_path("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e", 1347928453932, "jpg")
    assert path == "/data/images/a/b/ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg"


def test_build_image_path_png():
    path = build_image_path("/data", "ff123456-0000-1111-2222-333344445555", 999, "png")
    assert path == "/data/f/f/ff123456-0000-1111-2222-333344445555-999.png"


def test_parse_local_filename():
    result = parse_local_filename("ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg")
    assert result == {"release_mbid": "ab5245f6-ae8d-49a5-be42-6347f6c0330e", "caa_id": 1347928453932, "ext": "jpg"}


def test_parse_local_filename_png():
    result = parse_local_filename("ff123456-0000-1111-2222-333344445555-999.png")
    assert result == {"release_mbid": "ff123456-0000-1111-2222-333344445555", "caa_id": 999, "ext": "png"}


def test_parse_local_filename_invalid():
    assert parse_local_filename("random.txt") is None
    assert parse_local_filename("short-name.jpg") is None
    assert parse_local_filename(".hidden") is None
    assert parse_local_filename("ab5245f6-ae8d-49a5-be42-6347f6c0330e-1000.jpg.tmp") is None


def test_parse_ia_filename():
    result = parse_ia_filename("mbid-ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg")
    assert result == {"release_mbid": "ab5245f6-ae8d-49a5-be42-6347f6c0330e", "caa_id": 1347928453932, "ext": "jpg"}


def test_parse_ia_filename_invalid():
    assert parse_ia_filename("__ia_thumb.jpg") is None
    assert parse_ia_filename("history/files/index.json") is None
    assert parse_ia_filename("ab5245f6-ae8d-49a5-be42-6347f6c0330e-1000.jpg") is None  # missing mbid- prefix
