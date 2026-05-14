"""Tests for helpers.py."""

from helpers import build_download_url, build_image_path, extension_from_mime


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
