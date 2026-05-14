"""Pure helper functions for CAA backup operations."""

import os
import re

# Regex for local filenames: {release_mbid}-{caa_id}.{ext}
LOCAL_FILENAME_RE = re.compile(r"^(?P<release_mbid>[0-9a-f-]{36})-(?P<caa_id>\d+)\.(?P<ext>\w+)$")

# Regex for IA filenames: mbid-{release_mbid}-{caa_id}.{ext}
IA_FILENAME_RE = re.compile(r"^mbid-(?P<release_mbid>[0-9a-f-]{36})-(?P<caa_id>\d+)\.(?P<ext>\w+)$")


def parse_local_filename(filename: str) -> dict | None:
    """Parse a local image filename into its components.

    >>> parse_local_filename("ab5245f6-ae8d-49a5-be42-6347f6c0330e-1000.jpg")
    {'release_mbid': 'ab5245f6-ae8d-49a5-be42-6347f6c0330e', 'caa_id': 1000, 'ext': 'jpg'}
    >>> parse_local_filename("not-a-valid-file.txt") is None
    True
    """
    m = LOCAL_FILENAME_RE.match(filename)
    if not m:
        return None
    return {"release_mbid": m.group("release_mbid"), "caa_id": int(m.group("caa_id")), "ext": m.group("ext")}


def parse_ia_filename(filename: str) -> dict | None:
    """Parse an Internet Archive image filename into its components.

    >>> parse_ia_filename("mbid-ab5245f6-ae8d-49a5-be42-6347f6c0330e-1000.jpg")
    {'release_mbid': 'ab5245f6-ae8d-49a5-be42-6347f6c0330e', 'caa_id': 1000, 'ext': 'jpg'}
    >>> parse_ia_filename("__ia_thumb.jpg") is None
    True
    """
    m = IA_FILENAME_RE.match(filename)
    if not m:
        return None
    return {"release_mbid": m.group("release_mbid"), "caa_id": int(m.group("caa_id")), "ext": m.group("ext")}


def extension_from_mime(mime_type: str | None) -> str:
    """Return file extension for a given MIME type.

    >>> extension_from_mime("image/jpeg")
    'jpg'
    >>> extension_from_mime("image/png")
    'png'
    >>> extension_from_mime(None)
    'jpg'
    """
    if not mime_type:
        return "jpg"
    if mime_type == "image/jpeg":
        return "jpg"
    return mime_type.split("/")[-1]


def build_download_url(release_mbid: str, caa_id: int, extension: str) -> str:
    """Build the Internet Archive download URL for a cover art image.

    >>> build_download_url("ab5245f6-ae8d-49a5-be42-6347f6c0330e", 1347928453932, "jpg")
    'https://archive.org/download/mbid-ab5245f6-ae8d-49a5-be42-6347f6c0330e/mbid-ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg'
    """
    return f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}.{extension}"


def release_dir(images_dir: str, release_mbid: str) -> str:
    """Return the directory for a release's files.

    Uses a two-level prefix structure based on the first two characters
    of the release MBID to distribute files across directories.

    >>> release_dir("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e")
    '/data/images/a/b'
    """
    return os.path.join(images_dir, release_mbid[0], release_mbid[1])


def build_image_path(images_dir: str, release_mbid: str, caa_id: int, extension: str) -> str:
    """Build the local file path for a cover art image.

    >>> build_image_path("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e", 1347928453932, "jpg")
    '/data/images/a/b/ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg'
    """
    filename = f"{release_mbid}-{caa_id}.{extension}"
    return os.path.join(release_dir(images_dir, release_mbid), filename)
