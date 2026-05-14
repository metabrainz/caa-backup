"""Pure helper functions for CAA backup operations."""

import os
import re

USER_AGENT = "Cover Art Archive Backup (admins@metabrainz.org)"

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


DEFAULT_DIR_DEPTH = 2


def get_dir_depth() -> int:
    """Get configured directory depth from DIR_DEPTH env var, or default."""
    try:
        return int(os.environ.get("DIR_DEPTH", DEFAULT_DIR_DEPTH))
    except ValueError:
        return DEFAULT_DIR_DEPTH


def release_dir(images_dir: str, release_mbid: str, depth: int | None = None) -> str:
    """Return the directory for a release's files.

    Uses a prefix structure based on the first N characters of the release
    MBID to distribute files across directories. Depth 0 means all files
    go directly in images_dir.

    >>> release_dir("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e")
    '/data/images/a/b'
    >>> release_dir("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e", depth=3)
    '/data/images/a/b/5'
    >>> release_dir("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e", depth=0)
    '/data/images'
    """
    if depth is None:
        depth = get_dir_depth()
    parts = [release_mbid[i] for i in range(depth)]
    return os.path.join(images_dir, *parts)


def build_image_path(images_dir: str, release_mbid: str, caa_id: int, extension: str) -> str:
    """Build the local file path for a cover art image.

    >>> build_image_path("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e", 1347928453932, "jpg")
    '/data/images/a/b/ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg'
    """
    filename = f"{release_mbid}-{caa_id}.{extension}"
    return os.path.join(release_dir(images_dir, release_mbid), filename)


def migrate_release_files(images_dir: str, release_mbid: str, filename: str, src_path: str, new_depth: int) -> bool:
    """Move a file to the correct directory for the given depth.

    Args:
        images_dir: Root images directory.
        release_mbid: The release MBID.
        filename: The filename.
        src_path: Current full path of the file.
        new_depth: Target directory depth.

    Returns True if file was moved, False if already in place.
    """
    new_dir = release_dir(images_dir, release_mbid, new_depth)
    new_path = os.path.join(new_dir, filename)

    if src_path == new_path:
        return False  # Already at correct location

    os.makedirs(new_dir, exist_ok=True)
    os.rename(src_path, new_path)
    return True
