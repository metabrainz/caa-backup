"""Pure helper functions for CAA backup operations."""

import os


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


def build_image_path(images_dir: str, release_mbid: str, caa_id: int, extension: str) -> str:
    """Build the local file path for a cover art image.

    Images are stored in a two-level directory structure based on the first
    two characters of the release MBID.

    >>> build_image_path("/data/images", "ab5245f6-ae8d-49a5-be42-6347f6c0330e", 1347928453932, "jpg")
    '/data/images/a/b/ab5245f6-ae8d-49a5-be42-6347f6c0330e-1347928453932.jpg'
    """
    prefix_1 = release_mbid[0]
    prefix_2 = release_mbid[1]
    filename = f"{release_mbid}-{caa_id}.{extension}"
    return os.path.join(images_dir, prefix_1, prefix_2, filename)
