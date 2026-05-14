"""Fetch and store Internet Archive metadata for CAA releases.

Metadata is stored as gzipped JSON files alongside the images:
    {images_dir}/{a}/{b}/{release_mbid}.meta.json.gz

This provides checksums (md5, sha1, crc32) and file sizes for
integrity verification without requiring any database state.
"""

import gzip
import hashlib
import json
import logging
import os
import time

import requests

from helpers import parse_ia_filename, release_dir

IA_METADATA_URL = "https://archive.org/metadata/mbid-{release_mbid}/files"
USER_AGENT = "Cover Art Archive Backup (rob at metabrainz)"


def metadata_path(images_dir: str, release_mbid: str) -> str:
    """Return the path where metadata for a release should be stored."""
    return os.path.join(release_dir(images_dir, release_mbid), f"{release_mbid}.meta.json.gz")


def fetch_and_save_metadata(images_dir: str, release_mbid: str, timeout: int = 30) -> bool:
    """Fetch IA metadata for a release and save as .meta.json.gz.

    Returns True on success, False on failure.
    """
    url = IA_METADATA_URL.format(release_mbid=release_mbid)
    dest = metadata_path(images_dir, release_mbid)

    try:
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        response.raise_for_status()

        # Validate it's valid JSON with expected structure
        data = response.json()
        if not isinstance(data.get("result"), list):
            logging.warning(f"Unexpected metadata format for {release_mbid}")
            return False

        # Atomic write: gzip to unique .tmp, then rename
        import threading

        tmp_path = f"{dest}.{threading.get_ident()}.tmp"
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_path, dest)
        return True

    except (requests.RequestException, json.JSONDecodeError, OSError) as e:
        logging.warning(f"Failed to fetch metadata for {release_mbid}: {e}")
        return False


def load_metadata(images_dir: str, release_mbid: str) -> dict | None:
    """Load stored metadata for a release. Returns None if not available."""
    path = metadata_path(images_dir, release_mbid)
    if not os.path.exists(path):
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def get_expected_file_info(metadata: dict, filename: str) -> dict | None:
    """Find a file entry in IA metadata by filename.

    Returns dict with 'size', 'md5', 'sha1', 'crc32' or None.
    """
    for entry in metadata.get("result", []):
        if entry.get("name") == filename:
            return entry
    return None


def verify_file_integrity(filepath: str, expected: dict, check_md5: bool = False) -> str | None:
    """Verify a file against expected metadata.

    Returns None if OK, or a string describing the mismatch.
    """
    if not os.path.exists(filepath):
        return "file missing"

    actual_size = os.path.getsize(filepath)
    expected_size = int(expected.get("size", 0))

    if expected_size and actual_size != expected_size:
        return f"size mismatch: expected {expected_size}, got {actual_size}"

    if check_md5 and "md5" in expected:
        md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        if md5.hexdigest() != expected["md5"]:
            return f"md5 mismatch: expected {expected['md5']}, got {md5.hexdigest()}"

    return None


class MetadataFetcher:
    """Fetches IA metadata for releases during idle time."""

    def __init__(self, images_dir: str, rate_limit: float = 1.0):
        """
        Args:
            images_dir: Root images directory.
            rate_limit: Minimum seconds between requests.
        """
        self.images_dir = images_dir
        self.rate_limit = rate_limit
        self._shutdown_requested = False

    def get_releases_needing_metadata(self, max_scan: int = 10000) -> list:
        """Get release MBIDs that have images on disk but no metadata file.

        Scans the filesystem directly rather than querying the database,
        which is much faster for large datasets.
        """
        from helpers import parse_local_filename

        needing = []
        seen_mbids = set()
        scanned = 0

        for root, _, files in os.walk(self.images_dir):
            for file in files:
                if scanned >= max_scan:
                    return needing

                parsed = parse_local_filename(file)
                if not parsed:
                    continue

                mbid = parsed["release_mbid"]
                if mbid in seen_mbids:
                    continue
                seen_mbids.add(mbid)
                scanned += 1

                if not os.path.exists(metadata_path(self.images_dir, mbid)):
                    needing.append(mbid)

        return needing

    def run(self, max_fetches: int | None = None, deadline: float | None = None, stats=None):
        """Fetch metadata for releases that need it.

        Args:
            max_fetches: Stop after this many fetches (None = no limit).
            deadline: Stop after this time (unix timestamp). None = no limit.
            stats: Object with metadata_fetched attribute to update in real-time.
        """
        mbids = self.get_releases_needing_metadata()
        self.fetched = 0
        if not mbids:
            logging.info("All releases have metadata.")
            return

        logging.info(f"Fetching metadata for {len(mbids)} releases...")
        errors = 0

        for mbid in mbids:
            if self._shutdown_requested:
                break
            if max_fetches and self.fetched >= max_fetches:
                break
            if deadline and time.time() >= deadline:
                break

            if fetch_and_save_metadata(self.images_dir, mbid):
                self.fetched += 1
                if stats:
                    stats.metadata_fetched += 1
            else:
                errors += 1

            time.sleep(self.rate_limit)

        remaining = len(mbids) - self.fetched - errors
        logging.info(f"Metadata fetch complete: {self.fetched} fetched, {errors} errors, {remaining} remaining")


class IntegrityChecker:
    """Background integrity checker using stored IA metadata."""

    def __init__(self, images_dir: str, datastore=None, check_md5: bool = False, rate_limit: float = 0.1):
        """
        Args:
            images_dir: Root images directory.
            datastore: CAABackupDataStore instance (optional, for marking failures for re-download).
            check_md5: Whether to compute and verify MD5 (slow).
            rate_limit: Seconds between file checks (to limit I/O).
        """
        self.images_dir = images_dir
        self.datastore = datastore
        self.check_md5 = check_md5
        self.rate_limit = rate_limit
        self._shutdown_requested = False

    def run(self, max_checks: int | None = None, stats=None):
        """Walk images and verify against metadata.

        Returns list of (filepath, error_description) tuples for failures.
        Args:
            max_checks: Stop after this many checks (None = no limit).
            stats: Object with integrity_checked/integrity_failures attributes to update in real-time.
        """
        failures = []
        self.checked = 0

        for root, _, files in os.walk(self.images_dir):
            if self._shutdown_requested:
                break

            # Find .meta.json.gz files in this directory
            meta_files = [f for f in files if f.endswith(".meta.json.gz")]

            for meta_file in meta_files:
                if self._shutdown_requested:
                    break

                release_mbid = meta_file.replace(".meta.json.gz", "")
                meta_path = os.path.join(root, meta_file)

                try:
                    with gzip.open(meta_path, "rt", encoding="utf-8") as f:
                        metadata = json.load(f)
                except (OSError, json.JSONDecodeError):
                    continue

                # Check each image file for this release
                for entry in metadata.get("result", []):
                    if self._shutdown_requested:
                        break
                    if max_checks and self.checked >= max_checks:
                        return failures

                    name = entry.get("name", "")
                    parsed = parse_ia_filename(name)
                    if not parsed or parsed["release_mbid"] != release_mbid:
                        continue

                    filepath = os.path.join(root, f"{parsed['release_mbid']}-{parsed['caa_id']}.{parsed['ext']}")
                    error = verify_file_integrity(filepath, entry, check_md5=self.check_md5)
                    if error:
                        if error == "file missing":
                            logging.debug(f"File not on disk (likely deleted from CAA): {filepath}")
                        else:
                            failures.append((filepath, error))
                            logging.warning(f"Integrity check failed: {filepath}: {error}")
                            if stats:
                                stats.integrity_failures += 1

                            # Mark for re-download if datastore is available
                            if self.datastore:
                                from store import CoverStatus

                                self.datastore.update(
                                    caa_id=parsed["caa_id"],
                                    release_mbid=release_mbid,
                                    new_status=CoverStatus.NOT_DOWNLOADED,
                                    error=f"integrity: {error}",
                                )

                    self.checked += 1
                    if stats:
                        stats.integrity_checked += 1
                    if self.rate_limit:
                        time.sleep(self.rate_limit)

        logging.info(f"Integrity check complete: {self.checked} files checked, {len(failures)} failures")
        return failures
