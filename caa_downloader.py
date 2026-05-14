#!/usr/bin/env python3
#
# This module uses a local SQLite data store to find missing cover art images,
# downloads them from a remote server, and saves them to a local directory
# organized by MBID. This version has been updated to support multithreaded
# downloading for improved performance and includes a retry mechanism for
# database lock errors.
#
# Before running, ensure you have the required libraries installed:
# pip install peewee psycopg2-binary tqdm click python-dotenv requests
#
# You must also ensure that the 'store.py' file is in the same directory and
# that your .env file has the necessary configuration.

import logging
import os
import shutil
import signal
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import click
import requests
from dotenv import load_dotenv

from caa_importer import CAAImporter
from caa_monitor import CAAServiceMonitor
from caa_verify import CAAVerifier
from helpers import USER_AGENT, build_download_url, build_image_path, extension_from_mime
from metadata_fetcher import IntegrityChecker, MetadataFetcher
from store import CAABackupDataStore, CoverStatus

# How often to check for new images (in seconds)
UPDATE_FREQUENCY = 3600  # 1 hour
# How often to log download progress (in seconds)
DOWNLOAD_PROGRESS_INTERVAL = 10


# -----------------------------------------------------------------------------
# The main class for the downloader project.
# -----------------------------------------------------------------------------
class CAADownloader:
    """
    A class to handle downloading cover art images from a remote server and
    storing them locally based on a configured directory structure.
    """

    def __init__(self, db_path: str, images_dir: str, batch_size: int = 1000, download_threads: int = 8):
        """
        Initializes the downloader with paths to the datastore and download directory.

        Args:
            db_path (str): The path to the local SQLite database file.
            images_dir (str): The root directory where images will be saved.
            batch_size (int): The number of records to fetch and process per batch.
            download_threads (int): The number of threads to use for downloading.
        """
        self.datastore = CAABackupDataStore(db_path=db_path)
        self.images_dir = images_dir
        self.headers = {"User-Agent": USER_AGENT}
        self.batch_size = batch_size
        self.download_threads = download_threads
        self.total = 0
        self.downloaded = 0
        self.errors = 0
        self.metadata_fetched = 0
        self.integrity_checked = 0
        self.integrity_failures = 0
        self.lock = Lock()
        self._shutdown_requested = False

        # Queue to track download completion times for rate calculation
        self.download_times = deque(maxlen=25)

        # Ensure the base download directory exists
        if not os.path.exists(self.images_dir):
            os.makedirs(self.images_dir)
            logging.info(f"Created base images directory: {self.images_dir}")

    def get_download_rate(self):
        """
        Calculate the current download rate based on recent download completion times.

        Returns:
            float: Downloads per second, or 0 if insufficient data
        """
        with self.lock:
            if len(self.download_times) < 2:
                return 0.0

            # Calculate time elapsed since the oldest recorded download
            time_elapsed = self.download_times[-1] - self.download_times[0]

            if time_elapsed <= 0:
                return 0.0

            # Rate = number of downloads / time elapsed
            return (len(self.download_times) - 1) / time_elapsed

    def get_disk_usage_stats(self):
        """
        Safely retrieve disk usage statistics for the cache directory.

        Returns:
            tuple: (total_bytes, free_bytes, used_bytes, used_percent)
                   Returns (None, None, None, None) if unavailable.
        """
        if self.images_dir and os.path.exists(self.images_dir):
            try:
                total, used, free = shutil.disk_usage(self.images_dir)
                used_percent = (used / total * 100) if total else 0
                return total, free, used, used_percent
            except Exception as exc:
                logging.warning("Failed to get disk usage for images_dir '%s': %s", self.images_dir, exc)
        return None, None, None, None

    def estimate_seconds_before_full(self, download_rate, used_bytes, total_bytes, downloaded):
        """
        Estimate the number of seconds before the disk is full, based on current download rate.

        Returns:
            float or None: Seconds before full, or None if cannot estimate.
        """
        if not all([download_rate, used_bytes, total_bytes, downloaded]):
            return None
        avg_file_size = used_bytes / downloaded if downloaded else 0
        bytes_until_full = total_bytes - used_bytes
        if download_rate > 0 and avg_file_size > 0 and bytes_until_full > 0:
            files_left = bytes_until_full / avg_file_size
            return files_left / download_rate
        return None

    def estimate_seconds_before_completed(self, download_rate):
        """
        Estimate the number of seconds before all downloads are completed.

        Returns:
            float or None: Estimated seconds remaining, or None if cannot estimate.
        """
        if self.total is not None and self.downloaded is not None:
            files_left_to_download = self.total - self.downloaded
            if download_rate > 0 and files_left_to_download > 0:
                return files_left_to_download / download_rate
        return None

    def stats(self):
        """
        Return current download and disk usage statistics, including
        estimates of seconds before completion and before disk is full.

        Returns:
            dict: Download stats, disk usage, and time estimates.
        """

        download_rate = self.get_download_rate()
        total, free, used, used_percent = self.get_disk_usage_stats()
        seconds_before_full = self.estimate_seconds_before_full(download_rate, used, total, self.downloaded)
        seconds_before_completed = self.estimate_seconds_before_completed(download_rate)
        return {
            "total_to_download": self.total,
            "downloaded": self.downloaded,
            "download_errors": self.errors,
            "download_rate": download_rate,
            "disk_total_bytes": total,
            "disk_free_bytes": free,
            "disk_used_percent": used_percent,
            "seconds_before_full": seconds_before_full,
            "seconds_before_completed": seconds_before_completed,
            "metadata_fetched": self.metadata_fetched,
            "integrity_checked": self.integrity_checked,
            "integrity_failures": self.integrity_failures,
        }

    def _verify_after_download(self, release_mbid: str, caa_id: int, filepath: str, extension: str) -> str | None:
        """Verify a just-downloaded file against IA metadata.

        Fetches metadata if not already present. Returns None if OK or
        metadata unavailable, or an error string if verification fails.
        """
        from metadata_fetcher import (
            fetch_and_save_metadata,
            get_expected_file_info,
            load_metadata,
            metadata_path,
            verify_file_integrity,
        )

        # Fetch metadata if we don't have it yet
        meta_file = metadata_path(self.images_dir, release_mbid)
        if not os.path.exists(meta_file):
            # Re-check with lock to avoid multiple threads fetching the same release
            with self.lock:
                if not os.path.exists(meta_file):
                    if not fetch_and_save_metadata(self.images_dir, release_mbid):
                        return None  # Can't verify, not an error

        metadata = load_metadata(self.images_dir, release_mbid)
        if not metadata:
            return None

        ia_filename = f"mbid-{release_mbid}-{caa_id}.{extension}"
        expected = get_expected_file_info(metadata, ia_filename)
        if not expected:
            return None  # File not in metadata (shouldn't happen, but not an error)

        return verify_file_integrity(filepath, expected, check_md5=False)

    def _download_and_save_record(self, record):
        """
        Private method to download and save a single image record.
        This method is designed to be run in a separate thread.

        Returns the release_mbid and caa_id on completion.
        """
        # The record object must have 'mbid' and 'caa_id' attributes
        # to function correctly.
        try:
            release_mbid = record.release_mbid
            caa_id = record.caa_id
        except AttributeError as e:
            logging.error(f"A record object is missing a required attribute: {e}")
            return None, None

        mime_type = getattr(record, "mime_type", None)
        extension = extension_from_mime(mime_type)
        url = build_download_url(release_mbid, caa_id, extension)
        filepath = build_image_path(self.images_dir, release_mbid, caa_id, extension)

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        status = CoverStatus.DOWNLOADED
        error = None

        # Download the file first (no retry — network errors are handled separately)
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()

            tmp_filepath = filepath + ".tmp"
            with open(tmp_filepath, "wb") as f:
                f.write(response.content)
            os.replace(tmp_filepath, filepath)

        except requests.exceptions.HTTPError as e:
            if 400 <= e.response.status_code < 500:
                status = CoverStatus.PERMANENT_ERROR
                error = str(e)
                logging.error(str(e))
            else:
                status = CoverStatus.TEMP_ERROR
                error = str(e)
            self.datastore.update(
                release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error
            )
            return None, None

        except requests.exceptions.RequestException as e:
            status = CoverStatus.TEMP_ERROR
            error = str(e)
            logging.error(str(e))
            self.datastore.update(
                release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error
            )
            return None, None

        # Retry loop for DB update only (handles transient locks)
        max_retries = 5

        # Verify download against IA metadata if available
        integrity_error = self._verify_after_download(release_mbid, caa_id, filepath, extension)
        if integrity_error:
            status = CoverStatus.TEMP_ERROR
            error = f"integrity: {integrity_error}"
            logging.warning(f"Post-download verification failed for {filepath}: {integrity_error}")

        for attempt in range(max_retries):
            try:
                self.datastore.update(
                    release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error
                )

                with self.lock:
                    self.download_times.append(time.time())

                return release_mbid, caa_id
            except Exception as e:
                if "database is locked" in str(e).lower():
                    time.sleep(0.1 * (attempt + 1))
                else:
                    logging.error(str(e))
                    return None, None

        # Max retries exhausted
        self.datastore.update(
            release_mbid=record.release_mbid,
            caa_id=record.caa_id,
            new_status=CoverStatus.TEMP_ERROR,
            error="Max retries reached due to database lock.",
        )
        return None, None

    def run_downloader(self):
        """
        Fetches records from the datastore that need downloading, then downloads
        and saves the corresponding images using a thread pool.
        """

        with self.datastore:
            # Fetch existing status counts from database to set initial values
            status_counts = self.datastore.get_status_counts()
            self.downloaded = status_counts.get("DOWNLOADED", 0)
            self.errors = status_counts.get("TEMP_ERROR", 0) + status_counts.get("PERMANENT_ERROR", 0)
            self.total = sum(status_counts.values())
            pending = status_counts.get("NOT_DOWNLOADED", 0)

            if pending == 0:
                logging.info("No pending downloads.")
                return

            logging.info(f"Download status: {self.downloaded:,} done, {self.errors:,} errors, {pending:,} pending")

            try:
                with ThreadPoolExecutor(max_workers=self.download_threads) as executor:
                    total_to_download = self.total
                    downloaded_this_session = 0
                    errors_this_session = 0
                    start_time = time.time()
                    last_log = start_time
                    while not self._shutdown_requested:
                        records_to_download = self.datastore.get_batch(
                            status=CoverStatus.NOT_DOWNLOADED, count=self.batch_size
                        )

                        if not records_to_download:
                            break

                        # Submit download tasks to the thread pool
                        future_to_record = {
                            executor.submit(self._download_and_save_record, record): record
                            for record in records_to_download
                        }

                        # Process results as they become available
                        for future in as_completed(future_to_record):
                            try:
                                result = future.result()
                                if result[0] is not None:
                                    downloaded_this_session += 1
                                    self.downloaded += 1
                                else:
                                    errors_this_session += 1
                                    self.errors += 1
                            except Exception as e:
                                logging.error(f"A download task generated an exception: {e}")

                            now = time.time()
                            if now - last_log >= DOWNLOAD_PROGRESS_INTERVAL:
                                download_rate = self.get_download_rate()
                                rate_str = f" {download_rate:.2f}/sec " if download_rate > 0 else ""
                                logging.info(
                                    f"Downloaded: {self.downloaded} / {total_to_download}"
                                    f" {rate_str}Errors: {self.errors}"
                                )
                                last_log = now

                    logging.info(
                        f"Session complete: {downloaded_this_session} downloaded, {errors_this_session} errors"
                    )
            except KeyboardInterrupt:
                logging.info("Shutdown requested, waiting for in-flight downloads to finish...")
            return


# -----------------------------------------------------------------------------
# Main entry point for the script
# -----------------------------------------------------------------------------
@click.command()
def main():
    """
    Script to download missing cover art from a remote server.
    Configuration is read from a .env file.
    This version runs in a loop: download, incremental import, download new, sleep until next hour.
    """

    load_dotenv()

    db_path = os.getenv("DB_PATH")
    images_dir = os.getenv("IMAGES_DIR") or os.getenv("CACHE_DIR") or os.getenv("BACKUP_DIR")
    download_threads = os.getenv("DOWNLOAD_THREADS", "8")
    monitor_port = int(os.getenv("MONITOR_PORT", "8080"))
    pg_conn_string = os.getenv("PG_CONN_STRING")

    # In Docker, PG_CONN_STRING is provided by consul-template via consul_config.py
    try:
        import consul_config

        if hasattr(consul_config, "PG_CONN_STRING") and consul_config.PG_CONN_STRING:
            pg_conn_string = consul_config.PG_CONN_STRING
    except ImportError:
        pass

    if not db_path:
        logging.error("DB_PATH environment variable is not set.")
        return
    if not images_dir:
        logging.error("IMAGES_DIR environment variable is not set.")
        return
    if not pg_conn_string:
        logging.error("PG_CONN_STRING is not set (check env or consul_config).")
        return

    try:
        download_threads = int(download_threads)
    except ValueError:
        logging.warning("DOWNLOAD_THREADS must be an integer. Defaulting to 8.")
        download_threads = 8
    if download_threads <= 0:
        logging.warning("DOWNLOAD_THREADS must be greater than 0. Defaulting to 8.")
        download_threads = 8

    logging.info(
        "Config: db_path=%s images_dir=%s threads=%d pg=%s", db_path, images_dir, download_threads, pg_conn_string
    )

    if not os.path.exists(db_path):
        logging.info("No database found, running initial import...")
        importer = CAAImporter(pg_conn_string=pg_conn_string, db_path=db_path)
        importer.run_import()

        logging.info("Import complete, scanning local files...")
        verify = CAAVerifier(db_path=db_path, images_dir=images_dir)
        verify.run_verifier()

    downloader = CAADownloader(db_path=db_path, images_dir=images_dir, download_threads=download_threads)

    def _handle_sigterm(signum, frame):
        logging.info("Received SIGTERM, shutting down gracefully...")
        downloader._shutdown_requested = True

    signal.signal(signal.SIGTERM, _handle_sigterm)

    monitor = CAAServiceMonitor(downloader=downloader, port=monitor_port)
    monitor.start()

    downloader.run_downloader()
    while not downloader._shutdown_requested:
        cycle_start = time.time()

        logging.info("Running incremental import...")
        importer = CAAImporter(pg_conn_string=pg_conn_string, db_path=db_path)
        importer.run_import_incremental()

        downloader.run_downloader()

        cycle_end = time.time()
        elapsed = cycle_end - cycle_start
        now = time.time()
        next_cycle = ((int(now) // UPDATE_FREQUENCY) + 1) * UPDATE_FREQUENCY
        sleep_time = max(0, next_cycle - now) if elapsed < UPDATE_FREQUENCY else 0
        if sleep_time > 0:
            logging.info(f"Idle time: {int(sleep_time)}s — running background tasks...")

            # Integrity checks are fast (stat calls only) — run first
            if not downloader._shutdown_requested:
                checker = IntegrityChecker(
                    images_dir=images_dir, datastore=downloader.datastore, check_md5=False, rate_limit=0
                )
                checker._shutdown_requested = downloader._shutdown_requested
                checker.run(stats=downloader)

            # Metadata fetch uses remaining time until next cycle
            if not downloader._shutdown_requested:
                fetcher = MetadataFetcher(images_dir=images_dir, rate_limit=1.0)
                fetcher._shutdown_requested = downloader._shutdown_requested
                fetcher.run(deadline=next_cycle, stats=downloader)
        else:
            logging.info("Cycle took longer than the update frequency, starting next cycle immediately.")


if __name__ == "__main__":
    main()
