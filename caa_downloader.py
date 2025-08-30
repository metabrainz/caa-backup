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

import os
import requests
import click
import time
import shutil
import sys
from collections import deque
from dotenv import load_dotenv
from store import CAABackupDataStore, CoverStatus
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


from caa_monitor import CAAServiceMonitor
from caa_importer import CAAImporter


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

    def __init__(self, db_path: str, cache_dir: str, batch_size: int = 1000, download_threads: int = 8):
        """
        Initializes the downloader with paths to the datastore and download directory.

        Args:
            db_path (str): The path to the local SQLite database file.
            cache_dir (str): The root directory where images will be saved.
            batch_size (int): The number of records to fetch and process per batch.
            download_threads (int): The number of threads to use for downloading.
        """
        self.datastore = CAABackupDataStore(db_path=db_path)
        self.cache_dir = cache_dir
        self.headers = {'User-Agent': 'Cover Art Archive Backup (rob at metabrainz)'}
        self.batch_size = batch_size
        self.download_threads = download_threads
        self.total = 0
        self.downloaded = 0
        self.errors = 0
        self.pbar = None  # No longer used
        self.lock = Lock()
        
        # Queue to track download completion times for rate calculation
        self.download_times = deque(maxlen=25) 

        # Ensure the base download directory exists
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            logging.info(f"Created base cache directory: {self.cache_dir}")

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
        if self.cache_dir and os.path.exists(self.cache_dir):
            try:
                total, used, free = shutil.disk_usage(self.cache_dir)
                used_percent = (used / total * 100) if total else 0
                return total, free, used, used_percent
            except Exception as exc:
                logging.warning(
                    "Failed to get disk usage for cache_dir '%s': %s", self.cache_dir, exc
                )
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
        with self.lock:
            download_rate = self.get_download_rate()
            total, free, used, used_percent = self.get_disk_usage_stats()
            seconds_before_full = self.estimate_seconds_before_full(
                download_rate, used, total, self.downloaded
            )
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
            }

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

        # The mime_type attribute is also expected for correct file extension.
        if hasattr(record, 'mime_type') and record.mime_type:
            extension = 'jpg' if record.mime_type == 'image/jpeg' else record.mime_type.split('/')[-1]
        else:
            extension = 'jpg'

        url = f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}.{extension}"

        mbid_prefix_1 = release_mbid[0]
        mbid_prefix_2 = release_mbid[1]
        target_dir = os.path.join(self.cache_dir, mbid_prefix_1, mbid_prefix_2)
        os.makedirs(target_dir, exist_ok=True)


        filename = f"{release_mbid}-{caa_id}.{extension}"
        filepath = os.path.join(target_dir, filename)

        status = CoverStatus.DOWNLOADED
        error = None

        # Use a retry loop for transient database errors like locks
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                # The datastore update needs to be handled within the thread
                # to avoid lock contention, so we perform it here.
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()

                with open(filepath, 'wb') as f:
                    f.write(response.content)

                # Update the database
                self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error)
                
                # Record successful download time for rate calculation
                with self.lock:
                    self.download_times.append(time.time())
                
                return release_mbid, caa_id  # Success, exit the loop

            except requests.exceptions.HTTPError as e:
                # Handle HTTP errors
                if 400 <= e.response.status_code < 500:
                    status = CoverStatus.PERMANENT_ERROR
                    error = str(e)
                    logging.error(str(e))
                else:  # 5xx server errors
                    status = CoverStatus.TEMP_ERROR
                    error = str(e)
                self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error)
                return None, None  # Don't retry, just return

            except requests.exceptions.RequestException as e:
                # Handle other request exceptions like timeouts
                status = CoverStatus.TEMP_ERROR
                error = str(e)
                logging.error(str(e))
                self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error)
                return None, None  # Don't retry, just return

            except Exception as e:
                # Handle other unexpected errors
                status = CoverStatus.TEMP_ERROR
                error = str(e)
                if "database is locked" in str(e).lower():
                    retries += 1
                    time.sleep(0.1 * retries)  # Exponential backoff
                else:
                    logging.error(str(e))
                    self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error)
                    return None, None

        # If the loop finishes without success, mark as a temp error
        self.datastore.update(release_mbid=record.release_mbid,
                              caa_id=record.caa_id,
                              new_status=CoverStatus.TEMP_ERROR,
                              error="Max retries reached due to database lock.")
        return None, None

    def run_downloader(self):
        """
        Fetches records from the datastore that need downloading, then downloads
        and saves the corresponding images using a thread pool.
        """

        logging.info("Starting cover art download ...")


        with self.datastore:
            self.total = self.datastore.get_undownloaded_count()
            if self.total == 0:
                logging.info("No records found with 'NOT_DOWNLOADED' status. Exiting.")
                return

            logging.info(f"Found {self.total:,} covers to download. Starting threads...")

            try:
                with ThreadPoolExecutor(max_workers=self.download_threads) as executor:
                    total_to_download = self.total
                    downloaded = 0
                    errors = 0
                    start_time = time.time()
                    last_log = start_time
                    while True:
                        records_to_download = self.datastore.get_batch(status=CoverStatus.NOT_DOWNLOADED, count=self.batch_size)

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
                                    downloaded += 1
                                else:
                                    errors += 1
                            except Exception as e:
                                logging.error(f"A download task generated an exception: {e}")

                            now = time.time()
                            if now - last_log >= DOWNLOAD_PROGRESS_INTERVAL:
                                download_rate = self.get_download_rate()
                                rate_str = f" {download_rate:.2f}/sec " if download_rate > 0 else ""
                                logging.info(f"Downloaded: {downloaded} / {total_to_download} {rate_str} Errors: {errors}")
                                last_log = now

                    # Final progress log
                    logging.info(f"Downloaded: {downloaded} / {total_to_download} (Errors: {errors})")
            except KeyboardInterrupt:
                pass

            logging.info("Download process complete, exiting...")
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

    db_path = os.getenv('DB_PATH')
    cache_dir = os.getenv('CACHE_DIR')
    download_threads = os.getenv('DOWNLOAD_THREADS', '8')
    monitor_port = int(os.getenv('MONITOR_PORT', '8000'))
    pg_conn_string = os.getenv('PG_CONN_STRING')

    if not db_path:
        print("Error: DB_PATH environment variable is not set.")
        return
    if not cache_dir:
        print("Error: CACHE_DIR environment variable is not set.")
        return
    if not pg_conn_string:
        print("Error: PG_CONN_STRING environment variable is not set.")
        return

    try:
        download_threads = int(download_threads)
    except ValueError:
        print("Warning: DOWNLOAD_THREADS must be an integer. Defaulting to 8.")
        download_threads = 8
    if download_threads <= 0:
        print("Warning: DOWNLOAD_THREADS must be greater than 0. Defaulting to 8.")
        download_threads = 8


    logging.info("Current config")
    logging.info("  db_path: %s" % db_path)
    logging.info("  cache_dir: %s" % cache_dir)
    logging.info("  threads: %d" % download_threads)

    if not os.path.exists(db_path):
        logging.info("No database was found. Running caa_importer to create and populate the DB...")
        importer = CAAImporter(pg_conn_string=pg_conn_string, db_path=db_path)
        importer.run_import()
    logging.info("Database created and populated. Proceeding to download.")

    downloader = CAADownloader(db_path=db_path, cache_dir=cache_dir, download_threads=download_threads)
    monitor = CAAServiceMonitor(downloader=downloader, port=monitor_port)
    monitor.start()

    logging.info("--- Starting download cycle ---")
    downloader.run_downloader()
    while True:
        cycle_start = time.time()

        logging.info("--- Running incremental import to fetch new rows ---")
        importer = CAAImporter(pg_conn_string=pg_conn_string, db_path=db_path)
        importer.run_import_incremental()

        logging.info("--- Downloading any new images from incremental import ---")
        downloader.run_downloader()

        cycle_end = time.time()
        elapsed = cycle_end - cycle_start
        now = time.time()
        next_cycle = ((int(now) // UPDATE_FREQUENCY) + 1) * UPDATE_FREQUENCY
        sleep_time = max(0, next_cycle - now) if elapsed < UPDATE_FREQUENCY else 0
        if sleep_time > 0:
            logging.info(f"Cycle finished early, sleeping {int(sleep_time)} seconds until the next update...")
            time.sleep(sleep_time)
        else:
            logging.info("Cycle took longer than the update frequency, starting next cycle immediately.")

if __name__ == '__main__':
    main()
