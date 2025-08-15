#!/usr/bin/env python3
#
# This module verifies the local cover art cache against the database records.
# It now first resets the status of all records to 'NOT_DOWNLOADED' and then
# updates the status of records for which a corresponding file exists in the
# cache to 'DOWNLOADED' in batches to improve performance and memory usage.

import os
import peewee
import click
import time
import logging
from dotenv import load_dotenv
from store import CAABackupDataStore, CoverStatus
from typing import List
# How often to log verification progress (in seconds)
VERIFY_PROGRESS_INTERVAL = 10


# -----------------------------------------------------------------------------
# Helper function for batching
# -----------------------------------------------------------------------------
def chunk_list(data: list, size: int):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(data), size):
        yield data[i:i + size]


# -----------------------------------------------------------------------------
# The main class for the verifier project.
# -----------------------------------------------------------------------------
class CAAVerifier:
    """
    A class to verify the local cover art cache against the database.
    It scans the local file system and updates the database records to ensure
    they accurately reflect the status of the downloaded files.
    """

    def __init__(self, db_path: str, cache_dir: str):
        """
        Initializes the verifier with paths to the datastore and cache directory.

        Args:
            db_path (str): The path to the local SQLite database file.
            cache_dir (str): The root directory where images are stored.
        """
        self.datastore = CAABackupDataStore(db_path=db_path)
        self.cache_dir = cache_dir

    def _get_caa_ids_from_cache(self) -> List[int]:
        """
        Scans the local cache directory and returns a list of all
        CAA IDs found in filenames. This is a memory-efficient way to
        build a lookup list for bulk updates.
        """
        logging.info("Scanning local cache for files...")
        found_caa_ids = []
        last_log = time.time()
        processed = 0
        for root, _, files in os.walk(self.cache_dir):
            for file in files:
                # Filename format: "mbid-uuid-caa_id.ext"
                parts = os.path.splitext(file)[0].split('-')
                if len(parts) >= 6:
                    try:
                        caa_id = int(parts[5])
                        found_caa_ids.append(caa_id)
                    except (ValueError, IndexError):
                        continue
                processed += 1
                now = time.time()
                if now - last_log >= VERIFY_PROGRESS_INTERVAL:
                    logging.info(f"Scanned {processed} files...")
                    last_log = now
        logging.info(f"Finished scanning. Total files processed: {processed}")
        return found_caa_ids

    def run_verifier(self):
        """
        Executes the verification process.
        """

    logging.info("Starting cache verification process...")

        with self.datastore:
            # Step 1: Mark all records in the database as NOT_DOWNLOADED.
            logging.info("Resetting all records to 'NOT_DOWNLOADED' status...")
            self.datastore.mark_all_as_undownloaded()

            # Step 2: Scan the cache and get a list of all found CAA IDs.
            on_disk_caa_ids = self._get_caa_ids_from_cache()

            # Step 3: Update the status of all found CAA IDs to DOWNLOADED in batches.

            if on_disk_caa_ids:
                logging.info(f"Applying bulk update for {len(on_disk_caa_ids)} downloaded records in batches...")
                total = len(on_disk_caa_ids)
                processed = 0
                last_log = time.time()
                for batch in chunk_list(on_disk_caa_ids, 1000):
                    self.datastore.bulk_update_downloaded_status(batch)
                    processed += len(batch)
                    now = time.time()
                    if now - last_log >= VERIFY_PROGRESS_INTERVAL:
                        logging.info(f"Updated {processed} / {total} records in DB...")
                        last_log = now
                logging.info(f"Finished updating DB. Total records updated: {processed}")
            else:
                logging.info("No downloaded records found in cache.")

    self._print_summary()
    logging.info("Verification complete.")

    def _print_summary(self):
        """
        Private method to fetch and print a summary of the download statuses.
        """
        with self.datastore:
            logging.info("--- Verification Summary ---")
            status_counts = self.datastore.get_status_counts()
            for status, count in status_counts.items():
                logging.info(f"- {status.replace('_', ' ').title()}: {count}")
            logging.info("----------------------------")


# -----------------------------------------------------------------------------
# Main entry point for the script
# -----------------------------------------------------------------------------
@click.command()
def main():
    """
    Script to verify the local cover art backup.
    Configuration is read from a .env file.
    """
    # Load environment variables from a .env file
    load_dotenv()

    db_path = os.getenv('DB_PATH')
    cache_dir = os.getenv('CACHE_DIR')

    if not db_path:
        click.echo("Error: DB_PATH environment variable is not set.", err=True)
        return

    if not cache_dir:
        click.echo("Error: CACHE_DIR environment variable is not set.", err=True)
        return

    verifier = CAAVerifier(db_path=db_path, cache_dir=cache_dir)
    verifier.run_verifier()


if __name__ == '__main__':
    main()
