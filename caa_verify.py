#!/usr/bin/env python3
#
# This module verifies the local cover art cache against the database records.
# It now first resets the status of all records to 'NOT_DOWNLOADED' and then
# updates the status of records for which a corresponding file exists in the
# cache to 'DOWNLOADED' in batches to improve performance and memory usage.

import logging
import os
import time

import click
from dotenv import load_dotenv

from store import CAABackupDataStore

# How often to log verification progress (in seconds)
VERIFY_PROGRESS_INTERVAL = 10


# -----------------------------------------------------------------------------
# The main class for the verifier project.
# -----------------------------------------------------------------------------
class CAAVerifier:
    """
    A class to verify the local cover art cache against the database.
    It scans the local file system and updates the database records to ensure
    they accurately reflect the status of the downloaded files.
    """

    def __init__(self, db_path: str, images_dir: str):
        """
        Initializes the verifier with paths to the datastore and images directory.

        Args:
            db_path (str): The path to the local SQLite database file.
            images_dir (str): The root directory where images are stored.
        """
        self.datastore = CAABackupDataStore(db_path=db_path)
        self.images_dir = images_dir

    def _scan_and_update(self, batch_size: int = 1000):
        """
        Scans the images directory and updates the database in streaming batches.
        Avoids building a full list of all caa_ids in memory.
        """
        logging.info("Scanning local images directory for files...")
        batch = []
        processed = 0
        updated = 0
        tmp_cleaned = 0
        last_log = time.time()

        for root, _, files in os.walk(self.images_dir):
            for file in files:
                if file.endswith(".tmp"):
                    os.remove(os.path.join(root, file))
                    tmp_cleaned += 1
                    continue

                parts = os.path.splitext(file)[0].split("-")
                if len(parts) >= 6:
                    try:
                        caa_id = int(parts[5])
                        batch.append(caa_id)
                    except (ValueError, IndexError):
                        continue

                processed += 1

                if len(batch) >= batch_size:
                    self.datastore.bulk_update_downloaded_status(batch)
                    updated += len(batch)
                    batch = []

                if processed % 10000 == 0:
                    now = time.time()
                    if now - last_log >= VERIFY_PROGRESS_INTERVAL:
                        logging.info(f"Scanned {processed} files, updated {updated} records...")
                        last_log = now

        # Flush remaining batch
        if batch:
            self.datastore.bulk_update_downloaded_status(batch)
            updated += len(batch)

        logging.info(
            f"Finished scanning. Files processed: {processed}, "
            f"records updated: {updated}, tmp files cleaned: {tmp_cleaned}"
        )
        return updated

    def run_verifier(self):
        """
        Executes the verification process.
        """

        logging.info("Starting cache verification process...")

        with self.datastore:
            # Step 1: Mark all records in the database as NOT_DOWNLOADED.
            logging.info("Resetting all records to 'NOT_DOWNLOADED' status...")
            self.datastore.mark_all_as_undownloaded()

            # Step 2: Scan files and update DB in streaming batches.
            self._scan_and_update()

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

    db_path = os.getenv("DB_PATH")
    images_dir = os.getenv("IMAGES_DIR") or os.getenv("CACHE_DIR") or os.getenv("BACKUP_DIR")

    if not db_path:
        click.echo("Error: DB_PATH environment variable is not set.", err=True)
        return

    if not images_dir:
        click.echo("Error: IMAGES_DIR environment variable is not set.", err=True)
        return

    verifier = CAAVerifier(db_path=db_path, images_dir=images_dir)
    verifier.run_verifier()


if __name__ == "__main__":
    main()
