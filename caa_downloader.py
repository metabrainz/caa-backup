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
from dotenv import load_dotenv
from store import CAABackupDataStore, CoverStatus
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self.headers = {'User-Agent': 'Cover Art Archive Backup'}
        self.batch_size = batch_size
        self.download_threads = download_threads

        # Ensure the base download directory exists
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            print(f"Created base cache directory: {self.cache_dir}")

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
            print(f"Error: A record object is missing a required attribute: {e}")
            return None, None
            
        url = f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}.jpg"

        mbid_prefix_1 = release_mbid[0]
        mbid_prefix_2 = release_mbid[1]
        target_dir = os.path.join(self.cache_dir, mbid_prefix_1, mbid_prefix_2)
        os.makedirs(target_dir, exist_ok=True)

        # The mime_type attribute is also expected for correct file extension.
        if hasattr(record, 'mime_type') and record.mime_type:
            extension = 'jpg' if record.mime_type == 'image/jpeg' else record.mime_type.split('/')[-1]
        else:
            extension = 'jpg'
        
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
                return release_mbid, caa_id # Success, exit the loop
            
            except requests.exceptions.HTTPError as e:
                # Handle HTTP errors
                if 400 <= e.response.status_code < 500:
                    status = CoverStatus.PERMANENT_ERROR
                    error = str(e)
                else: # 5xx server errors
                    status = CoverStatus.TEMP_ERROR
                    error = str(e)
                self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error)
                return None, None # Don't retry, just return
            
            except requests.exceptions.RequestException as e:
                # Handle other request exceptions like timeouts
                status = CoverStatus.TEMP_ERROR
                error = str(e)
                self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error)
                return None, None # Don't retry, just return
            
            except Exception as e:
                # Handle other unexpected errors
                status = CoverStatus.TEMP_ERROR
                error = str(e)
                if "database is locked" in str(e).lower():
                    retries += 1
                    time.sleep(0.1 * retries) # Exponential backoff
                else:
                    self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=status, error=error)
                    return None, None
        
        # If the loop finishes without success, mark as a temp error
        self.datastore.update(release_mbid=record.release_mbid, caa_id=record.caa_id, new_status=CoverStatus.TEMP_ERROR, error="Max retries reached due to database lock.")
        return None, None

    def run_downloader(self):
        """
        Fetches records from the datastore that need downloading, then downloads
        and saves the corresponding images using a thread pool.
        """
        print("Starting image download process...")
        
        with self.datastore:
            total_missing = self.datastore.get_undownloaded_count()
            
            if total_missing == 0:
                print("No records found with 'NOT_DOWNLOADED' status. Exiting.")
                return

            print(f"Found {total_missing} records to download. Starting threads...")

            with tqdm(total=total_missing, desc="Downloading images", unit="images") as pbar:
                with ThreadPoolExecutor(max_workers=self.download_threads) as executor:
                    while True:
                        records_to_download = self.datastore.get_batch(
                            status=CoverStatus.NOT_DOWNLOADED, 
                            count=self.batch_size
                        )

                        if not records_to_download:
                            break

                        # Submit download tasks to the thread pool
                        future_to_record = {executor.submit(self._download_and_save_record, record): record for record in records_to_download}
                        
                        # Process results as they become available
                        for future in as_completed(future_to_record):
                            try:
                                # Get the result of the completed future
                                result = future.result()
                                # The result is (mbid, caa_id) or (None, None) if failed
                                if result[0] is not None:
                                    pbar.update(1)
                                else:
                                    # Log a failed download, the specific error is handled in _download_and_save_record
                                    pass
                            except Exception as e:
                                # This block handles exceptions from the thread itself
                                print(f"A download task generated an exception: {e}")

            print("\nDownload process complete.")


# -----------------------------------------------------------------------------
# Main entry point for the script
# -----------------------------------------------------------------------------
@click.command()
def main():
    """
    Script to download missing cover art from a remote server.
    Configuration is read from a .env file.
    """
    # Load environment variables from a .env file
    load_dotenv()
    
    db_path = os.getenv('DB_PATH')
    cache_dir = os.getenv('CACHE_DIR')
    download_threads = os.getenv('DOWNLOAD_THREADS', '8')
    
    if not db_path:
        click.echo("Error: DB_PATH environment variable is not set.", err=True)
        return
    
    if not cache_dir:
        click.echo("Error: CACHE_DIR environment variable is not set.", err=True)
        return

    try:
        download_threads = int(download_threads)
    except ValueError:
        click.echo("Warning: DOWNLOAD_THREADS must be an integer. Defaulting to 8.", err=True)
        download_threads = 8

    if download_threads <= 0:
        click.echo("Warning: DOWNLOAD_THREADS must be greater than 0. Defaulting to 8.", err=True)
        download_threads = 8
        
    downloader = CAADownloader(
        db_path=db_path,
        cache_dir=cache_dir,
        download_threads=download_threads
    )
    downloader.run_downloader()


if __name__ == '__main__':
    main()
