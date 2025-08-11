#!/usr/bin/env python3
#
# This module uses a local SQLite data store to find missing cover art images,
# downloads them from a remote server, and saves them to a local directory
# organized by MBID. This version has been updated to support multithreaded
# downloading for improved performance.
#
# Before running, ensure you have the required libraries installed:
# pip install peewee psycopg2-binary tqdm click python-dotenv requests
#
# You must also ensure that the 'store.py' file is in the same directory and
# that your .env file has the necessary configuration.

import os
import requests
import click
from dotenv import load_dotenv
from store import CAABackupDataStore, CoverStatus
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

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
        self.batch_size = batch_size
        self.headers = {'User-Agent': 'Cover Art Archive Backup'}
        self.download_threads = download_threads

        # Ensure the base download directory exists
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            print(f"Created base cache directory: {self.cache_dir}")

    def _download_and_save_record(self, record):
        """
        Private method to download and save a single image record.
        This method is designed to be run in a separate thread.
        """
        release_mbid = record.release_mbid
        caa_id = record.caa_id
        url = f"https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}.jpg"

        mbid_prefix_1 = release_mbid[0]
        mbid_prefix_2 = release_mbid[1]
        target_dir = os.path.join(self.cache_dir, mbid_prefix_1, mbid_prefix_2)
        os.makedirs(target_dir, exist_ok=True)

        if record.mime_type:
            extension = 'jpg' if record.mime_type == 'image/jpeg' else record.mime_type.split('/')[-1]
        else:
            extension = 'jpg'
        
        filename = f"{release_mbid}-{caa_id}.{extension}"
        filepath = os.path.join(target_dir, filename)

        status = CoverStatus.DOWNLOADED
        error = None

        while True:
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()

                with open(filepath, 'wb') as f:
                    f.write(response.content)
                    
                break

            except requests.exceptions.HTTPError as e:
                if 400 <= e.response.status_code < 500:
                    status = CoverStatus.PERMANENT_ERROR
                    error = str(e)
                elif 500 <= e.response.status_code < 600:
                    status = CoverStatus.TEMP_ERROR
                    error = str(e)
            except requests.exceptions.RequestException as e:
                status = CoverStatus.TEMP_ERROR
                error = str(e)
            finally:
                self.datastore.update(caa_id=caa_id, new_status=status, error=error)
            
        return record.caa_id # Return something to indicate completion

    def run_downloader(self):
        """
        Fetches records from the datastore that need downloading, then downloads
        and saves the corresponding images using a thread pool.
        """
        print("Starting image download process...")
        
        with self.datastore:
            total_missing = self.datastore.model.select().where(
                self.datastore.model.status == CoverStatus.NOT_DOWNLOADED.value
            ).count()
            
            if total_missing == 0:
                print("No records found with 'NOT_DOWNLOADED' status. Exiting.")
                return

            with tqdm(total=total_missing, desc="Downloading images", unit="images") as pbar:
                with ThreadPoolExecutor(max_workers=self.download_threads) as executor:
                    while True:
                        records_to_download = self.datastore.get_batch(
                            status=CoverStatus.NOT_DOWNLOADED, 
                            count=self.batch_size
                        )

                        if not records_to_download:
                            break

                        future_to_record = {executor.submit(self._download_and_save_record, record): record for record in records_to_download}
                        for future in future_to_record:
                            future.result()
                            pbar.update(1)

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
        download_threads=download_threads,
        batch_size=1000
    )
    downloader.run_downloader()


if __name__ == '__main__':
    main()

