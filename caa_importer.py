#!/usr/bin/env python3
#
# This module imports data from a PostgreSQL database into a local SQLite data store
# using a persistent connection and batched processing for large datasets.
# The importer has been updated to match the new datastore schema and now uses
# python-dotenv for environment variable management.
#
# Before running, ensure you have the required libraries installed:
# pip install peewee psycopg2-binary tqdm click python-dotenv requests
#
# You must also ensure that the 'store.py' file is in the same directory.

import os
import peewee
import sys
from datetime import datetime

import psycopg2
import click
from dotenv import load_dotenv
from store import CAABackupDataStore, CoverStatus
from tqdm import tqdm

# -----------------------------------------------------------------------------
# The main class for the import project.
# -----------------------------------------------------------------------------
class CAAImporter:
    """
    A class to handle importing data from a PostgreSQL database into
    a local SQLite data store.
    """
    def __init__(self, pg_conn_string: str, db_path: str, batch_size: int = 100000):
        """
        Initializes the importer with a PostgreSQL connection string.

        Args:
            pg_conn_string (str): The connection string for the PostgreSQL DB.
            db_path (str): The path to the local SQLite database file for the datastore.
            batch_size (int): The number of records to fetch per batch.
        """
        self.pg_conn_string = pg_conn_string
        self.batch_size = batch_size
        self.datastore = CAABackupDataStore(db_path=db_path)
        self.pg_conn = None

    def connect_to_postgres(self):
        """
        Establishes a connection to the PostgreSQL database.
        Returns the connection object if successful, otherwise None.
        """
        print("Connecting to PostgreSQL...")
        try:
            self.pg_conn = psycopg2.connect(self.pg_conn_string)
            print("Successfully connected to PostgreSQL.")
            return self.pg_conn
        except psycopg2.Error as e:
            print(f"PostgreSQL connection error: {e}")
            return None

    def get_caa_records(self, cursor: psycopg2.extensions.cursor):
        """
        Fetches a batch of CAA records from the PostgreSQL query result.

        Args:
            cursor (psycopg2.extensions.cursor): The cursor to fetch records from.

        Returns:
            list: A list of dictionaries representing database records.
        """
        # Fetch the next batch of records from the cursor.
        records_tuples = cursor.fetchmany(self.batch_size)

        # If no more records, return an empty list to signal the end.
        if not records_tuples:
            return []

        # Convert tuples to a list of dictionaries to match our datastore format.
        # The query returns caa.id, r.gid, and caa.mime_type.
        records_dict = []
        for row in records_tuples:
            records_dict.append({
                'caa_id': row[0],
                'release_mbid': row[1],
                'mime_type': row[2],  # New field added to the record
                'status': CoverStatus.NOT_DOWNLOADED
            })

        return records_dict

    def get_caa_records_with_date(self, cursor: psycopg2.extensions.cursor):
        """
        Fetches a batch of CAA records with date_uploaded from the PostgreSQL query result.

        Args:
            cursor (psycopg2.extensions.cursor): The cursor to fetch records from.

        Returns:
            list: A list of dictionaries representing database records including date_uploaded.
        """
        # Fetch the next batch of records from the cursor.
        records_tuples = cursor.fetchmany(self.batch_size)

        # If no more records, return an empty list to signal the end.
        if not records_tuples:
            return []

        # Convert tuples to a list of dictionaries to match our datastore format.
        # The query returns caa.id, r.gid, caa.mime_type, and caa.date_uploaded.
        records_dict = []
        for row in records_tuples:
            records_dict.append({
                'caa_id': row[0],
                'release_mbid': row[1],
                'mime_type': row[2],
                'date_uploaded': row[3],
                'status': CoverStatus.NOT_DOWNLOADED
            })

        return records_dict

    def run_import(self):
        """
        Connects to a PostgreSQL database, queries data, and imports it
        into the CAABackupDataStore in batches with a progress bar.
        """
        print("Starting import process...")
        
        # Initialize the data store's table
        self.datastore.create()
        
        # Connect to PostgreSQL once
        if not self.connect_to_postgres():
            print("Import failed due to database connection error.")
            return

        try:
            # Use a cursor to get the total count for the progress bar
            with self.pg_conn.cursor() as cursor:
                count_query = """SELECT count(*)
                                 FROM cover_art_archive.cover_art caa
                                 JOIN musicbrainz.release r
                                   ON caa.release = r.id"""
                cursor.execute(count_query)
                total_records = cursor.fetchone()[0]

            # Use a new cursor for the main data query
            with self.pg_conn.cursor() as cursor:
                # The main query to fetch the records, now including mime_type
                data_query = """SELECT caa.id, r.gid, caa.mime_type
                                 FROM cover_art_archive.cover_art caa
                                 JOIN musicbrainz.release r
                                   ON caa.release = r.id
                             ORDER BY r.gid"""
                print(f"Executing query to fetch data...")
                cursor.execute(data_query)

                # Open the datastore connection once for the entire import process.
                with self.datastore:
                    total_imported = 0
                    # Initialize the progress bar with the total count
                    with tqdm(total=total_records, desc="Importing records", unit="records") as pbar:
                        while True:
                            # Fetch a batch of records from the cursor
                            records = self.get_caa_records(cursor)

                            if not records:
                                break

                            # Use the datastore's `bulk_add` function
                            self.datastore.bulk_add(records)

                            # Update the progress bar
                            pbar.update(len(records))
                            total_imported += len(records)

            print(f"\nImport process complete. Total records imported: {total_imported}")

            # After import, update the import timestamp using the latest date_uploaded from Postgres
            latest_ts = self.datastore.fetch_latest_date_uploaded(self.pg_conn)
            if latest_ts:
                self.datastore.update_import_timestamp(latest_ts)
                print(f"Updated import timestamp to: {latest_ts}")
            else:
                print("Warning: Could not fetch latest date_uploaded from Postgres.")

        except psycopg2.Error as e:
            print(f"PostgreSQL query error: {e}")
        finally:
            if self.pg_conn:
                self.pg_conn.close()
                print("PostgreSQL connection closed.")

    def run_import_incremental(self):
        """
        Connects to a PostgreSQL database and imports only new records based on
        the date_uploaded timestamp. Updates the local timestamp after successful import.
        """
        print("Starting incremental import process...")
        
        # Initialize the data store's table
        self.datastore.create()
        
        # Connect to PostgreSQL once
        if not self.connect_to_postgres():
            print("Incremental import failed due to database connection error.")
            return

        try:
            with self.datastore:
                # Get the last import timestamp
                last_import_date = self.datastore.get_last_import_timestamp()
                
                if last_import_date:
                    print(f"Last import was at: {last_import_date}")
                    print("Fetching records uploaded since then...")
                else:
                    print("No previous import found, importing all records...")

                # Build the query with date filter if we have a last import date
                if last_import_date:
                    count_query = """SELECT count(*)
                                     FROM cover_art_archive.cover_art caa
                                     JOIN musicbrainz.release r
                                       ON caa.release = r.id
                                     WHERE caa.date_uploaded > %s"""
                    
                    data_query = """SELECT caa.id, r.gid, caa.mime_type, caa.date_uploaded
                                     FROM cover_art_archive.cover_art caa
                                     JOIN musicbrainz.release r
                                       ON caa.release = r.id
                                     WHERE caa.date_uploaded > %s
                                     ORDER BY caa.date_uploaded"""
                    query_params = (last_import_date,)
                else:
                    # If no previous import, fetch all records
                    count_query = """SELECT count(*)
                                     FROM cover_art_archive.cover_art caa
                                     JOIN musicbrainz.release r
                                       ON caa.release = r.id"""
                    
                    data_query = """SELECT caa.id, r.gid, caa.mime_type, caa.date_uploaded
                                     FROM cover_art_archive.cover_art caa
                                     JOIN musicbrainz.release r
                                       ON caa.release = r.id
                                     ORDER BY caa.date_uploaded"""
                    query_params = ()

                # Use a cursor to get the total count for the progress bar
                with self.pg_conn.cursor() as cursor:
                    cursor.execute(count_query, query_params)
                    total_records = cursor.fetchone()[0]
                    
                if total_records == 0:
                    print("No new records found to import.")
                    return
                    
                print(f"Found {total_records:,} new records to import.")
                
                # Use a new cursor for the main data query
                with self.pg_conn.cursor() as cursor:
                    print(f"Executing query to fetch new data...")
                    cursor.execute(data_query, query_params)

                    total_imported = 0
                    latest_date_uploaded = None
                    
                    # Initialize the progress bar with the total count
                    with tqdm(total=total_records, desc="Importing new records", unit="records") as pbar:
                        while True:
                            # Fetch a batch of records from the cursor
                            records = self.get_caa_records_with_date(cursor)

                            if not records:
                                break

                            # Track the latest date_uploaded for updating our timestamp
                            for record in records:
                                if record['date_uploaded']:
                                    if latest_date_uploaded is None or record['date_uploaded'] > latest_date_uploaded:
                                        latest_date_uploaded = record['date_uploaded']

                            # Use the datastore's `bulk_add` function
                            try:
                                self.datastore.bulk_add(records)
                                total_imported += len(records)
                            except Exception as e:
                                print(f"Error adding batch: {e}")
                                # Continue with next batch instead of stopping
                            
                            # Update the progress bar
                            pbar.update(len(records))

                    # Update the import timestamp if we imported any records
                    if total_imported > 0:
                        # Use the latest date_uploaded from Postgres, not just the last batch
                        latest_ts = self.datastore.fetch_latest_date_uploaded(self.pg_conn)
                        if latest_ts:
                            self.datastore.update_import_timestamp(latest_ts)
                            print(f"Updated import timestamp to: {latest_ts}")
                        else:
                            print("Warning: Could not fetch latest date_uploaded from Postgres.")
                    print(f"\nIncremental import complete. New records imported: {total_imported}")

        except psycopg2.Error as e:
            print(f"PostgreSQL query error: {e}")
        finally:
            if self.pg_conn:
                self.pg_conn.close()
                print("PostgreSQL connection closed.")


# -----------------------------------------------------------------------------
# Main entry point for the script
# -----------------------------------------------------------------------------
@click.command()
@click.option('--incremental', is_flag=True, help='Run incremental import (fetch only new records since last import)')
def main(incremental):
    """
    Script to import data from a PostgreSQL database into a local SQLite datastore.
    Configuration is read from a .env file.
    
    Use --incremental flag to import only records uploaded since the last import.
    """
    # Load environment variables from a .env file
    load_dotenv()
    
    pg_conn_string = os.getenv('PG_CONN_STRING')
    db_path = os.getenv('DB_PATH')
    print("CAA importer config:")
    print("  pg conn: '%s'" % pg_conn_string)
    print("  db path: '%s'" % db_path)
    print("  incremental: %s" % incremental)
    
    # For full import, check that database doesn't exist
    if not incremental and os.path.exists(db_path):
        print("The DB file %s exists. Please remove it before running this command, or use --incremental flag.")
        sys.exit(-1)

    # For incremental import, database should exist
    if incremental and not os.path.exists(db_path):
        print("Database file %s not found. Run a full import first (without --incremental flag).")
        sys.exit(-1)

    try:
        import consul_config

        if hasattr(consul_config, 'PG_CONN_STRING') and consul_config.PG_CONN_STRING:
            pg_conn_string = consul_config.PG_CONN_STRING
            print("pg conn string: '%s'" % pg_conn_string)
    except ImportError:
        pass

    # Ensure environment variables are set
    if not pg_conn_string:
        click.echo("Error: PG_CONN_STRING environment variable is not set.", err=True)
        return
    
    if not db_path:
        click.echo("Error: DB_PATH environment variable is not set.", err=True)
        return

    importer = CAAImporter(
        pg_conn_string=pg_conn_string,
        db_path=db_path,
        batch_size=1000
    )
    
    if incremental:
        importer.run_import_incremental()
    else:
        importer.run_import()

if __name__ == '__main__':
    main()
