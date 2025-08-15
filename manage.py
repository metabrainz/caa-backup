#!/usr/bin/env python3
"""
CAA Backup Management Script

This script provides a unified interface for managing the Cover Art Archive (CAA) 
backup system. It includes commands for importing data, downloading images, 
verifying the cache, and monitoring the system.

Usage:
    python manage.py import      # Import data from PostgreSQL to SQLite
    python manage.py download    # Download cover art images
    python manage.py verify      # Verify local cache against database
    python manage.py monitor     # Start monitoring server
"""

import os
import sys
import click
from dotenv import load_dotenv

# Import the main classes from each module
from caa_importer import CAAImporter
from caa_downloader import CAADownloader
from caa_verify import CAAVerifier
from caa_monitor import CAAServiceMonitor


@click.group()
@click.version_option(version='1.0.0', prog_name='CAA Backup Manager')
def cli():
    """
    Cover Art Archive Backup Management System
    
    This tool provides commands to import, download, verify, and monitor
    cover art archive backups.
    """
    # Load environment variables for all commands
    load_dotenv()


@cli.command()
@click.option('--batch-size', default=1000, type=int, help='Number of records to process per batch (default: 1000)')
@click.option('--force', is_flag=True, help='Force import even if database file exists')
@click.option('--incremental', is_flag=True, help='Run incremental import (fetch only new records since last import)')
def import_data(batch_size, force, incremental):
    """
    Import data from PostgreSQL database into local SQLite datastore.
    
    This command connects to the PostgreSQL database specified in your .env file
    and imports cover art metadata into a local SQLite database for faster processing.
    
    Use --incremental to import only records uploaded since the last import.
    """
    pg_conn_string = os.getenv('PG_CONN_STRING')
    db_path = os.getenv('DB_PATH')
    
    click.echo("CAA Import Configuration:")
    click.echo(f"  PostgreSQL: {pg_conn_string}")
    click.echo(f"  Database: {db_path}")
    click.echo(f"  Batch size: {batch_size}")
    click.echo(f"  Incremental: {incremental}")
    
    if not pg_conn_string:
        click.echo("Error: PG_CONN_STRING environment variable is not set.", err=True)
        sys.exit(1)
    
    if not db_path:
        click.echo("Error: DB_PATH environment variable is not set.", err=True)
        sys.exit(1)
    
    # Check conditions based on import type
    if incremental:
        # For incremental import, database should exist
        if not os.path.exists(db_path):
            click.echo(f"Error: Database file '{db_path}' not found for incremental import.", err=True)
            click.echo("Run a full import first (without --incremental flag).", err=True)
            sys.exit(1)
    else:
        # For full import, check if database already exists
        if os.path.exists(db_path) and not force:
            click.echo(f"Error: Database file '{db_path}' already exists.", err=True)
            click.echo("Use --force to overwrite, --incremental to update, or remove the file manually.", err=True)
            sys.exit(1)
        
        if force and os.path.exists(db_path):
            click.echo(f"Removing existing database file: {db_path}")
            os.remove(db_path)
    
    try:
        # Try to import consul_config if available
        import consul_config
        if hasattr(consul_config, 'PG_CONN_STRING') and consul_config.PG_CONN_STRING:
            pg_conn_string = consul_config.PG_CONN_STRING
            click.echo("Using PostgreSQL connection from consul_config")
    except ImportError:
        pass
    
    importer = CAAImporter(
        pg_conn_string=pg_conn_string,
        db_path=db_path,
        batch_size=batch_size
    )
    
    try:
        if incremental:
            click.echo("Running incremental import...")
            importer.run_import_incremental()
        else:
            click.echo("Running full import...")
            importer.run_import()
        click.echo("Import completed successfully!")
    except Exception as e:
        click.echo(f"Import failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--threads', default=8, type=int, help='Number of download threads (default: 8)')
@click.option('--batch-size', default=1000, type=int, help='Number of records to fetch per batch (default: 1000)')
@click.option('--monitor-port', default=8080, type=int, help='Port for monitoring server (default: 8080)')
def download(threads, batch_size, monitor_port):
    """
    Download cover art images from remote server.
    
    This command downloads missing cover art images based on the records in your
    local database. It supports multithreaded downloading for improved performance.
    """
    db_path = os.getenv('DB_PATH')
    cache_dir = os.getenv('BACKUP_DIR')
    
    click.echo("CAA Download Configuration:")
    click.echo(f"  Database: {db_path}")
    click.echo(f"  Cache directory: {cache_dir}")
    click.echo(f"  Download threads: {threads}")
    click.echo(f"  Batch size: {batch_size}")
    click.echo(f"  Monitor port: {monitor_port}")
    
    if not db_path:
        click.echo("Error: DB_PATH environment variable is not set.", err=True)
        sys.exit(1)
    
    if not cache_dir:
        click.echo("Error: BACKUP_DIR environment variable is not set.", err=True)
        sys.exit(1)
    
    if not os.path.exists(db_path):
        click.echo(f"Error: Database file '{db_path}' not found.", err=True)
        click.echo("Run 'manage.py import' first to create the database.", err=True)
        sys.exit(1)
    
    if threads <= 0:
        click.echo("Warning: Thread count must be greater than 0. Using default of 8.")
        threads = 8
    
    try:
        downloader = CAADownloader(
            db_path=db_path, 
            cache_dir=cache_dir, 
            batch_size=batch_size,
            download_threads=threads
        )
        
        # Start monitoring server
        monitor = CAAServiceMonitor(downloader=downloader, port=monitor_port)
        monitor.start()
        
        click.echo(f"Monitoring server started on port {monitor_port}")
        click.echo("Access status at: http://localhost:{}/status".format(monitor_port))
        
        # Start downloading
        downloader.run_downloader()
        
    except KeyboardInterrupt:
        click.echo("\nDownload interrupted by user.")
    except Exception as e:
        click.echo(f"Download failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def verify():
    """
    Verify local cache against database records.
    
    This command scans your local cover art cache and updates the database
    to reflect the actual status of downloaded files. It resets all records
    to 'NOT_DOWNLOADED' and then marks files found in the cache as 'DOWNLOADED'.
    """
    db_path = os.getenv('DB_PATH')
    cache_dir = os.getenv('BACKUP_DIR')
    
    click.echo("CAA Verify Configuration:")
    click.echo(f"  Database: {db_path}")
    click.echo(f"  Cache directory: {cache_dir}")
    
    if not db_path:
        click.echo("Error: DB_PATH environment variable is not set.", err=True)
        sys.exit(1)
    
    if not cache_dir:
        click.echo("Error: BACKUP_DIR environment variable is not set.", err=True)
        sys.exit(1)
    
    if not os.path.exists(db_path):
        click.echo(f"Error: Database file '{db_path}' not found.", err=True)
        click.echo("Run 'manage.py import' first to create the database.", err=True)
        sys.exit(1)
    
    if not os.path.exists(cache_dir):
        click.echo(f"Error: Cache directory '{cache_dir}' not found.", err=True)
        sys.exit(1)
    
    try:
        verifier = CAAVerifier(db_path=db_path, cache_dir=cache_dir)
        verifier.run_verifier()
        click.echo("Verification completed successfully!")
    except Exception as e:
        click.echo(f"Verification failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--port', default=8080, type=int, help='Port to run monitoring server on (default: 8080)')
@click.option('--host', default='localhost', help='Host to bind monitoring server to (default: localhost)')
def monitor(port, host):
    """
    Start standalone monitoring server.
    
    This command starts a web server that provides status information about
    the CAA backup system. The server runs independently of the download process.
    """
    click.echo(f"Starting CAA monitoring server on {host}:{port}")
    click.echo(f"Access status at: http://{host}:{port}/status")
    click.echo("Press Ctrl+C to stop the server")
    
    try:
        # Create a dummy downloader-like object for standalone monitoring
        class StandaloneMonitor:
            def stats(self):
                return {
                    "message": "Standalone monitoring server",
                    "status": "running",
                    "note": "This is a standalone monitor. For download stats, use the download command."
                }
        
        standalone = StandaloneMonitor()
        monitor_server = CAAServiceMonitor(downloader=standalone, host=host, port=port)
        monitor_server.start()
        monitor_server.join()  # Wait for the thread to complete
        
    except KeyboardInterrupt:
        click.echo("\nMonitoring server stopped.")
    except Exception as e:
        click.echo(f"Monitoring server failed: {e}", err=True)
        sys.exit(1)


@cli.command()
def status():
    """
    Display current system status and configuration.
    
    This command shows the current environment configuration and basic
    statistics about your CAA backup system.
    """
    db_path = os.getenv('DB_PATH')
    cache_dir = os.getenv('BACKUP_DIR')
    pg_conn_string = os.getenv('PG_CONN_STRING')
    
    click.echo("=== CAA Backup System Status ===")
    click.echo(f"Database: {db_path}")
    click.echo(f"Cache directory: {cache_dir}")
    click.echo(f"PostgreSQL connection: {pg_conn_string}")
    
    # Check file/directory existence
    click.echo("\n=== File System Status ===")
    if db_path:
        if os.path.exists(db_path):
            size = os.path.getsize(db_path)
            click.echo(f"✓ Database file exists ({size:,} bytes)")
        else:
            click.echo("✗ Database file does not exist")
    
    if cache_dir:
        if os.path.exists(cache_dir):
            click.echo("✓ Cache directory exists")
            # Count files in cache directory
            file_count = 0
            for root, dirs, files in os.walk(cache_dir):
                file_count += len(files)
            click.echo(f"  Files in cache: {file_count:,}")
        else:
            click.echo("✗ Cache directory does not exist")
    
    # If database exists, show some statistics
    if db_path and os.path.exists(db_path):
        try:
            from store import CAABackupDataStore
            with CAABackupDataStore(db_path) as datastore:
                click.echo("\n=== Database Statistics ===")
                
                # Show last import timestamp
                last_import = datastore.get_last_import_timestamp()
                if last_import:
                    click.echo(f"Last import: {last_import}")
                else:
                    click.echo("Last import: Never")
                
                status_counts = datastore.get_status_counts()
                total_records = sum(status_counts.values())
                click.echo(f"Total records: {total_records:,}")
                for status, count in status_counts.items():
                    percentage = (count / total_records * 100) if total_records > 0 else 0
                    click.echo(f"  {status.replace('_', ' ').title()}: {count:,} ({percentage:.1f}%)")
        except Exception as e:
            click.echo(f"Could not read database statistics: {e}")


if __name__ == '__main__':
    cli()
