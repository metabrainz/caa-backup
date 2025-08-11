Cover Art Archive BackupThis project provides a set of Python scripts to back up cover art from the MusicBrainz Cover Art Archive to a local file system. It works by first importing a list of available cover art from a PostgreSQL database into a local SQLite data store, and then using a multithreaded downloader to fetch the images.Getting StartedPrerequisitesTo run this project, you'll need the following installed:Python 3.xAccess to a MusicBrainz PostgreSQL database to run the importer.The project dependencies, which can be installed with pip.InstallationClone this repository or place the three Python files (store.py, caa_importer.py, and caa_downloader.py) in the same directory.Install the required Python packages by running the following command:pip install peewee psycopg2-binary tqdm click python-dotenv requests
ConfigurationThe project uses a .env file to manage all configuration settings. Create a file named .env in the root directory of the project and add the following variables:# PostgreSQL connection string for the MusicBrainz database.
# Replace the values with your database credentials.
# Example: "postgresql://user:password@host:port/database"
PG_CONN_STRING="your_postgres_connection_string"

# Path to the local SQLite database file.
# This file will be created automatically if it doesn't exist.
DB_PATH="caa_backup.db"

# Root directory where the downloaded cover art images will be cached.
CACHE_DIR="cache"

# Number of threads to use for concurrent downloads. The default is 8.
# Adjust this value based on your network speed and system resources.
DOWNLOAD_THREADS=8
UsageThe project is split into two main steps: importing and downloading.1. Import Records with caa_importer.pyThis script connects to your PostgreSQL database, queries the IDs and metadata for all available cover art, and saves them to the local SQLite data store (caa_backup.db).To run the importer, execute the following command:python caa_importer.py
The script will print its progress and the total number of records imported.2. Download Images with caa_downloader.pyOnce the local data store is populated, this script downloads the full-resolution cover art images. It uses a multithreaded approach with the number of threads specified in your .env file.To start the downloader, run:python caa_downloader.py
The script will display a progress bar showing the download status.Project Structurestore.py: Defines the CAABackupDataStore class and the CAABackup model for the SQLite database. It handles all database operations.caa_importer.py: Imports record metadata from a PostgreSQL database into the local data store.caa_downloader.py: Downloads the images from the Cover Art Archive and saves them to the specified cache directory.
