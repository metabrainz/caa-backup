# store.py
#
# This module defines the SQLite datastore model for tracking cover art images
# that need to be downloaded, along with their status and metadata.
# This version has been updated to include the mime_type field while
# retaining the original function signatures and class names.

import peewee
import enum
import time

# Define a constant for the database retry delay
DB_RETRY_DELAY_SECONDS = .1

# -----------------------------------------------------------------------------
# Define the Enum for the record status.
# -----------------------------------------------------------------------------
class CoverStatus(enum.Enum):
    NOT_DOWNLOADED = 1
    DOWNLOADED = 2
    TEMP_ERROR = 3
    PERMANENT_ERROR = 4

# -----------------------------------------------------------------------------
# Define the PeeWee Model for our table.
# -----------------------------------------------------------------------------
db = peewee.SqliteDatabase(None)

class CAABackup(peewee.Model):
    """
    Represents the 'caa_backup' table with an updated schema.

    Fields:
    - caa_id: The CAA ID, which is the 64-bit integer primary key.
    - release_mbid: The MusicBrainz ID for the release.
    - status: The current status of the backup.
    - error: An optional text field to store error messages if the backup fails.
    - mime_type: The MIME type of the cover art image.
    """
    caa_id = peewee.BigIntegerField(primary_key=True, null=False)
    release_mbid = peewee.TextField(null=False)
    status = peewee.IntegerField(null=False)
    error = peewee.TextField(null=True)
    mime_type = peewee.TextField(null=True)  # New field to store MIME type

    class Meta:
        database = db  # This tells the model which database to use
        table_name = 'caa_backup'

    @property
    def status_enum(self):
        """Returns the status as a CoverStatus enum member."""
        return CoverStatus(self.status)

# -----------------------------------------------------------------------------
# The main class for the data store project.
# -----------------------------------------------------------------------------
class CAABackupDataStore:
    """
    A simple data store for managing CAA backup statuses using PeeWee.
    """
    def __init__(self, db_path='caa_backup.db'):
        """
        Initializes the data store.

        Args:
            db_path (str): The path to the SQLite database file.
        """
        self.db = db
        self.db.init(db_path)
        self.model = CAABackup

    def create(self):
        """
        Connects to the database and creates the table if it does not exist.
        """
        try:
            self.db.connect()
            if not self.model.table_exists():
                print("Creating table 'caa_backup'...")
                self.model.create_table(safe=True)
            else:
                print("Table 'caa_backup' already exists.")
        except peewee.OperationalError as e:
            print(f"Database error: {e}")
        finally:
            self.db.close()

    def add(self, caa_id: int, release_mbid: str, status: CoverStatus, mime_type: str, error: str = None):
        """Adds a new record to the database."""
        try:
            with self.db.atomic():
                self.model.create(
                    caa_id=caa_id, 
                    release_mbid=release_mbid, 
                    status=status.value, 
                    mime_type=mime_type,
                    error=error
                )
            print(f"Successfully added record for CAA ID: {caa_id}")
        except peewee.IntegrityError:
            print(f"Error: A record with CAA ID {caa_id} already exists.")

    def bulk_add(self, records: list):
        """
        Adds multiple records to the database in a single transaction.

        Args:
            records (list): A list of dictionaries, where each dictionary
                            represents a record. The 'status' key should be
                            a CoverStatus enum member.
        """
        if not records:
            print("No records to add.")
            return

        # Convert enum status to integer value and include mime_type
        records_for_db = [{
            'caa_id': r['caa_id'],
            'release_mbid': r['release_mbid'],
            'status': r['status'].value,
            'mime_type': r['mime_type'],
            'error': r.get('error')
        } for r in records]

        try:
            with self.db.atomic():
                self.model.insert_many(records_for_db).execute()
        except peewee.IntegrityError:
            print("Error: One or more records in the list already exist.")

    def get(self, caa_id: int):
        """Retrieves a single record by its CAA ID."""
        while True:
            try:
                return self.model.get_or_none(self.model.caa_id == caa_id)
            except peewee.OperationalError as e:
                print(f"Database error: {e}")
                return None
            except peewee.OperationalError as err:
                if "database is locked" in str(err):
                    time.sleep(DB_RETRY_DELAY_SECONDS)
                    continue
                raise err

    def get_batch(self, status: CoverStatus = CoverStatus.NOT_DOWNLOADED, count: int = 100):
        """
        Retrieves a batch of up to `count` records with the specified status.

        Args:
            status (CoverStatus): The status to filter by. Defaults to NOT_DOWNLOADED.
            count (int): The maximum number of records to retrieve.
        """
        while True:
            try:
                return self.model.select().where(
                    self.model.status == status.value
                ).order_by(self.model.release_mbid).limit(count)
            except peewee.OperationalError as e:
                print(f"Database error: {e}")
                return []
            except peewee.OperationalError as err:
                if "database is locked" in str(err):
                    time.sleep(DB_RETRY_DELAY_SECONDS)
                    continue
                raise err

    def update(self, caa_id: int, new_status: CoverStatus, error: str = None):
        """Updates the status and error for a specific record."""
        while True:
            try:
                record = self.model.get(self.model.caa_id == caa_id)
                record.status = new_status.value
                record.error = error
                record.save()
                return
            except self.model.DoesNotExist:
                print(f"Error: Record with CAA ID {caa_id} not found.")
            except peewee.OperationalError as err:
                if "database is locked" in str(err):
                    time.sleep(DB_RETRY_DELAY_SECONDS)
                    continue
                raise err

    def get_failed(self):
        """Retrieves all records with a 'failed' status."""
        while True:
            try:
                # We need to query for both temporary and permanent errors
                return self.model.select().where(
                    (self.model.status == CoverStatus.TEMP_ERROR.value) |
                    (self.model.status == CoverStatus.PERMANENT_ERROR.value)
                )
            except peewee.OperationalError as err:
                if "database is locked" in str(err):
                    time.sleep(DB_RETRY_DELAY_SECONDS)
                    continue
                raise []

    def get_undownloaded_count(self):
        """
        Retrieves the total count of records that have not been downloaded.
        """
        while True:
            try:
                return self.model.select().where(
                    self.model.status == CoverStatus.NOT_DOWNLOADED.value
                ).count()
            except peewee.OperationalError as err:
                if "database is locked" in str(err):
                    time.sleep(DB_RETRY_DELAY_SECONDS)
                    continue
                raise err

    def __enter__(self):
        """Context manager entry point. Opens the database connection."""
        self.db.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point. Closes the database connection."""
        self.db.close()
