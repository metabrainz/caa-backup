# Cover Art Archive Backup

This project backs up the Cover Art Archive's original sized cover art images.


## Getting Started

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:

```bash
uv sync
```

Copy `dot-env-sample` to `.env` and edit it:

```bash
cp dot-env-sample .env
```

Environment variables:

* `PG_CONN_STRING` -- PostgreSQL connection string for a MusicBrainz database
* `DB_PATH="caa_backup.db"` -- path to the local SQLite database tracking progress
* `IMAGES_DIR="caa-backup"` -- directory where downloaded cover art images are stored
* `DOWNLOAD_THREADS=12` -- number of threads for simultaneous downloads

## Usage with `manage.py` (Recommended)

```bash
# View all available commands
uv run python manage.py --help

# Check system status and configuration
uv run python manage.py status

# Import data from PostgreSQL (first run)
uv run python manage.py import-data

# Download cover art images (this will take DAYS or WEEKS!)
uv run python manage.py download

# Verify local images against database
uv run python manage.py verify

# Start standalone monitoring server
uv run python manage.py monitor --port 8080
```

### Command Options

- `import-data`: Import from PostgreSQL to SQLite
  - `--batch-size INTEGER`: Records per batch (default: 1000)
  - `--force`: Overwrite existing database
  - `--incremental`: Import only new records since last import

- `download`: Download cover art images
  - `--threads INTEGER`: Download threads (default: 8)
  - `--batch-size INTEGER`: Records per batch (default: 1000)
  - `--monitor-port INTEGER`: Monitoring port (default: 8080)

- `verify`: Verify images against database
- `monitor`: Standalone monitoring server
  - `--port INTEGER`: Server port (default: 8080)
  - `--host TEXT`: Server host (default: localhost)

- `status`: Display system status and statistics

## First run

1. Import the database: `uv run python manage.py import-data`
2. Download images: `uv run python manage.py download`

## Subsequent runs

To keep the backup up-to-date:
1. Update with new records: `uv run python manage.py import-data --incremental`
2. Verify existing files: `uv run python manage.py verify`
3. Download new files: `uv run python manage.py download`

Alternatively, for a complete refresh:
1. Re-import all data: `uv run python manage.py import-data --force`
2. Verify existing files: `uv run python manage.py verify`
3. Download new files: `uv run python manage.py download`

## Docker

The container runs `caa_downloader.py` which operates in a loop:
1. Downloads all pending images
2. Runs an incremental import to fetch new records
3. Downloads any new images
4. Sleeps until the next hourly cycle

## Development

```bash
# Install with dev dependencies
uv sync --group dev

# Lint
uv run ruff check .

# Format
uv run ruff format .

# Type check
uv run ty check .
```
