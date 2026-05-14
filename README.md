# Cover Art Archive Backup

This project backs up the Cover Art Archive's original sized cover art images.

For detailed system design, data flow, and deployment procedures, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Getting Started

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:

```bash
uv sync
cp dot-env-sample .env  # edit with your settings
```

Environment variables:

* `PG_CONN_STRING` -- PostgreSQL connection string for a MusicBrainz database
* `DB_PATH` -- path to the local SQLite database tracking progress
* `IMAGES_DIR` -- directory where downloaded cover art images are stored
* `DOWNLOAD_THREADS` -- number of threads for simultaneous downloads

## Usage

```bash
uv run python manage.py --help
uv run python manage.py status
uv run python manage.py import-data
uv run python manage.py download
uv run python manage.py verify
uv run python manage.py fetch-metadata
uv run python manage.py check-integrity
uv run python manage.py check-integrity --check-md5
uv run python manage.py monitor --port 8080
uv run python manage.py migrate-dirs --new-depth 3 --dry-run
```

### First run

1. `uv run python manage.py import-data`
2. `uv run python manage.py download`

### Subsequent runs

1. `uv run python manage.py import-data --incremental`
2. `uv run python manage.py verify`
3. `uv run python manage.py download`

## Development

```bash
uv sync --group dev
pre-commit install

uv run ruff check .
uv run ruff format .
uv run pytest tests/
uv run ty check .
```
