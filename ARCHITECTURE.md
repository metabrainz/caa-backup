# CAA Backup — Architecture & Design

## Overview

This project maintains a local backup of all original-size cover art images from the [Cover Art Archive](https://coverartarchive.org/) (CAA). Images are stored on the Internet Archive; this tool downloads them to local disk for redundancy.

As of May 2026, the backup contains ~7.1 million images.

## Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Container                          │
│                                                                 │
│  consul-template ──renders──> consul_config.py (PG_CONN_STRING) │
│       │                                                         │
│       └──exec──> start.sh ──> caa_downloader.py                 │
│                       │                                         │
│                       ├── CAAImporter (incremental)              │
│                       ├── CAADownloader (12 threads)             │
│                       └── CAAServiceMonitor (:8080/status)       │
│                                                                 │
│  SQLite DB ◄──────────────────────────────────────────────────┐ │
│  /data/caa-backup/caa-backup.db                               │ │
│                                                               │ │
│  Images directory                                             │ │
│  /data/caa-backup/caa-backup/{0-f}/{0-f}/{0-f}/mbid-caa_id.ext│ │
└───────────────────────────────────────────────────────────────┘ │
         │                                                        │
         │ PostgreSQL (read-only)                                  │
         ▼                                                        │
┌─────────────────┐                                               │
│ MusicBrainz DB  │  cover_art_archive.cover_art table            │
│ (pgbouncer)     │  → caa.id, release.gid, mime_type,            │
└─────────────────┘    date_uploaded                              │
                                                                  │
         │ HTTPS                                                  │
         ▼                                                        │
┌─────────────────┐                                               │
│ Internet Archive│  https://archive.org/download/mbid-{uuid}/    │
│                 │  mbid-{uuid}-{caa_id}.{ext}                   │
└─────────────────┘                                               │
```

## Data Flow

### 1. Discovery (what images exist)

The MusicBrainz PostgreSQL database contains `cover_art_archive.cover_art` — the canonical list of all cover art. Each row has:
- `id` — unique CAA image ID (64-bit integer)
- `release` — FK to `musicbrainz.release`
- `mime_type` — image/jpeg, image/png, etc.
- `date_uploaded` — when the image was uploaded to CAA

The importer queries this table (joining with `release` to get the MBID) and inserts records into the local SQLite database.

### 2. Tracking (what's been downloaded)

SQLite (`caa_backup` table) tracks each image's status:
- `NOT_DOWNLOADED` (0) — needs downloading
- `DOWNLOADED` (1) — successfully saved to disk
- `TEMP_ERROR` (2) — transient failure (timeout, 5xx), will retry
- `PERMANENT_ERROR` (3) — permanent failure (404), won't retry

### 3. Downloading (fetching the actual images)

The downloader reads batches of `NOT_DOWNLOADED` records from SQLite and fetches images from Internet Archive using 12 concurrent threads.

Each download:
1. Constructs URL: `https://archive.org/download/mbid-{release_mbid}/mbid-{release_mbid}-{caa_id}.{ext}`
2. Downloads to a `.tmp` file (atomic write)
3. Renames `.tmp` → final path via `os.replace()`
4. Updates SQLite status to `DOWNLOADED`

File storage layout: `{images_dir}/{prefix dirs}/{release_mbid}-{caa_id}.{ext}` (see Directory Depth below)

### 4. Verification (reconciling disk with database)

The verifier walks all files on disk and marks matching records as `DOWNLOADED`. Used after:
- Initial import (to detect files already on disk)
- Database loss recovery (rebuilds tracking state from files)

## Main Loop (caa_downloader.py)

```
startup:
    load config (env vars + consul_config.py)
    if no database: full import + verify
    start monitor thread on :8080

    download all pending images

    loop (hourly):
        incremental import (new records since last timestamp)
        download any new pending images
        idle time:
            run integrity checks on files with metadata (size comparison)
            fetch IA metadata for releases missing it (~1 req/sec, until next cycle)
```

## Configuration

### Environment Variables

| Variable | Description | Source in Docker |
|----------|-------------|-----------------|
| `DB_PATH` | SQLite database path | docker env |
| `IMAGES_DIR` | Images storage directory | docker env |
| `DIR_DEPTH` | Directory prefix depth (default: 2) | docker env |
| `DOWNLOAD_THREADS` | Concurrent download threads | docker env |
| `MONITOR_PORT` | HTTP status endpoint port | default 8080 |
| `PG_CONN_STRING` | PostgreSQL connection | consul_config.py |

Config priority: `consul_config.py` > environment variables > `.env` file (local dev only)

### Consul Integration

consul-template renders `consul_config.py` from `consul_config.py.ctmpl`:
- Resolves `pgbouncer-slave` service (preferred)
- Falls back to `pgbouncer-master` if slave unavailable
- Sets `PG_CONN_STRING = ""` if neither available

On service change, consul-template sends SIGHUP → process restarts with new config.

## File Layout

```
/data/caa-backup/
├── caa-backup.db          # SQLite tracking database
├── caa-backup.db-wal      # SQLite write-ahead log (WAL mode)
├── caa-backup.db-shm      # SQLite shared memory (WAL mode)
└── caa-backup/            # Images directory
    ├── .metadata_progress # Metadata fetch progress tracker
    ├── a/
    │   ├── b/
    │   │   ├── 5/
    │   │   │   ├── ab5245f6-...-1347928453932.jpg
    │   │   │   └── ab5245f6-....meta.json.gz
    │   │   └── ...
    │   └── ...
    └── ...
```

Prefix directories (based on the first N characters of the MBID, configurable via
`DIR_DEPTH`) distribute files across directories, avoiding filesystem performance
issues with millions of files in one directory.

### Directory Depth

The directory depth is configurable via the `DIR_DEPTH` environment variable (default: 2).

| Depth | Directories | Files per dir (~7M total) | Example path |
|-------|-------------|---------------------------|--------------|
| 0 | 1 | 7,000,000 | `images/ab52...-1000.jpg` |
| 1 | 16 | 437,500 | `images/a/ab52...-1000.jpg` |
| 2 | 256 | 27,000 | `images/a/b/ab52...-1000.jpg` |
| 3 | 4,096 | 1,700 | `images/a/b/5/ab52...-1000.jpg` |

To change depth, use the migration command:

```bash
# Preview what would be moved
uv run python manage.py migrate-dirs --new-depth 3 --dry-run

# Migrate (can be interrupted and resumed with --max-moves)
uv run python manage.py migrate-dirs --new-depth 3

# Migrate in batches
uv run python manage.py migrate-dirs --new-depth 3 --max-moves 500000
```

Migration uses `os.rename` (atomic, no data copy on same filesystem) and takes
approximately 5-10 minutes for 7M files. The downloader can run concurrently
since it uses `os.walk` (depth-agnostic) and writes new files at the configured depth.

### Metadata Files

Each release can have a `.meta.json.gz` file containing the full Internet Archive
file listing (md5, sha1, crc32, size for each file in the IA item). These are
fetched during idle time and used for integrity verification. The metadata is
stored compressed alongside the images — no database state required.

### Metadata Fetch Progress

The file `{images_dir}/.metadata_progress` tracks where the metadata fetcher
left off. It contains a single line: `{depth}:{prefix}` (e.g., `3:4/4/a`).

The fetcher iterates all prefix directories in sorted hex order (`0/0/0` → `f/f/f`),
fetching IA metadata for releases that don't have a `.meta.json.gz` file.
Progress is saved after each prefix directory so the next cycle resumes where
it left off.

When a full pass completes (all prefixes processed), the progress file is
deleted so the next cycle starts fresh — picking up any new releases or
retrying previous failures.

If `DIR_DEPTH` changes, progress resets automatically (the stored depth
won't match the current depth).

## Error Handling

- **HTTP 4xx** → `PERMANENT_ERROR` (image deleted or never existed)
- **HTTP 5xx** → `TEMP_ERROR` (Internet Archive issue, will retry next cycle)
- **Timeout/network** → `TEMP_ERROR`
- **Database locked** → retry with exponential backoff (up to 5 attempts)
- **SIGTERM** → sets shutdown flag, in-flight downloads finish, process exits cleanly
- **Crash during write** → `.tmp` file left on disk, final file never created, DB still says `NOT_DOWNLOADED`, next run retries
- **Integrity check failure** → record marked as `NOT_DOWNLOADED`, corrupt file will be overwritten on next download cycle

## Monitoring

HTTP endpoint at `:{MONITOR_PORT}/status` returns JSON:
```json
{
    "total_to_download": 7165557,
    "downloaded": 7141592,
    "download_errors": 23965,
    "download_rate": 2.5,
    "disk_total_bytes": 21826318385152,
    "disk_free_bytes": 7473833836544,
    "disk_used_percent": 65.02,
    "seconds_before_full": 83755746,
    "seconds_before_completed": 522529,
    "cycle_metadata_fetched": 1627,
    "cycle_integrity_checked": 114201,
    "cycle_integrity_failures": 0,
    "cycle_downloaded_files": 277,
    "cycle_downloaded_bytes": 54832640,
    "cycle_download_errors": 1
}
```

Field descriptions:

- `total_to_download` / `downloaded` / `download_errors`: cumulative totals from the database
- `download_rate`: instantaneous download rate (files/sec) from recent downloads
- `disk_*`: current disk usage for the images partition
- `seconds_before_full`: estimated seconds until disk is full, based on observed
  disk growth over a 1-hour sliding window (accounts for pauses and idle periods)
- `seconds_before_completed`: estimated seconds until all pending downloads finish,
  based on observed download rate over a 1-hour window (excludes permanent errors)
- `cycle_*`: per-cycle stats that reset each cycle (default: every hour, controlled by `UPDATE_FREQUENCY`):
  - `cycle_downloaded_files` / `cycle_downloaded_bytes` / `cycle_download_errors`:
    image downloads in the current/last download session (bytes includes metadata fetches)
  - `cycle_metadata_fetched`: metadata files fetched from Internet Archive
  - `cycle_integrity_checked` / `cycle_integrity_failures`: files verified in the last integrity run

## Disaster Recovery

This backup combined with the MusicBrainz PostgreSQL database contains everything
needed to fully reconstruct the Cover Art Archive:

- **Images:** All original-size files are on disk
- **Metadata:** Image types (front, back, booklet, etc.), comments, ordering, and
  edit history are in PostgreSQL (`cover_art_archive.*` tables)
- **index.json:** The per-release JSON served by the CAA API can be regenerated
  from PostgreSQL (same logic as [artwork-indexer](https://github.com/metabrainz/artwork-indexer))
- **Thumbnails:** Can be regenerated from the original images

The only data not recoverable are images that were deleted from both IA and
MusicBrainz before being backed up.

## Known Limitations

- **Deleted images not cleaned up** — if an image is removed from CAA, the local copy remains.
- **Metadata backfill is slow** — at 1 req/sec during idle time, backfilling metadata for all ~2.5M existing releases takes ~29 days. New releases are covered immediately.
- **No resume within a file** — large images that timeout are re-downloaded from scratch.

## Development

```bash
uv sync --group dev
pre-commit install
uv run pytest tests/
uv run ruff check .
uv run ruff format .
```

## Deployment

1. Merge to main
2. Tag: `git tag v-YYYY-MM-DD.N && git push origin --tags`
3. GitHub Actions builds and pushes Docker image
4. On oum: update tag in `docker-server-configs/scripts/nodes/oum.sh`
5. Restart container: `docker stop caa-backup && docker rm caa-backup && start_caa_backup 'v-...'`
