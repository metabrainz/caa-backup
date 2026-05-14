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
│  /data/caa-backup/caa-backup/{a-f}/{a-f}/mbid-caa_id.ext      │ │
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

File storage layout: `{images_dir}/{mbid[0]}/{mbid[1]}/{release_mbid}-{caa_id}.{ext}`

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
            fetch IA metadata for releases missing it (~1 req/sec)
            run integrity checks on files with metadata (size comparison)
            sleep remaining time
```

## Configuration

### Environment Variables

| Variable | Description | Source in Docker |
|----------|-------------|-----------------|
| `DB_PATH` | SQLite database path | docker env |
| `IMAGES_DIR` | Images storage directory | docker env |
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
└── caa-backup/            # Images directory
    ├── a/
    │   ├── a/
    │   │   ├── aa123456-...-12345.jpg
    │   │   ├── aa123456-....meta.json.gz   # IA metadata (gzipped)
    │   │   └── aa789012-...-67890.png
    │   ├── b/
    │   │   ├── ab5245f6-...-1347928453932.jpg
    │   │   └── ab5245f6-....meta.json.gz
    │   └── ...
    ├── b/
    └── ...
```

Two-level prefix directories (first two chars of MBID) distribute files across 256 directories, avoiding filesystem performance issues with millions of files in one directory.

### Metadata Files

Each release can have a `.meta.json.gz` file containing the full Internet Archive
file listing (md5, sha1, crc32, size for each file in the IA item). These are
fetched during idle time and used for integrity verification. The metadata is
stored compressed alongside the images — no database state required.

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
    "total_to_download": 7164080,
    "downloaded": 7140212,
    "download_errors": 23868,
    "download_rate": 2.5,
    "disk_total_bytes": ...,
    "disk_free_bytes": ...,
    "disk_used_percent": ...,
    "seconds_before_full": ...,
    "seconds_before_completed": ...
}
```

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

- **Deleted images not cleaned up** — if an image is removed from CAA, the local copy remains. ~22K orphaned files currently exist (~0.3% of total).
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
