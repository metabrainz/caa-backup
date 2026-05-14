# CAA Backup — Maintenance Guide

Operational procedures for the caa-backup system running on oum.

## Running one-off commands

The running container uses the `/data` volume. You can run management commands
alongside it using a one-off container with the same volume — no need to stop
the running container:

```bash
docker run --rm \
  --volume /data:/data \
  --env IMAGES_DIR=/data/caa-backup/caa-backup \
  --env DB_PATH=/data/caa-backup/caa-backup.db \
  metabrainz/caa-backup:v-2026-05-14.7 \
  uv run python manage.py <command>
```

This works because:
- SQLite handles concurrent readers (WAL mode)
- `os.walk` and `os.rename` are safe with concurrent access
- The downloader only writes new files, never modifies existing ones

## Integrity check with MD5

Run a full checksum verification (reads every file, slow):

```bash
docker run --rm \
  --volume /data:/data \
  --env IMAGES_DIR=/data/caa-backup/caa-backup \
  --env DB_PATH=/data/caa-backup/caa-backup.db \
  metabrainz/caa-backup:v-2026-05-14.7 \
  uv run python manage.py check-integrity --check-md5
```

Limit scope for a quick spot-check:

```bash
docker run --rm \
  --volume /data:/data \
  --env IMAGES_DIR=/data/caa-backup/caa-backup \
  --env DB_PATH=/data/caa-backup/caa-backup.db \
  metabrainz/caa-backup:v-2026-05-14.7 \
  uv run python manage.py check-integrity --check-md5 --max-checks 1000
```

## Fetch metadata manually

Fetch IA metadata faster than the background task (e.g., for backfill):

```bash
docker run --rm \
  --volume /data:/data \
  --env IMAGES_DIR=/data/caa-backup/caa-backup \
  metabrainz/caa-backup:v-2026-05-14.7 \
  uv run python manage.py fetch-metadata --rate-limit 0.5
```

## Change directory depth

Changing the directory structure requires stopping the main container:

```bash
# 1. Stop the running container
docker stop caa-backup && docker rm caa-backup

# 2. Preview the migration
docker run --rm \
  --volume /data:/data \
  --env IMAGES_DIR=/data/caa-backup/caa-backup \
  metabrainz/caa-backup:v-2026-05-14.7 \
  uv run python manage.py migrate-dirs --new-depth 3 --dry-run

# 3. Run the migration (~5-10 minutes for 7M files)
docker run --rm \
  --volume /data:/data \
  --env IMAGES_DIR=/data/caa-backup/caa-backup \
  metabrainz/caa-backup:v-2026-05-14.7 \
  uv run python manage.py migrate-dirs --new-depth 3

# 4. Restart with new depth (add DIR_DEPTH to services.sh)
start_caa_backup 'v-2026-05-14.7'
```

The container must be stopped for migration because:
- The downloader would write new files at the old depth during migration
- `os.rename` across directories needs exclusive access for consistency

## Check system status

```bash
# From the host
curl -s http://localhost:61444/status | python3 -m json.tool

# View logs
docker logs --tail 50 caa-backup

# Check database stats
docker exec caa-backup uv run python manage.py status
```

## Restart the container

```bash
docker stop caa-backup && docker rm caa-backup
source /root/docker-server-configs/scripts/services.sh
start_caa_backup 'v-2026-05-14.7'
```

## Recover from database loss

If the SQLite database is lost, the container auto-recovers on startup:
1. Full import from PostgreSQL (~7M records)
2. Scans all files on disk to rebuild download status
3. Resumes normal operation

This takes 30-60 minutes depending on PostgreSQL and disk speed.
