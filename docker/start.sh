#!/bin/bash

cd /code/caa-backup
exec uv run --no-dev python3 caa_downloader.py
