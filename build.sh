#!/bin/bash

#!/bin/bash

# Usage: ./build.sh [TAG]
# If TAG is not provided, use v-YYYY-MM-DD.0 as default

if [ -z "$1" ]; then
	TAG="v-$(date +%Y-%m-%d).0"
else
	TAG="$1"
fi

echo "Building Docker image with tag: $TAG"
docker build -t metabrainz/caa-backup:$TAG .
