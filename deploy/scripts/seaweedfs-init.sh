#!/bin/sh
# Creates the S3 bucket that the ClickHouse object_storage disk writes into.
set -eu

MASTER="${MASTER:-seaweedfs:9333}"
BUCKET="${BUCKET:-clickhouse}"

echo "waiting for SeaweedFS filer via master $MASTER ..."
until echo 's3.bucket.list' | weed shell -master "$MASTER" >/dev/null 2>&1; do
    sleep 2
done

echo "creating bucket '$BUCKET'"
echo "s3.bucket.create -name $BUCKET" | weed shell -master "$MASTER"
echo "buckets:"
echo "s3.bucket.list" | weed shell -master "$MASTER"
