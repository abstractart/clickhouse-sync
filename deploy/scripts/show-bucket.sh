#!/bin/sh
# Lists the objects stored in the SeaweedFS S3 bucket backing the
# object_storage disk. After a partition is moved off object_storage, ClickHouse
# deletes its parts here, so the bucket shrinks (and empties once everything is
# moved away).
set -eu

S3="${S3:-http://seaweedfs:8333}"
BUCKET="${BUCKET:-clickhouse}"
# SeaweedFS returns an empty listing for an oversized max-keys, so page with a
# sane size and follow the continuation token.
PAGE=1000

keys=""
total=0
token=""
while : ; do
    url="$S3/$BUCKET?list-type=2&max-keys=$PAGE"
    [ -n "$token" ] && url="$url&continuation-token=$token"
    resp=$(curl -sS "$url")

    page_keys=$(printf '%s' "$resp" | grep -oE '<Key>[^<]+</Key>' | sed 's/<[^>]*>//g' || true)
    if [ -n "$page_keys" ]; then
        keys="${keys:+$keys
}$page_keys"
    fi

    page_bytes=$(printf '%s' "$resp" | grep -oE '<Size>[0-9]+</Size>' | sed 's/[^0-9]//g' | awk '{s+=$1} END{print s+0}')
    total=$((total + page_bytes))

    truncated=$(printf '%s' "$resp" | grep -oE '<IsTruncated>[^<]+</IsTruncated>' | sed 's/<[^>]*>//g' || true)
    [ "$truncated" = "true" ] || break
    token=$(printf '%s' "$resp" | grep -oE '<NextContinuationToken>[^<]+</NextContinuationToken>' | sed 's/<[^>]*>//g' || true)
    [ -n "$token" ] || break
done

if [ -z "$keys" ]; then
    count=0
else
    count=$(printf '%s\n' "$keys" | grep -c .)
fi

echo "Bucket s3://$BUCKET : $count object(s), $total byte(s) total"
if [ "$count" -gt 0 ]; then
    echo "---"
    printf '%s\n' "$keys"
fi
