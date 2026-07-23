#!/bin/sh
# Creates the sharded + replicated demo schema (no data). Talks to ClickHouse
# over HTTPS only. No IF NOT EXISTS: re-applying onto an existing schema fails
# loudly, so a botched/duplicated setup is obvious.
set -eu

CH="${CH:-https://clickhouse-s1r1:8443/}"
CURL="curl -sS -k --fail-with-body -H X-ClickHouse-User:default -H X-ClickHouse-Key:secret"

echo "waiting for ClickHouse at $CH ..."
until $CURL "$CH" --data-binary 'SELECT 1' >/dev/null 2>&1; do
    sleep 2
done

run() {
    echo "-> $1"
    $CURL "$CH" --data-binary "$1"
}

run "CREATE DATABASE demo ON CLUSTER cluster_2s_2r"

run "CREATE TABLE demo.events_local ON CLUSTER cluster_2s_2r
(
    event_date Date,
    id         UInt64,
    message    String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events_local', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY id
-- old_parts_lifetime is intentionally short (30s instead of the 480s default)
-- so that outdated parts (and their object_storage blobs) are cleaned up
-- quickly after TRUNCATE / MOVE while testing.
SETTINGS storage_policy = 'tiered', old_parts_lifetime = 30"

run "CREATE TABLE demo.events ON CLUSTER cluster_2s_2r
AS demo.events_local
ENGINE = Distributed('cluster_2s_2r', demo, events_local, rand())"

echo
echo "Schema created."
