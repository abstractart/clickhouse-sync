#!/bin/sh
# Loads sample data into the (already created) demo schema. The rows land on the
# object_storage disk via the 'tiered' storage policy. HTTPS only.
set -eu

CH="${CH:-https://clickhouse-s1r1:8443/}"
CURL="curl -sS -k --fail-with-body -H X-ClickHouse-User:default -H X-ClickHouse-Key:secret"

run() {
    echo "-> $1"
    $CURL "$CH" --data-binary "$1"
}

# Two partitions (2024-01 and 2024-02), spread across both shards.
run "INSERT INTO demo.events
SELECT toDate('2024-01-01') + (number % 60) AS event_date, number AS id, concat('msg-', toString(number)) AS message
FROM numbers(2000)
SETTINGS insert_distributed_sync = 1"

echo
echo "Part placement per node/disk after load:"
run "SELECT hostName() AS host, partition, disk_name, count() AS parts, sum(rows) AS rows
FROM clusterAllReplicas('cluster_2s_2r', system.parts)
WHERE database = 'demo' AND table = 'events_local' AND active
GROUP BY host, partition, disk_name
ORDER BY host, partition
FORMAT PrettyCompact"

echo
echo "Data loaded."
