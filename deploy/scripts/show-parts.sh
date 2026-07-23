#!/bin/sh
# Prints active-part placement per node and disk for the demo table.
set -eu

CH="${CH:-https://clickhouse-s1r1:8443/}"
CURL="curl -sS -k --fail-with-body -H X-ClickHouse-User:default -H X-ClickHouse-Key:secret"

# Cluster-wide total. Aggregates with no GROUP BY always return exactly one row,
# so this prints an explicit 0 when the table is empty (instead of nothing).
echo "Totals across the cluster:"
$CURL "$CH" --data-binary "
SELECT count() AS parts, sum(rows) AS rows
FROM clusterAllReplicas('cluster_2s_2r', system.parts)
WHERE database = 'demo' AND table = 'events_local' AND active
FORMAT PrettyCompact"

echo
echo "Part placement per node and disk:"
$CURL "$CH" --data-binary "
SELECT hostName() AS host, partition, disk_name, count() AS parts, sum(rows) AS rows
FROM clusterAllReplicas('cluster_2s_2r', system.parts)
WHERE database = 'demo' AND table = 'events_local' AND active
GROUP BY host, partition, disk_name
ORDER BY host, partition
FORMAT PrettyCompact"
