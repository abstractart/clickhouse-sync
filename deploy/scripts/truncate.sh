#!/bin/sh
# Fully empties demo.events_local on every node, so the mover's stages can be
# tested from a clean slate. Re-run `make init` afterwards to reload demo data.
set -eu

CH="${CH:-https://clickhouse-s1r1:8443/}"
CURL="curl -sS -k --fail-with-body -H X-ClickHouse-User:default -H X-ClickHouse-Key:secret"

echo "-> TRUNCATE TABLE demo.events_local ON CLUSTER cluster_2s_2r"
$CURL "$CH" --data-binary "TRUNCATE TABLE demo.events_local ON CLUSTER cluster_2s_2r"

echo
# A plain count() (no GROUP BY) always returns one row, so it reports 0 for an
# empty table instead of yielding an empty result set.
rows=$($CURL "$CH" --data-binary "SELECT count() FROM clusterAllReplicas('cluster_2s_2r', demo.events_local)")
echo "Rows in demo.events_local across the cluster: $rows"
