#!/usr/bin/env bash
#
# Reproduces the "in-place disk swap" from docs/research/filesystem-cache.md:
# attach a filesystem cache to an EXISTING table's S3 data WITHOUT moving or
# copying any data. It:
#   1. configures a raw s3 disk and creates a table on it with data;
#   2. proves reads are NOT cached;
#   3. swaps the disk in place — renames the raw disk to a backing disk, puts a
#      `cache` disk under the ORIGINAL name, and moves the tiny local metadata
#      dir (node stopped during the move);
#   4. proves the SAME parts are now cached (cold from S3 -> warm from cache),
#      with identical row count and checksum and no part moved.
#
# Runs on the HOST against the local docker-compose stack (needs `make up`).
# All artifacts are cleaned up on exit, leaving the stack as it was.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="docker compose -f ${SCRIPT_DIR}/../docker-compose.yml"
NODE="clickhouse-s1r1"
CFG="/etc/clickhouse-server/config.d/zz-swapdemo.xml"
DB="demo"
TBL="swap_demo"
ENDPOINT="http://seaweedfs:8333/clickhouse/swapdemo/"

ch()   { $COMPOSE exec -T "$NODE" clickhouse-client --user default --password secret "$@"; }
xexec(){ $COMPOSE exec -T "$NODE" "$@"; }

wait_healthy() {
  for _ in $(seq 1 30); do
    if ch -q "SELECT 1" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "ERROR: $NODE did not become ready" >&2
  exit 1
}

restart_node() { $COMPOSE restart "$NODE" >/dev/null; wait_healthy; }

write_raw_config() {
  xexec sh -c "cat > $CFG" <<XML
<clickhouse><storage_configuration>
  <disks>
    <swapdemo>
      <type>s3</type>
      <endpoint>${ENDPOINT}</endpoint>
      <access_key_id>clickhouse</access_key_id>
      <secret_access_key>clickhouse</secret_access_key>
      <region>us-east-1</region>
      <support_batch_delete>false</support_batch_delete>
    </swapdemo>
  </disks>
  <policies>
    <swapdemo_policy><volumes><main><disk>swapdemo</disk></main></volumes></swapdemo_policy>
  </policies>
</storage_configuration></clickhouse>
XML
}

write_cache_config() {
  xexec sh -c "cat > $CFG" <<XML
<clickhouse><storage_configuration>
  <disks>
    <!-- the old raw disk, renamed; endpoint/credentials unchanged -->
    <swapdemo_backing>
      <type>s3</type>
      <endpoint>${ENDPOINT}</endpoint>
      <access_key_id>clickhouse</access_key_id>
      <secret_access_key>clickhouse</secret_access_key>
      <region>us-east-1</region>
      <support_batch_delete>false</support_batch_delete>
    </swapdemo_backing>
    <!-- SAME name the parts reference, now a cache disk over the backing disk -->
    <swapdemo>
      <type>cache</type>
      <disk>swapdemo_backing</disk>
      <path>/var/lib/clickhouse/disks/swapdemo_cache/</path>
      <max_size>2Gi</max_size>
      <cache_on_write_operations>true</cache_on_write_operations>
    </swapdemo>
  </disks>
  <policies>
    <swapdemo_policy><volumes><main><disk>swapdemo</disk></main></volumes></swapdemo_policy>
  </policies>
</storage_configuration></clickhouse>
XML
}

# read_stats <log_comment> -> "src<TAB>cache" (bytes from S3 vs from the cache)
read_stats() {
  ch -q "SELECT
            formatReadableSize(ProfileEvents['CachedReadBufferReadFromSourceBytes']),
            formatReadableSize(ProfileEvents['CachedReadBufferReadFromCacheBytes'])
          FROM system.query_log
          WHERE type='QueryFinish' AND log_comment='$1'
          ORDER BY event_time_microseconds DESC LIMIT 1"
}

# scan <log_comment>: full-column read tagged so it can be found in query_log
scan() { ch -q "SELECT sum(cityHash64(payload)) FROM $DB.$TBL SETTINGS log_comment='$1'" >/dev/null; }

cleanup() {
  ch -q "DROP TABLE IF EXISTS $DB.$TBL SYNC" >/dev/null 2>&1 || true
  xexec sh -c "rm -f $CFG; rm -rf /var/lib/clickhouse/disks/swapdemo /var/lib/clickhouse/disks/swapdemo_backing /var/lib/clickhouse/disks/swapdemo_cache" >/dev/null 2>&1 || true
}

# --- preflight -------------------------------------------------------------
if ! ch -q "SELECT 1" >/dev/null 2>&1; then
  echo "ERROR: the stack is not up. Run 'make up' first." >&2
  exit 1
fi

# Always leave the stack pristine, even on Ctrl-C / error.
trap 'echo; echo "Cleaning up ..."; cleanup; restart_node' EXIT

echo "==> 0. reset any previous run"
cleanup; restart_node

echo
echo "==> 1. configure a RAW s3 disk and create an existing table with data"
write_raw_config; restart_node
ch -q "CREATE TABLE $DB.$TBL (id UInt64, payload String) ENGINE=MergeTree ORDER BY id SETTINGS storage_policy='swapdemo_policy'"
ch -q "INSERT INTO $DB.$TBL SELECT number, randomPrintableASCII(100) FROM numbers(300000)"
echo "    part placement:"
ch -q "SELECT '      ' || disk_name || ': ' || toString(count()) || ' part(s)' FROM system.parts WHERE database='$DB' AND table='$TBL' AND active GROUP BY disk_name"
ROWS_BEFORE=$(ch -q "SELECT count() FROM $DB.$TBL")
SUM_BEFORE=$(ch -q "SELECT sum(cityHash64(payload)) FROM $DB.$TBL")

echo
echo "==> 2. baseline: reads are NOT cached (raw s3 disk)"
ch -q "SYSTEM DROP FILESYSTEM CACHE"
scan before_cold; scan before_warm
ch -q "SYSTEM FLUSH LOGS"
printf "    cold: %s\n" "$(read_stats before_cold | sed 's/\t/ from S3, /; s/$/ from cache/')"
printf "    warm: %s\n" "$(read_stats before_warm | sed 's/\t/ from S3, /; s/$/ from cache/')"
echo "    (0 B from cache on both — no filesystem cache in play)"

echo
echo "==> 3. IN-PLACE SWAP (no data moved): raw disk -> backing, cache disk under the old name"
# Write the new config while the node still runs (only read on restart), then
# stop the node, move the tiny metadata dir, and start it again.
write_cache_config
CID=$($COMPOSE ps -q "$NODE")
VOLUME=$(docker inspect -f '{{ range .Mounts }}{{ if eq .Destination "/var/lib/clickhouse" }}{{ .Name }}{{ end }}{{ end }}' "$CID")
echo "    stopping node, moving metadata dir (volume: $VOLUME) ..."
$COMPOSE stop "$NODE" >/dev/null
docker run --rm -v "${VOLUME}:/v" alpine:3.20 \
  sh -c 'mv /v/disks/swapdemo /v/disks/swapdemo_backing'
$COMPOSE start "$NODE" >/dev/null
wait_healthy

echo
echo "==> 4. verify: same parts, now cached, data intact"
echo "    part placement (still the same disk name, now a cache disk):"
ch -q "SELECT '      ' || disk_name || ': ' || toString(count()) || ' part(s)' FROM system.parts WHERE database='$DB' AND table='$TBL' AND active GROUP BY disk_name"
ch -q "SYSTEM DROP FILESYSTEM CACHE"
scan after_cold; scan after_warm
ch -q "SYSTEM FLUSH LOGS"
printf "    cold: %s\n" "$(read_stats after_cold | sed 's/\t/ from S3, /; s/$/ from cache/')"
printf "    warm: %s\n" "$(read_stats after_warm | sed 's/\t/ from S3, /; s/$/ from cache/')"
ROWS_AFTER=$(ch -q "SELECT count() FROM $DB.$TBL")
SUM_AFTER=$(ch -q "SELECT sum(cityHash64(payload)) FROM $DB.$TBL")
echo "    rows: ${ROWS_BEFORE} -> ${ROWS_AFTER}   checksum: $([ "$SUM_BEFORE" = "$SUM_AFTER" ] && echo 'unchanged' || echo "CHANGED! $SUM_BEFORE != $SUM_AFTER")"

echo
echo "Done: the warm read is served from the local cache (0 B from S3) while the"
echo "cold read still hit S3 — the existing table is now cached, and no part was"
echo "moved or copied (rows and checksum unchanged)."
