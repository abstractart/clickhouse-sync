#!/bin/sh
# Generates a self-signed certificate shared by all ClickHouse nodes for HTTPS.
# Runs once as an init container; the mover connects with -insecure.
set -eu

CERT_DIR="${CERT_DIR:-/certs}"

if [ -f "$CERT_DIR/server.crt" ] && [ -f "$CERT_DIR/server.key" ]; then
    echo "certs already present in $CERT_DIR, skipping"
    exit 0
fi

echo "generating self-signed certificate in $CERT_DIR"
openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.crt" \
    -days 3650 \
    -subj "/CN=clickhouse" \
    -addext "subjectAltName=DNS:clickhouse-s1r1,DNS:clickhouse-s1r2,DNS:clickhouse-s2r1,DNS:clickhouse-s2r2,DNS:localhost"

# ClickHouse runs as uid 101; make the key readable by it.
chmod 0644 "$CERT_DIR/server.key" "$CERT_DIR/server.crt"
echo "done"
