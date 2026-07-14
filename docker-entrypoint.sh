#!/bin/sh
# Generate gtsdb.ini from environment variables

CONFIG="/etc/gtsdb/gtsdb.ini"
mkdir -p /etc/gtsdb

cat > "$CONFIG" << EOF
[paths]
data = ${GTSDB_DATA_DIR:-/data}

[listens]
http = ${GTSDB_HTTP_ADDR:-0.0.0.0:5556}
tcp = ${GTSDB_TCP_ADDR:-0.0.0.0:5555}

[buffer]
file_handle_lru_capacity = 700
compaction_compression = ${GTSDB_COMPRESSION:-true}
sync_mode = ${GTSDB_SYNC_MODE:-async}
sync_interval_ms = ${GTSDB_SYNC_INTERVAL_MS:-1000}
cache_size = ${GTSDB_CACHE_SIZE:-10000}

[auth]
EOF

if [ -n "$GTSDB_NO_AUTH_USER" ]; then
  echo "no_auth_user = $GTSDB_NO_AUTH_USER" >> "$CONFIG"
fi
if [ -n "$GTSDB_ROOT_TOKEN" ]; then
  echo "root_token = $GTSDB_ROOT_TOKEN" >> "$CONFIG"
fi

exec /usr/local/bin/gtsdb "$CONFIG"
