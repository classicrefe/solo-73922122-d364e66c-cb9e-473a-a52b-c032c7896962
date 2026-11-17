#!/usr/bin/env bash
# Allow physical replication connections (pg_basebackup) from the api container.
# The stock image only appends a `host all all all` rule, which does NOT match
# replication connections, so pg_basebackup would be rejected without this.
set -euo pipefail
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
