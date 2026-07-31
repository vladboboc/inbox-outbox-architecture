#!/bin/bash
# One Postgres instance, one database per service. Each service owns its own schema and
# migrations — no shared tables, no cross-service joins. That separation is the whole reason
# the outbox and inbox patterns are needed in the first place.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE orderdb    OWNER $POSTGRES_USER;
    CREATE DATABASE shippingdb OWNER $POSTGRES_USER;
EOSQL

echo "created databases: orderdb, shippingdb"
