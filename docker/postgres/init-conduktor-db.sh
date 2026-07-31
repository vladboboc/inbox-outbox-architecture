#!/bin/bash
# Conduktor Console 1.46.x needs two databases: one for its own state (CDK_DATABASE_URL) and a
# separate one for the SQL-querying feature (CDK_KAFKASQL_DATABASE_URL). Keeping them apart means
# the core Console UI keeps working if the SQL database becomes unavailable.
# POSTGRES_DB already created conduktor-console, so only the SQL one is added here.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE "conduktor-sql" OWNER $POSTGRES_USER;
EOSQL

echo "created database: conduktor-sql"
