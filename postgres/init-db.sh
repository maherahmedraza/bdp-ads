#!/bin/bash
set -e

# Variables
POSTGRES_DB="job_ads_db"
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="airflow"

# Perform initialization tasks
function initialize_database() {
    # Check if the database already exists
    if psql -h localhost -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "" 2>/dev/null; then
        echo "Database '$POSTGRES_DB' already exists. Skipping initialization."
    else
        echo "Database '$POSTGRES_DB' does not exist. Initializing..."
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
          CREATE DATABASE "$POSTGRES_DB";
        EOSQL
        echo "Database '$POSTGRES_DB' created successfully"
    fi
}

# Run the initialization function
initialize_database

# Start the PostgreSQL server
#exec "$@"

#set -e
#
#psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" <<-EOSQL
#  CREATE DATABASE ${POSTGRES_DB};
#  GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_USER};
#EOSQL
#
#psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<-EOSQL
#   GRANT ALL ON SCHEMA public TO ${POSTGRES_USER};
#EOSQL