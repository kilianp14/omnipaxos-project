#!/bin/bash

# Connect to PostgreSQL and list all databases starting with 'omnipaxos'
databases=$(psql -U postgres -t -c "SELECT datname FROM pg_database WHERE datname LIKE 'omnipaxos%'")

# Loop through each database and drop it
for db in $databases; do
    db=$(echo "$db" | tr -d '[:space:]')

    if [[ -n "$db" ]]; then
        echo "Checking for prepared transactions in: $db"

        # Get all prepared transactions specific to this database
        transactions=$(psql -U postgres -d "$db" -t -c "SELECT gid FROM pg_prepared_xacts")

        # Loop through each transaction and abort it
        for tx in $transactions; do
            tx=$(echo "$tx" | tr -d '[:space:]')
            if [[ -n "$tx" ]]; then
                echo "Aborting transaction '$tx' in database '$db'"
                psql -U postgres -d "$db" -c "ROLLBACK PREPARED '$tx'"
            fi
        done

        # Now it's safe to drop the database
        echo "Dropping database: $db"
        psql -U postgres -c "DROP DATABASE \"$db\""
    fi
done

echo "All databases starting with 'omnipaxos' have been deleted."