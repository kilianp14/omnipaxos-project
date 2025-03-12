#!/bin/bash

# Connect to PostgreSQL and list all databases starting with 'omnipaxos'
databases=$(psql -U postgres -t -c "SELECT datname FROM pg_database WHERE datname LIKE 'omnipaxos%'")

# Loop through each database and drop it
for db in $databases; do
    echo "Dropping database: $db"
    psql -U postgres -c "DROP DATABASE $db"
done

echo "All databases starting with 'omnipaxos' have been deleted."