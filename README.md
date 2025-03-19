# SQL Database using Omnipaxos (Sharding)

This is an example repo showcasing the use of the [Omnipaxos](https://omnipaxos.com) consensus library to create a
distributed SQL database. The source can be used to build server and client binaries which communicate over
TCP.

# Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [Docker](https://www.docker.com/)
- [Postgresql](https://www.postgresql.org/)

# How to run

The `build_scripts` directory contains various utilities for configuring and running AutoQuorum clients and servers.
Also contains examples of TOML file configuration.

- `run-local-client.sh` runs two clients in separate local processes. Configuration such as which server to connect to
  defined in TOML files.
- `run-local-cluster.sh` runs a 3 unit cluster in separate local processes. Make sure to have a `Postgresql` server
  running on `localhost:5432` with user role `postgres` .
- `docker-compose.yml` docker compose for a 3 unit cluster.

# Testing

You can run `cargo run --bin tester` to execute the test cases for the server implementation with 3 units and 2 shards.
