# Omnipaxos-kv

This is an example repo showcasing the use of the [Omnipaxos](https://omnipaxos.com) consensus library to create a
simple distributed key-value store. The source can be used to build server and client binaries which communicate over
TCP. The repo also contains a benchmarking example which delploys Omnipaxos servers and clients
onto [GCP](https://cloud.google.com) instances and runs an experiment collecting client response latencies (see
`benchmarks/README.md`).

# Prerequisites

- [Rust](https://www.rust-lang.org/tools/install)
- [Docker](https://www.docker.com/)
- [Postgresql](https://www.postgresql.org/)

# How to run

The `build_scripts` directory contains various utilities for configuring and running AutoQuorum clients and servers.
Also contains examples of TOML file configuration.

- `run-local-client.sh` runs two clients in separate local processes. Configuration such as which server to connect to
  defined in TOML files.
- `run-local-cluster.sh` runs a 3 server cluster in separate local processes. Make sure to have a `Postgresql` server
  running on `localhost:5432` with user role `postgres` .
- `docker-compose.yml` docker compose for a 3 server cluster.
- See `benchmarks/README.md` for benchmarking scripts 
