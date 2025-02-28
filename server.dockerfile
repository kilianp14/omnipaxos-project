FROM rust:1.84 AS chef

# Stop if a command fails
RUN set -eux

# Only fetch crates.io index for used crates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# cargo-chef will be cached from the second build onwards
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release --bin server

FROM debian:bookworm-slim AS runtime
    
# Install dependencies
RUN apt-get update && apt-get install -y \
    postgresql \
    postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

# Set up PostgreSQL
RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql
# Configure PostgreSQL to not require passwords
RUN echo "local all all trust" > /etc/postgresql/15/main/pg_hba.conf && \
    echo "host all all 127.0.0.1/32 trust" >> /etc/postgresql/15/main/pg_hba.conf && \
    echo "host all all ::1/128 trust" >> /etc/postgresql/15/main/pg_hba.conf

WORKDIR /app
COPY --from=builder /app/target/release/server /usr/local/bin
EXPOSE 5432 8000

# Did this as I coudn't copy a file `entrypoint.sh` into the `usr/local/bin` directory.
COPY <<EOF /usr/local/bin/entrypoint.sh
#!/bin/bash

# Start PostgreSQL in the background
service postgresql start

# Wait for PostgreSQL to fully start
until pg_isready -U postgres; do
    echo "Waiting for PostgreSQL to start..."
    sleep 2
done

# Run the Rust server
exec /usr/local/bin/server
EOF

RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
