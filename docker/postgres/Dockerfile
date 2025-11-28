FROM postgres:17

# Install wal2json plugin for logical replication
RUN apt-get update && apt-get install -y \
    postgresql-17-wal2json \
    && rm -rf /var/lib/apt/lists/*
