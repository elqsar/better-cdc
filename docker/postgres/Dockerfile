# Pin the minor version: 17.11 added output_plugin_libraries, which older minors
# reject as an unknown setting, and a floating tag let CI and local caches diverge.
FROM docker.io/library/postgres:17.11

# Install wal2json plugin for logical replication
RUN apt-get update && apt-get install -y \
    postgresql-17-wal2json \
    && rm -rf /var/lib/apt/lists/*

# Since 17.11 only allowlisted libraries may back logical replication slots.
# initdb copies this sample into the data directory's postgresql.conf.
RUN echo "output_plugin_libraries = 'pgoutput, test_decoding, wal2json'" \
    >> /usr/share/postgresql/postgresql.conf.sample
