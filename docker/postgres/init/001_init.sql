-- Base schema and replication setup for local CDC testing.
-- Requires Postgres to be started with wal_level=logical (set in docker-compose).
DROP TABLE IF EXISTS public.orders;
DROP TABLE IF EXISTS public.accounts;

-- Create dedicated roles for replication and app access.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_replication') THEN
        CREATE ROLE cdc_replication WITH LOGIN REPLICATION PASSWORD 'cdc_replication';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_app') THEN
        CREATE ROLE cdc_app WITH LOGIN PASSWORD 'cdc_app';
    END IF;
END$$;

-- Base tables in the default 'postgres' database.
CREATE TABLE IF NOT EXISTS public.accounts (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
    id          BIGSERIAL PRIMARY KEY,
    account_id  BIGINT      NOT NULL REFERENCES public.accounts(id),
    total_cents INTEGER     NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.accounts (email, status)
VALUES
    ('alice@example.com', 'active'),
    ('bob@example.com', 'active')
ON CONFLICT DO NOTHING;

INSERT INTO public.orders (account_id, total_cents, status)
VALUES
    (1, 1999, 'pending'),
    (2, 2599, 'pending')
ON CONFLICT DO NOTHING;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO cdc_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO cdc_app;

-- Publication for logical replication; matches default config.Publications.
DROP PUBLICATION IF EXISTS better_cdc_pub;
CREATE PUBLICATION better_cdc_pub FOR TABLE public.accounts, public.orders;

-- Logical replication slot used by the Go CDC reader.
-- Using wal2json plugin by default (avoids pglogrepl TupleData.Decode bug).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'better_cdc_slot') THEN
        PERFORM pg_create_logical_replication_slot('better_cdc_slot', 'wal2json');
    END IF;
END$$;
