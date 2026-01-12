# 007 - Derive subject database name from `DATABASE_URL`

## Problem / Why
Subjects are `cdc.{database}.{schema}.{table}`. The `database` component defaults to `postgres` and only overrides via `AWS_RDS_DATABASE`, which can lead to silently wrong subjects if `DATABASE_URL` points to a different DB name.

## Goal
Ensure subject database name is correct by default.

## Scope
- Parse `DATABASE_URL` to extract the DB name when `Config.Database` is not explicitly set (or when `AWS_RDS_DATABASE` is absent).
- Keep an explicit override option (env var) for multi-db deployments.
- Add unit tests for URL parsing edge cases.

## Definition of done
- With `DATABASE_URL=.../mydb`, subjects are `cdc.mydb...` by default.
- Explicit override (env var) still works and is documented.
- Unit tests cover typical Postgres URLs and missing-path cases.


