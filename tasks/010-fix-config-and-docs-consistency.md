# 010 - Fix config and docs consistency

## Problem / Why
There are still config and documentation mismatches that make the project harder to operate safely:
- subjects default to `cdc.postgres...` unless `AWS_RDS_DATABASE` is set, even when `DATABASE_URL` points to another database
- the local quickstart recommends `pgoutput`, but the Docker init SQL creates the default slot with `wal2json`
- the docs still contain production-readiness claims that depend on these config details being correct

## Goal
Make startup configuration and documentation internally consistent so operators get the expected subject names and local setup works as documented.

## Scope
- Derive the database name from `DATABASE_URL` when no explicit override is provided.
- Keep an explicit override option for deployments that need a custom subject prefix.
- Fix the local quickstart / compose / init SQL mismatch so the documented default plugin matches the provisioned slot.
- Update README config docs to describe the precedence rules clearly.
- Add unit tests for database name derivation and edge cases around missing or malformed URLs.

## Definition of done
- With `DATABASE_URL=.../mydb`, emitted subjects default to `cdc.mydb...`.
- An explicit override still works and is documented.
- The default local quickstart works without slot/plugin mismatch.
- README and Docker bootstrap steps describe the same default plugin and slot behavior.

