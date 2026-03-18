# TODO

## Next up

- Add provider-aware batching limits so large payloads automatically respect PostgreSQL and SQLite parameter ceilings.
- Add tests for duplicate keys across batch boundaries, null key values, and rollback behavior when one batch fails.
- Add PostgreSQL coverage for renamed columns, schema-qualified tables, and composite-key synchronize/delete behavior.
- Decide on the long-term API for empty scoped snapshots, likely an explicit scoped delete/synchronize contract instead of inferring from an empty payload.
- Add support and tests for EF value converters and other non-trivial property mappings.
- Define and document change-tracker semantics when the same rows are already tracked in the calling `DbContext`.

## After correctness is stable

- Replace the current large `INSERT ... VALUES` strategy for PostgreSQL with a staging-table and `COPY` based path.
- Rerun and expand benchmarks for insert, merge, synchronize, and delete-by-key once the PostgreSQL fast path exists.
- Add CI-friendly PostgreSQL integration test configuration so the provider-specific suite can run outside local development.
