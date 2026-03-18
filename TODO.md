# TODO

## Next up

- Add PostgreSQL benchmark coverage so the new staging-table and `COPY` fast path is measured directly instead of only the SQLite fallback path.
- Reduce SQLite fallback overhead by reusing commands/parameters and cutting down per-batch allocations in the raw SQL execution path.
- Optimize synchronize/delete-missing, especially for large snapshots, where the current SQLite path is far slower than tracked EF in benchmarks.
- Add PostgreSQL coverage for renamed columns, schema-qualified tables, and composite-key synchronize/delete behavior.
- Decide on the long-term API for empty scoped snapshots, likely an explicit scoped delete/synchronize contract instead of inferring from an empty payload.
- Add support and tests for EF value converters and other non-trivial property mappings.
- Define and document change-tracker semantics when the same rows are already tracked in the calling `DbContext`.
- Tighten provider-aware batching further with provider-specific regression tests and better sizing around mixed delete/scope workloads.
- Expand transaction coverage further, especially around ambient transactions and provider-specific failure cases.

## After correctness is stable

- Rerun and expand benchmarks for insert, merge, synchronize, and delete-by-key now that the PostgreSQL staging-table and `COPY` fast path exists.
- Optimize the PostgreSQL fast path further with lower-allocation row writing, temp-table reuse opportunities, and targeted SQL tuning for synchronize/delete workloads.
