# Batchflow.EntityFrameworkExtensions

`Batchflow.EntityFrameworkExtensions` is a standalone library for high-throughput Entity Framework bulk operations focused on BatchFlow's import and synchronization workloads.

## Initial goals

- Bulk merge by configurable business keys
- Full snapshot synchronization with delete-missing behavior
- Bulk insert and delete-by-key helpers for import workflows
- PostgreSQL-first implementation
- Low-memory execution with minimal change tracking overhead
- API design that can later be published as a private or public package

## Planned API direction

```csharp
await dbContext.BulkInsertAsync(entities, cancellationToken: cancellationToken);
```

```csharp
await dbContext.BulkMergeAsync(
    entities,
    options =>
    {
        options.KeyProperties.Add("ImportKey");
    },
    cancellationToken);
```

```csharp
await dbContext.BulkSynchronizeAsync(
    entities,
    options =>
    {
        options.KeyProperties.Add("Section");
        options.KeyProperties.Add("ImportKey");
        options.ScopeProperties.Add("Section");
    },
    cancellationToken);
```

```csharp
await dbContext.BulkDeleteByKeyAsync(
    entities,
    options =>
    {
        options.KeyProperties.Add("ImportKey");
    },
    cancellationToken);
```

## Status

The library currently includes working `BulkInsertAsync`, `BulkMergeAsync`, `BulkSynchronizeAsync`, and `BulkDeleteByKeyAsync` support, plus scoped delete-missing synchronization and SQLite-backed tests/benchmarks. The next iteration is focused on broadening provider coverage and improving performance for larger batches.
