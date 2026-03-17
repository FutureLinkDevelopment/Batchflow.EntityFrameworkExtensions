# Batchflow.EntityFrameworkExtensions

`Batchflow.EntityFrameworkExtensions` is a standalone library for high-throughput Entity Framework bulk operations focused on BatchFlow's import and synchronization workloads.

## Initial goals

- Bulk merge by configurable business keys
- Full snapshot synchronization with delete-missing behavior
- PostgreSQL-first implementation
- Low-memory execution with minimal change tracking overhead
- API design that can later be published as a private or public package

## Planned API direction

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

## Status

This repository is currently in bootstrap mode. The initial focus is project structure, configuration primitives, and test coverage that will support a PostgreSQL-first implementation.
