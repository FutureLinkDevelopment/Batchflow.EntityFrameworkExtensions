using Batchflow.EntityFrameworkExtensions.Abstractions;
using Batchflow.EntityFrameworkExtensions.Internal;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions;

/// <summary>
/// Entry point for BatchFlow bulk operations on top of <see cref="DbContext"/>.
/// </summary>
public static class DbContextBulkExtensions
{
    /// <summary>
    /// Inserts new rows without attempting to match existing records.
    /// </summary>
    public static Task BulkInsertAsync<TEntity>(
        this DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        Action<BulkOperationOptions>? configure = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return ExecuteAsync(dbContext, entities, BulkOperationType.Insert, configure, cancellationToken);
    }

    /// <summary>
    /// Inserts new rows and updates matching rows by configured business key.
    /// </summary>
    public static Task BulkMergeAsync<TEntity>(
        this DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        Action<BulkOperationOptions>? configure = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return ExecuteAsync(dbContext, entities, BulkOperationType.Merge, configure, cancellationToken);
    }

    /// <summary>
    /// Inserts new rows, updates matching rows, and deletes rows missing from the source payload.
    /// </summary>
    public static Task BulkSynchronizeAsync<TEntity>(
        this DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        Action<BulkOperationOptions>? configure = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return ExecuteAsync(dbContext, entities, BulkOperationType.Synchronize, configure, cancellationToken);
    }

    /// <summary>
    /// Deletes rows matching the configured business key values from the source payload.
    /// </summary>
    public static Task BulkDeleteByKeyAsync<TEntity>(
        this DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        Action<BulkOperationOptions>? configure = null,
        CancellationToken cancellationToken = default)
        where TEntity : class
    {
        return ExecuteAsync(dbContext, entities, BulkOperationType.DeleteByKey, configure, cancellationToken);
    }

    private static Task ExecuteAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        BulkOperationType operationType,
        Action<BulkOperationOptions>? configure,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(dbContext);
        ArgumentNullException.ThrowIfNull(entities);

        cancellationToken.ThrowIfCancellationRequested();

        var options = new BulkOperationOptions
        {
            OperationType = operationType,
            DeleteMissingRows = operationType == BulkOperationType.Synchronize
        };

        configure?.Invoke(options);
        BulkOperationOptionsValidator.Validate(options);

        return operationType switch
        {
            BulkOperationType.Insert => BulkInsertExecutor.ExecuteAsync(dbContext, entities, options, cancellationToken),
            BulkOperationType.Merge => BulkMergeExecutor.ExecuteAsync(dbContext, entities, options, cancellationToken),
            BulkOperationType.Synchronize => BulkSynchronizeExecutor.ExecuteAsync(dbContext, entities, options, cancellationToken),
            BulkOperationType.DeleteByKey => BulkDeleteByKeyExecutor.ExecuteAsync(dbContext, entities, options, cancellationToken),
            _ => throw new NotImplementedException($"{operationType} is not implemented yet.")
        };
    }
}
