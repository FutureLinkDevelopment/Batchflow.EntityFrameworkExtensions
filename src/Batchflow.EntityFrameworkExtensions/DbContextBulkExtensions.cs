using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions;

/// <summary>
/// Entry point for BatchFlow bulk operations on top of <see cref="DbContext"/>.
/// </summary>
public static class DbContextBulkExtensions
{
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
        return ExecutePlaceholderAsync(dbContext, entities, BulkOperationType.Merge, configure, cancellationToken);
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
        return ExecutePlaceholderAsync(dbContext, entities, BulkOperationType.Synchronize, configure, cancellationToken);
    }

    private static Task ExecutePlaceholderAsync<TEntity>(
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

        throw new NotImplementedException(
            $"{operationType} is not implemented yet. The repository currently contains the initial public API and configuration primitives only.");
    }
}
