using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkDeleteByKeyExecutor
{
    public static async Task ExecuteAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        BulkOperationOptions options,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        if (entities.Count == 0)
        {
            return;
        }

        BulkMergeExecutor.EnsureSupportedProvider(dbContext);
        var table = BulkMergeExecutor.CreateTableModel<TEntity>(dbContext, options);

        await BulkMergeExecutor.ExecuteBatchedAsync(
            dbContext,
            entities,
            options.BatchSize,
            batch => BulkDeleteCommandFactory.BuildDeleteByKeyCommand(table, batch),
            table.KeyColumns.Count,
            0,
            cancellationToken);
    }
}
