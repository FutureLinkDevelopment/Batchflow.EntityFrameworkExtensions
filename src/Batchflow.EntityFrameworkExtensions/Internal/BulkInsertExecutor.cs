using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkInsertExecutor
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

        if (BulkMergeExecutor.IsPostgreSqlProvider(dbContext) && PostgreSqlBulkExecutor.ShouldUseStagingFastPath(entities.Count))
        {
            await PostgreSqlBulkExecutor.ExecuteInsertAsync(dbContext, entities, table, cancellationToken);
            return;
        }

        await BulkMergeExecutor.ExecuteBatchedAsync(
            dbContext,
            entities,
            options.BatchSize,
            batch => BulkMergeCommandFactory.BuildInsertCommand(table, batch),
            table.InsertColumns.Count,
            0,
            cancellationToken);
    }
}
