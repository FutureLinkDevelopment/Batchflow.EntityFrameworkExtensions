using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkSynchronizeExecutor
{
    public static async Task ExecuteAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        BulkOperationOptions options,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        BulkMergeExecutor.EnsureSupportedProvider(dbContext);

        var table = BulkMergeExecutor.CreateTableModel<TEntity>(dbContext, options);
        BulkModelValidator.ValidateSynchronizeScope(table, entities, options);
        BulkModelValidator.ValidateNoNullSourceKeys(table.KeyColumns, entities, nameof(BulkOperationType.Synchronize));
        BulkModelValidator.ValidateNoDuplicateSourceKeys(table.KeyColumns, entities, nameof(BulkOperationType.Synchronize));

        if (BulkMergeExecutor.IsPostgreSqlProvider(dbContext) &&
            (entities.Count == 0 || PostgreSqlBulkExecutor.ShouldUseStagingFastPath(entities.Count)))
        {
            await PostgreSqlBulkExecutor.ExecuteSynchronizeAsync(dbContext, entities, table, options, cancellationToken);
            return;
        }

        var startedTransaction = false;
        var transaction = dbContext.Database.CurrentTransaction;

        if (transaction is null)
        {
            transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
            startedTransaction = true;
        }

        try
        {
            if (entities.Count > 0)
            {
                await BulkMergeExecutor.ExecuteBatchedAsync(
                    dbContext,
                    entities,
                    options.BatchSize,
                    batch => BulkMergeCommandFactory.BuildCommand(table, batch),
                    table.InsertColumns.Count,
                    0,
                    cancellationToken);
            }

            if (options.DeleteMissingRows)
            {
                await DeleteMissingRowsAsync(dbContext, table, entities, options.BatchSize, cancellationToken);
            }

            if (startedTransaction)
            {
                await transaction.CommitAsync(cancellationToken);
            }
        }
        catch
        {
            if (startedTransaction)
            {
                await transaction.RollbackAsync(cancellationToken);
            }

            throw;
        }
        finally
        {
            if (startedTransaction)
            {
                await transaction.DisposeAsync();
            }
        }
    }

    private static async Task DeleteMissingRowsAsync<TEntity>(
        DbContext dbContext,
        BulkTableModel table,
        IReadOnlyCollection<TEntity> entities,
        int? requestedBatchSize,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        if (table.ScopeColumns.Count == 0 && entities.Count == 0)
        {
            await dbContext.Database.ExecuteSqlRawAsync("DELETE FROM " + table.QualifiedTableName + ';', cancellationToken);
            return;
        }

        var deleteBatchSize = BulkBatchPlanner.DetermineBatchSize(
            dbContext,
            requestedBatchSize,
            table.KeyColumns.Count,
            0);

        if (table.ScopeColumns.Count == 0)
        {
            var sourceKeys = CreateDistinctKeys(table.KeyColumns, entities);
            var existingKeys = await LoadExistingKeysAsync(dbContext, table, scope: null, cancellationToken);
            var keysToDelete = existingKeys.Except(sourceKeys).ToArray();

            foreach (var command in BulkDeleteCommandFactory.BuildDeleteByKeyCommands(table, keysToDelete, deleteBatchSize))
            {
                await dbContext.Database.ExecuteSqlRawAsync(
                    command.Sql,
                    BulkMergeExecutor.CreateDbParameters(dbContext, command.Parameters),
                    cancellationToken);
            }

            return;
        }

        var entitiesByScope = entities.GroupBy(entity => new CompositeValueKey(table.ScopeColumns.Select(column => column.GetValue(entity)).ToArray()));
        foreach (var scopeGroup in entitiesByScope)
        {
            var sourceKeys = CreateDistinctKeys(table.KeyColumns, scopeGroup.ToArray());
            var existingKeys = await LoadExistingKeysAsync(dbContext, table, scopeGroup.Key, cancellationToken);
            var keysToDelete = existingKeys.Except(sourceKeys).ToArray();

            foreach (var command in BulkDeleteCommandFactory.BuildDeleteByKeyCommands(table, keysToDelete, deleteBatchSize))
            {
                await dbContext.Database.ExecuteSqlRawAsync(
                    command.Sql,
                    BulkMergeExecutor.CreateDbParameters(dbContext, command.Parameters),
                    cancellationToken);
            }
        }
    }

    private static HashSet<CompositeValueKey> CreateDistinctKeys<TEntity>(IReadOnlyList<BulkColumn> columns, IReadOnlyCollection<TEntity> entities)
        where TEntity : class
    {
        var keys = new HashSet<CompositeValueKey>();

        foreach (var entity in entities)
        {
            keys.Add(new CompositeValueKey(columns.Select(column => column.GetValue(entity)).ToArray()));
        }

        return keys;
    }

    private static async Task<HashSet<CompositeValueKey>> LoadExistingKeysAsync(
        DbContext dbContext,
        BulkTableModel table,
        CompositeValueKey? scope,
        CancellationToken cancellationToken)
    {
        var keys = new HashSet<CompositeValueKey>();
        var connection = dbContext.Database.GetDbConnection();

        if (connection.State != System.Data.ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await using var command = connection.CreateCommand();
        command.Transaction = dbContext.Database.CurrentTransaction?.GetDbTransaction();
        command.CommandText = BuildLoadExistingKeysSql(table, scope, command);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var values = new object?[table.KeyColumns.Count];
            for (var index = 0; index < table.KeyColumns.Count; index++)
            {
                values[index] = await reader.IsDBNullAsync(index, cancellationToken)
                    ? null
                    : reader.GetValue(index);
            }

            keys.Add(new CompositeValueKey(values));
        }

        return keys;
    }

    private static string BuildLoadExistingKeysSql(BulkTableModel table, CompositeValueKey? scope, System.Data.Common.DbCommand command)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append("SELECT ");
        sql.Append(string.Join(", ", table.KeyColumns.Select(column => column.SqlColumnName)));
        sql.Append(" FROM ");
        sql.Append(table.QualifiedTableName);

        if (scope is null)
        {
            return sql.ToString();
        }

        sql.Append(" WHERE ");
        sql.Append(string.Join(
            " AND ",
            table.ScopeColumns.Select((column, index) =>
            {
                var parameterName = $"@p{index}";
                var parameter = command.CreateParameter();
                parameter.ParameterName = parameterName;
                parameter.Value = scope.Values[index] ?? DBNull.Value;
                command.Parameters.Add(parameter);
                return $"(({table.QualifiedTableName}.{column.SqlColumnName} = {parameterName}) OR ({table.QualifiedTableName}.{column.SqlColumnName} IS NULL AND {parameterName} IS NULL))";
            })));

        return sql.ToString();
    }
}
