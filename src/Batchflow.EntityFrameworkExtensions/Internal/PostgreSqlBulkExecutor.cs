using System.Data;
using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class PostgreSqlBulkExecutor
{
    private const int MinimumInsertRowsForStagingFastPath = 250;
    private const int MinimumMergeRowsForStagingFastPath = 250;
    private const int MinimumSynchronizeRowsForStagingFastPath = 250;
    private const int MinimumDeleteRowsForStagingFastPath = 500;

    public static bool ShouldUseInsertStagingFastPath(int rowCount)
    {
        return rowCount >= MinimumInsertRowsForStagingFastPath;
    }

    public static bool ShouldUseMergeStagingFastPath(int rowCount)
    {
        return rowCount >= MinimumMergeRowsForStagingFastPath;
    }

    public static bool ShouldUseSynchronizeStagingFastPath(int rowCount)
    {
        return rowCount >= MinimumSynchronizeRowsForStagingFastPath;
    }

    public static bool ShouldUseDeleteStagingFastPath(int rowCount)
    {
        return rowCount >= MinimumDeleteRowsForStagingFastPath;
    }

    public static Task ExecuteInsertAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        BulkTableModel table,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        return ExecuteWithStagingTableAsync(
            dbContext,
            table,
            table.InsertColumns,
            entities,
            async (connection, transaction, tempTableName) =>
            {
                await CopyRowsAsync(connection, tempTableName, table.InsertColumns, entities, cancellationToken);
                await ExecuteNonQueryAsync(connection, transaction, BuildInsertSql(table, tempTableName), cancellationToken);
            },
            cancellationToken);
    }

    public static Task ExecuteMergeAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        BulkTableModel table,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        return ExecuteWithStagingTableAsync(
            dbContext,
            table,
            table.InsertColumns,
            entities,
            async (connection, transaction, tempTableName) =>
            {
                await CopyRowsAsync(connection, tempTableName, table.InsertColumns, entities, cancellationToken);
                await ExecuteNonQueryAsync(connection, transaction, BuildMergeSql(table, tempTableName), cancellationToken);
            },
            cancellationToken);
    }

    public static async Task ExecuteSynchronizeAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        BulkTableModel table,
        BulkOperationOptions options,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        if (entities.Count == 0 && table.ScopeColumns.Count == 0)
        {
            await ExecuteWithinTransactionAsync(
                dbContext,
                async (connection, transaction) =>
                {
                    await ExecuteNonQueryAsync(connection, transaction, "DELETE FROM " + table.QualifiedTableName + ';', cancellationToken);
                },
                cancellationToken);

            return;
        }

        await ExecuteWithStagingTableAsync(
            dbContext,
            table,
            table.InsertColumns,
            entities,
            async (connection, transaction, tempTableName) =>
            {
                await CopyRowsAsync(connection, tempTableName, table.InsertColumns, entities, cancellationToken);
                await ExecuteNonQueryAsync(connection, transaction, BuildMergeSql(table, tempTableName), cancellationToken);

                if (options.DeleteMissingRows)
                {
                    await ExecuteNonQueryAsync(connection, transaction, BuildSynchronizeDeleteSql(table, tempTableName), cancellationToken);
                }
            },
            cancellationToken);
    }

    public static Task ExecuteDeleteByKeyAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        BulkTableModel table,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        return ExecuteWithStagingTableAsync(
            dbContext,
            table,
            table.KeyColumns,
            entities,
            async (connection, transaction, tempTableName) =>
            {
                await CopyRowsAsync(connection, tempTableName, table.KeyColumns, entities, cancellationToken);
                await ExecuteNonQueryAsync(connection, transaction, BuildDeleteByKeySql(table, tempTableName), cancellationToken);
            },
            cancellationToken);
    }

    private static async Task ExecuteWithStagingTableAsync<TEntity>(
        DbContext dbContext,
        BulkTableModel table,
        IReadOnlyList<BulkColumn> stagedColumns,
        IReadOnlyCollection<TEntity> entities,
        Func<NpgsqlConnection, NpgsqlTransaction?, string, Task> action,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        await ExecuteWithinTransactionAsync(
            dbContext,
            async (connection, transaction) =>
            {
                var tempTableName = CreateTempTableName();

                try
                {
                    await ExecuteNonQueryAsync(connection, transaction, BuildCreateTempTableSql(table, tempTableName, stagedColumns), cancellationToken);
                    await action(connection, transaction, tempTableName);
                }
                finally
                {
                    await TryDropTempTableAsync(connection, transaction, tempTableName, cancellationToken);
                }
            },
            cancellationToken);
    }

    private static async Task ExecuteWithinTransactionAsync(
        DbContext dbContext,
        Func<NpgsqlConnection, NpgsqlTransaction?, Task> action,
        CancellationToken cancellationToken)
    {
        var startedTransaction = false;
        var transaction = dbContext.Database.CurrentTransaction;

        if (transaction is null)
        {
            transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
            startedTransaction = true;
        }

        var connection = (NpgsqlConnection)dbContext.Database.GetDbConnection();
        var openedConnection = false;

        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync(cancellationToken);
            openedConnection = true;
        }

        try
        {
            await action(connection, transaction.GetDbTransaction() as NpgsqlTransaction);

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

            if (openedConnection)
            {
                await connection.CloseAsync();
            }
        }
    }

    private static async Task CopyRowsAsync<TEntity>(
        NpgsqlConnection connection,
        string tempTableName,
        IReadOnlyList<BulkColumn> columns,
        IReadOnlyCollection<TEntity> entities,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        if (entities.Count == 0)
        {
            return;
        }

        var copyCommand = $"COPY {BulkTableModel.QuoteIdentifier(tempTableName)} ({string.Join(", ", columns.Select(column => column.SqlColumnName))}) FROM STDIN (FORMAT BINARY)";

        await using var importer = await connection.BeginBinaryImportAsync(copyCommand, cancellationToken);
        foreach (var entity in entities)
        {
            await importer.StartRowAsync(cancellationToken);

            foreach (var column in columns)
            {
                var value = column.GetWriteValue(entity!);
                await WriteValueAsync(importer, column, value, cancellationToken);
            }
        }

        await importer.CompleteAsync(cancellationToken);
    }

    private static Task WriteValueAsync(
        NpgsqlBinaryImporter importer,
        BulkColumn column,
        object? value,
        CancellationToken cancellationToken)
    {
        if (value is null)
        {
            return importer.WriteNullAsync(cancellationToken);
        }

        return value switch
        {
            string stringValue => importer.WriteAsync(stringValue, cancellationToken),
            Guid guidValue => importer.WriteAsync(guidValue, cancellationToken),
            bool boolValue => importer.WriteAsync(boolValue, cancellationToken),
            short shortValue => importer.WriteAsync(shortValue, cancellationToken),
            int intValue => importer.WriteAsync(intValue, cancellationToken),
            long longValue => importer.WriteAsync(longValue, cancellationToken),
            float floatValue => importer.WriteAsync(floatValue, cancellationToken),
            double doubleValue => importer.WriteAsync(doubleValue, cancellationToken),
            decimal decimalValue => importer.WriteAsync(decimalValue, cancellationToken),
            byte byteValue => importer.WriteAsync(byteValue, cancellationToken),
            byte[] byteArrayValue => importer.WriteAsync(byteArrayValue, cancellationToken),
            DateTime dateTimeValue => importer.WriteAsync(dateTimeValue, cancellationToken),
            DateOnly dateOnlyValue => importer.WriteAsync(dateOnlyValue, cancellationToken),
            TimeOnly timeOnlyValue => importer.WriteAsync(timeOnlyValue, cancellationToken),
            TimeSpan timeSpanValue => importer.WriteAsync(timeSpanValue, cancellationToken),
            char charValue => importer.WriteAsync(charValue.ToString(), cancellationToken),
            _ => importer.WriteAsync(value, column.StoreTypeName, cancellationToken)
        };
    }

    private static async Task ExecuteNonQueryAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task TryDropTempTableAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string tempTableName,
        CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteNonQueryAsync(connection, transaction, $"DROP TABLE IF EXISTS {BulkTableModel.QuoteIdentifier(tempTableName)};", cancellationToken);
        }
        catch
        {
        }
    }

    private static string BuildCreateTempTableSql(BulkTableModel table, string tempTableName, IReadOnlyList<BulkColumn> columns)
    {
        return $"CREATE TEMP TABLE {BulkTableModel.QuoteIdentifier(tempTableName)} AS SELECT {string.Join(", ", columns.Select(column => column.SqlColumnName))} FROM {table.QualifiedTableName} WITH NO DATA;";
    }

    private static string BuildInsertSql(BulkTableModel table, string tempTableName)
    {
        return $"INSERT INTO {table.QualifiedTableName} ({string.Join(", ", table.InsertColumns.Select(column => column.SqlColumnName))}) SELECT {string.Join(", ", table.InsertColumns.Select(column => column.SqlColumnName))} FROM {BulkTableModel.QuoteIdentifier(tempTableName)};";
    }

    private static string BuildMergeSql(BulkTableModel table, string tempTableName)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append($"INSERT INTO {table.QualifiedTableName} (");
        sql.Append(string.Join(", ", table.InsertColumns.Select(column => column.SqlColumnName)));
        sql.Append(") SELECT ");
        sql.Append(string.Join(", ", table.InsertColumns.Select(column => column.SqlColumnName)));
        sql.Append(" FROM ");
        sql.Append(BulkTableModel.QuoteIdentifier(tempTableName));
        sql.Append(" ON CONFLICT (");
        sql.Append(string.Join(", ", table.KeyColumns.Select(column => column.SqlColumnName)));
        sql.Append(") ");

        if (table.UpdateColumns.Count == 0)
        {
            sql.Append("DO NOTHING;");
        }
        else
        {
            sql.Append("DO UPDATE SET ");
            sql.Append(string.Join(", ", table.UpdateColumns.Select(column => $"{column.SqlColumnName} = excluded.{column.SqlColumnName}")));
            sql.Append(';');
        }

        return sql.ToString();
    }

    private static string BuildDeleteByKeySql(BulkTableModel table, string tempTableName)
    {
        return $"DELETE FROM {table.QualifiedTableName} AS target USING {BulkTableModel.QuoteIdentifier(tempTableName)} AS source WHERE {BuildColumnEqualityClause(table.KeyColumns, "target", "source")};";
    }

    private static string BuildSynchronizeDeleteSql(BulkTableModel table, string tempTableName)
    {
        var sql = new System.Text.StringBuilder();
        sql.Append($"DELETE FROM {table.QualifiedTableName} AS target WHERE ");

        if (table.ScopeColumns.Count > 0)
        {
            sql.Append("EXISTS (SELECT 1 FROM ");
            sql.Append(BulkTableModel.QuoteIdentifier(tempTableName));
            sql.Append(" AS scope_source WHERE ");
            sql.Append(BuildColumnEqualityClause(table.ScopeColumns, "target", "scope_source"));
            sql.Append(") AND ");
        }

        sql.Append("NOT EXISTS (SELECT 1 FROM ");
        sql.Append(BulkTableModel.QuoteIdentifier(tempTableName));
        sql.Append(" AS source WHERE ");
        sql.Append(BuildColumnEqualityClause(table.KeyColumns, "target", "source"));
        sql.Append(");");

        return sql.ToString();
    }

    private static string BuildColumnEqualityClause(IReadOnlyList<BulkColumn> columns, string leftAlias, string rightAlias)
    {
        return string.Join(
            " AND ",
            columns.Select(column => $"{leftAlias}.{column.SqlColumnName} = {rightAlias}.{column.SqlColumnName}"));
    }

    private static string CreateTempTableName()
    {
        return $"bf_tmp_{Guid.NewGuid():N}";
    }
}
