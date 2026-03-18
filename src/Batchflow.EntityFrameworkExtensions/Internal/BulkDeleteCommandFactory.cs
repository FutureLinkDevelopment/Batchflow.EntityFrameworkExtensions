using System.Text;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkDeleteCommandFactory
{
    public static BulkCommand BuildDeleteByKeyCommand<TEntity>(BulkTableModel table, IReadOnlyCollection<TEntity> entities)
        where TEntity : class
    {
        var sourceKeys = ExtractDistinctKeys(table.KeyColumns, entities);
        return BuildDeleteCommand(table, sourceKeys, scope: null, deleteMatches: true);
    }

    public static IReadOnlyList<BulkCommand> BuildDeleteByKeyCommands(
        BulkTableModel table,
        IReadOnlyCollection<CompositeValueKey> sourceKeys,
        int keyBatchSize)
    {
        return BuildDeleteCommands(table, sourceKeys, scope: null, deleteMatches: true, keyBatchSize);
    }

    public static IReadOnlyList<BulkCommand> BuildDeleteMissingCommands<TEntity>(
        BulkTableModel table,
        IReadOnlyCollection<TEntity> entities,
        int keyBatchSize)
        where TEntity : class
    {
        if (table.ScopeColumns.Count == 0)
        {
            return entities.Count == 0
                ? [new BulkCommand($"DELETE FROM {table.QualifiedTableName};", [])]
                : BuildDeleteCommands(table, ExtractDistinctKeys(table.KeyColumns, entities), scope: null, deleteMatches: false, keyBatchSize);
        }

        if (entities.Count == 0)
        {
            return [];
        }

        var entitiesByScope = new Dictionary<CompositeValueKey, List<TEntity>>();

        foreach (var entity in entities)
        {
            var scopeKey = new CompositeValueKey(table.ScopeColumns.Select(column => column.GetValue(entity)).ToArray());

            if (!entitiesByScope.TryGetValue(scopeKey, out var scopedEntities))
            {
                scopedEntities = [];
                entitiesByScope.Add(scopeKey, scopedEntities);
            }

            scopedEntities.Add(entity);
        }

        return entitiesByScope
            .SelectMany(group => BuildDeleteCommands(
                table,
                ExtractDistinctKeys(table.KeyColumns, group.Value),
                group.Key,
                deleteMatches: false,
                keyBatchSize))
            .ToArray();
    }

    private static IReadOnlyList<BulkCommand> BuildDeleteCommands(
        BulkTableModel table,
        IReadOnlyCollection<CompositeValueKey> sourceKeys,
        CompositeValueKey? scope,
        bool deleteMatches,
        int keyBatchSize)
    {
        return sourceKeys
            .Chunk(keyBatchSize)
            .Select(batch => BuildDeleteCommand(table, batch, scope, deleteMatches))
            .ToArray();
    }

    private static HashSet<CompositeValueKey> ExtractDistinctKeys<TEntity>(IReadOnlyList<BulkColumn> columns, IReadOnlyCollection<TEntity> entities)
        where TEntity : class
    {
        var keys = new HashSet<CompositeValueKey>();

        foreach (var entity in entities)
        {
            keys.Add(new CompositeValueKey(columns.Select(column => column.GetValue(entity)).ToArray()));
        }

        return keys;
    }

    private static BulkCommand BuildDeleteCommand(
        BulkTableModel table,
        IReadOnlyCollection<CompositeValueKey> sourceKeys,
        CompositeValueKey? scope,
        bool deleteMatches)
    {
        var sql = new StringBuilder();
        var parameters = new List<object>();
        var parameterIndex = 0;

        sql.Append("WITH source (");
        sql.Append(string.Join(", ", table.KeyColumns.Select(column => column.SqlColumnName)));
        sql.Append(") AS (VALUES ");

        var rowIndex = 0;
        foreach (var key in sourceKeys)
        {
            if (rowIndex > 0)
            {
                sql.Append(", ");
            }

            sql.Append('(');
            for (var columnIndex = 0; columnIndex < key.Values.Count; columnIndex++)
            {
                if (columnIndex > 0)
                {
                    sql.Append(", ");
                }

                sql.Append("@p");
                sql.Append(parameterIndex);
                parameters.Add(key.Values[columnIndex] ?? DBNull.Value);
                parameterIndex++;
            }

            sql.Append(')');
            rowIndex++;
        }

        sql.Append(") DELETE FROM ");
        sql.Append(table.QualifiedTableName);
        sql.Append(" WHERE ");

        if (scope is not null)
        {
            sql.Append(string.Join(
                " AND ",
                table.ScopeColumns.Select((column, index) => BuildParameterEqualityExpression(
                    GetTargetColumnReference(table, column),
                    $"@p{parameterIndex + index}"))));

            foreach (var value in scope.Values)
            {
                parameters.Add(value ?? DBNull.Value);
            }

            parameterIndex += scope.Values.Count;
            sql.Append(" AND ");
        }

        sql.Append(deleteMatches ? "EXISTS (SELECT 1 FROM source WHERE " : "NOT EXISTS (SELECT 1 FROM source WHERE ");
        sql.Append(string.Join(
            " AND ",
            table.KeyColumns.Select(column => BuildColumnEqualityExpression(
                GetTargetColumnReference(table, column),
                $"source.{column.SqlColumnName}"))));
        sql.Append(");");

        return new BulkCommand(sql.ToString(), parameters.ToArray());
    }

    private static string GetTargetColumnReference(BulkTableModel table, BulkColumn column)
    {
        return $"{table.QualifiedTableName}.{column.SqlColumnName}";
    }

    private static string BuildParameterEqualityExpression(string columnReference, string parameterName)
    {
        return $"(({columnReference} = {parameterName}) OR ({columnReference} IS NULL AND {parameterName} IS NULL))";
    }

    private static string BuildColumnEqualityExpression(string leftColumnReference, string rightColumnReference)
    {
        return $"(({leftColumnReference} = {rightColumnReference}) OR ({leftColumnReference} IS NULL AND {rightColumnReference} IS NULL))";
    }
}

internal sealed class CompositeValueKey(IReadOnlyList<object?> values) : IEquatable<CompositeValueKey>
{
    public IReadOnlyList<object?> Values { get; } = values;

    public bool Equals(CompositeValueKey? other)
    {
        if (other is null || Values.Count != other.Values.Count)
        {
            return false;
        }

        for (var index = 0; index < Values.Count; index++)
        {
            if (!Equals(Values[index], other.Values[index]))
            {
                return false;
            }
        }

        return true;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as CompositeValueKey);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();

        foreach (var value in Values)
        {
            hash.Add(value);
        }

        return hash.ToHashCode();
    }
}
