using System.Reflection;
using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkMergeExecutor
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

        EnsureSupportedProvider(dbContext);

        var entityType = dbContext.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"Entity type '{typeof(TEntity).Name}' is not part of the current DbContext model.");

        var table = BulkTableModel.Create(entityType, options);
        var batchSize = options.BatchSize ?? entities.Count;
        var startedTransaction = false;
        var transaction = dbContext.Database.CurrentTransaction;

        if (transaction is null)
        {
            transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
            startedTransaction = true;
        }

        try
        {
            foreach (var batch in entities.Chunk(batchSize))
            {
                var command = BuildCommand(table, batch);
                await dbContext.Database.ExecuteSqlRawAsync(command.Sql, command.Parameters, cancellationToken);
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

    private static void EnsureSupportedProvider(DbContext dbContext)
    {
        var provider = dbContext.Database.ProviderName;

        if (provider is null || (!provider.Contains("Npgsql", StringComparison.OrdinalIgnoreCase) && !provider.Contains("Sqlite", StringComparison.OrdinalIgnoreCase)))
        {
            throw new NotSupportedException("BulkMerge currently supports PostgreSQL and SQLite providers only.");
        }
    }

    private static BulkCommand BuildCommand<TEntity>(BulkTableModel table, IReadOnlyCollection<TEntity> entities)
        where TEntity : class
    {
        var sql = new System.Text.StringBuilder();
        var parameters = new List<object>();

        sql.Append("INSERT INTO ");
        sql.Append(table.QualifiedTableName);
        sql.Append(" (");
        sql.Append(string.Join(", ", table.InsertColumns.Select(column => column.SqlColumnName)));
        sql.Append(") VALUES ");

        var parameterIndex = 0;
        var rowIndex = 0;

        foreach (var entity in entities)
        {
            if (rowIndex > 0)
            {
                sql.Append(", ");
            }

            sql.Append('(');

            for (var columnIndex = 0; columnIndex < table.InsertColumns.Count; columnIndex++)
            {
                if (columnIndex > 0)
                {
                    sql.Append(", ");
                }

                var column = table.InsertColumns[columnIndex];
                var value = column.GetValue(entity);
                if (column.ShouldGenerateGuid && value is Guid guid && guid == Guid.Empty)
                {
                    value = Guid.NewGuid();
                    column.SetValue(entity, value);
                }

                sql.Append("@p");
                sql.Append(parameterIndex);
                parameters.Add(value ?? DBNull.Value);
                parameterIndex++;
            }

            sql.Append(')');
            rowIndex++;
        }

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

        return new BulkCommand(sql.ToString(), parameters.ToArray());
    }

    private sealed record BulkCommand(string Sql, object[] Parameters);
}

internal sealed class BulkTableModel
{
    private BulkTableModel(
        string qualifiedTableName,
        IReadOnlyList<BulkColumn> insertColumns,
        IReadOnlyList<BulkColumn> keyColumns,
        IReadOnlyList<BulkColumn> updateColumns)
    {
        QualifiedTableName = qualifiedTableName;
        InsertColumns = insertColumns;
        KeyColumns = keyColumns;
        UpdateColumns = updateColumns;
    }

    public string QualifiedTableName { get; }

    public IReadOnlyList<BulkColumn> InsertColumns { get; }

    public IReadOnlyList<BulkColumn> KeyColumns { get; }

    public IReadOnlyList<BulkColumn> UpdateColumns { get; }

    public static BulkTableModel Create(IEntityType entityType, BulkOperationOptions options)
    {
        var tableName = entityType.GetTableName() ?? throw new InvalidOperationException($"Entity '{entityType.Name}' is not mapped to a table.");
        var schema = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schema);
        var properties = entityType.GetProperties()
            .Where(property => !property.IsShadowProperty() && property.PropertyInfo is not null)
            .ToDictionary(property => property.Name, StringComparer.OrdinalIgnoreCase);

        var keyColumns = options.KeyProperties
            .Select(keyProperty => CreateColumn(keyProperty, properties, storeObject))
            .ToList();

        var insertColumns = properties.Values
            .Select(property => CreateColumn(property, storeObject))
            .Where(column => ShouldIncludeOnInsert(column, keyColumns, options))
            .ToList();

        foreach (var keyColumn in keyColumns)
        {
            if (insertColumns.All(column => !column.Property.Name.Equals(keyColumn.Property.Name, StringComparison.OrdinalIgnoreCase)))
            {
                insertColumns.Add(keyColumn);
            }
        }

        var updateColumns = insertColumns
            .Where(column => keyColumns.All(keyColumn => !keyColumn.Property.Name.Equals(column.Property.Name, StringComparison.OrdinalIgnoreCase)))
            .Where(column => !options.IgnoredPropertiesOnUpdate.Contains(column.Property.Name, StringComparer.OrdinalIgnoreCase))
            .Where(column => !column.Property.IsPrimaryKey())
            .Where(column => column.Property.ValueGenerated != ValueGenerated.OnAddOrUpdate)
            .ToList();

        var qualifiedTableName = string.IsNullOrWhiteSpace(schema)
            ? QuoteIdentifier(tableName)
            : $"{QuoteIdentifier(schema)}.{QuoteIdentifier(tableName)}";

        return new BulkTableModel(qualifiedTableName, insertColumns, keyColumns, updateColumns);
    }

    private static bool ShouldIncludeOnInsert(BulkColumn column, IReadOnlyCollection<BulkColumn> keyColumns, BulkOperationOptions options)
    {
        if (options.IgnoredPropertiesOnInsert.Contains(column.Property.Name, StringComparer.OrdinalIgnoreCase))
        {
            return false;
        }

        if (column.Property.ValueGenerated == ValueGenerated.OnAddOrUpdate)
        {
            return false;
        }

        if (column.Property.IsPrimaryKey() && column.Property.ClrType != typeof(Guid))
        {
            return false;
        }

        return true;
    }

    private static BulkColumn CreateColumn(string propertyName, IReadOnlyDictionary<string, IProperty> properties, StoreObjectIdentifier storeObject)
    {
        if (!properties.TryGetValue(propertyName, out var property))
        {
            throw new InvalidOperationException($"Property '{propertyName}' is not mapped on entity '{storeObject.Name}'.");
        }

        return CreateColumn(property, storeObject);
    }

    private static BulkColumn CreateColumn(IProperty property, StoreObjectIdentifier storeObject)
    {
        var propertyInfo = property.PropertyInfo ?? throw new InvalidOperationException($"Property '{property.Name}' does not expose a CLR property.");
        var columnName = property.GetColumnName(storeObject) ?? property.Name;

        return new BulkColumn(property, propertyInfo, QuoteIdentifier(columnName));
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"") }\"";
    }
}

internal sealed class BulkColumn(IProperty property, PropertyInfo propertyInfo, string sqlColumnName)
{
    public IProperty Property { get; } = property;

    public PropertyInfo PropertyInfo { get; } = propertyInfo;

    public string SqlColumnName { get; } = sqlColumnName;

    public bool ShouldGenerateGuid => Property.IsPrimaryKey() && Property.ClrType == typeof(Guid);

    public object? GetValue(object entity)
    {
        return PropertyInfo.GetValue(entity);
    }

    public void SetValue(object entity, object? value)
    {
        PropertyInfo.SetValue(entity, value);
    }
}
