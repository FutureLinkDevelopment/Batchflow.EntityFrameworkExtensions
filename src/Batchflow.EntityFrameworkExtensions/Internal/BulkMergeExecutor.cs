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
        var table = CreateTableModel<TEntity>(dbContext, options);
        BulkModelValidator.ValidateNoNullSourceKeys(table.KeyColumns, entities, nameof(BulkOperationType.Merge));
        BulkModelValidator.ValidateNoDuplicateSourceKeys(table.KeyColumns, entities, nameof(BulkOperationType.Merge));

        if (IsPostgreSqlProvider(dbContext) && PostgreSqlBulkExecutor.ShouldUseStagingFastPath(entities.Count))
        {
            await PostgreSqlBulkExecutor.ExecuteMergeAsync(dbContext, entities, table, cancellationToken);
            return;
        }

        await ExecuteBatchedAsync(
            dbContext,
            entities,
            options.BatchSize,
            batch => BulkMergeCommandFactory.BuildCommand(table, batch),
            table.InsertColumns.Count,
            0,
            cancellationToken);
    }

    internal static async Task ExecuteBatchedAsync<TEntity>(
        DbContext dbContext,
        IReadOnlyCollection<TEntity> entities,
        int? batchSize,
        Func<IReadOnlyCollection<TEntity>, BulkCommand> commandFactory,
        int parametersPerRow,
        int fixedParameterCount,
        CancellationToken cancellationToken)
        where TEntity : class
    {
        if (entities.Count == 0)
        {
            return;
        }

        var effectiveBatchSize = BulkBatchPlanner.DetermineBatchSize(dbContext, batchSize, parametersPerRow, fixedParameterCount);
        var startedTransaction = false;
        var transaction = dbContext.Database.CurrentTransaction;

        if (transaction is null)
        {
            transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
            startedTransaction = true;
        }

        try
        {
            foreach (var batch in entities.Chunk(effectiveBatchSize))
            {
                var command = commandFactory(batch);
                await dbContext.Database.ExecuteSqlRawAsync(command.Sql, CreateDbParameters(dbContext, command.Parameters), cancellationToken);
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

    internal static BulkTableModel CreateTableModel<TEntity>(DbContext dbContext, BulkOperationOptions options)
        where TEntity : class
    {
        var entityType = dbContext.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"Entity type '{typeof(TEntity).Name}' is not part of the current DbContext model.");

        var table = BulkTableModel.Create(entityType, options);
        BulkModelValidator.ValidateConfiguredModel(entityType, table, options);

        return table;
    }

    internal static void EnsureSupportedProvider(DbContext dbContext)
    {
        if (!IsPostgreSqlProvider(dbContext) && !IsSqliteProvider(dbContext))
        {
            throw new NotSupportedException("Bulk operations currently support PostgreSQL and SQLite providers only.");
        }
    }

    internal static bool IsPostgreSqlProvider(DbContext dbContext)
    {
        return dbContext.Database.ProviderName?.Contains("Npgsql", StringComparison.OrdinalIgnoreCase) == true;
    }

    internal static bool IsSqliteProvider(DbContext dbContext)
    {
        return dbContext.Database.ProviderName?.Contains("Sqlite", StringComparison.OrdinalIgnoreCase) == true;
    }

    internal static object[] CreateDbParameters(DbContext dbContext, IReadOnlyList<BulkSqlParameter> parameters)
    {
        using var parameterCommand = dbContext.Database.GetDbConnection().CreateCommand();

        return parameters
            .Select(parameter =>
            {
                var dbParameter = parameterCommand.CreateParameter();
                dbParameter.ParameterName = parameter.Name;
                dbParameter.Value = parameter.Value ?? DBNull.Value;
                return (object)dbParameter;
            })
            .ToArray();
    }
}

internal sealed record BulkCommand(string Sql, BulkSqlParameter[] Parameters);

internal sealed record BulkSqlParameter(string Name, object? Value);

internal sealed class BulkTableModel
{
    private BulkTableModel(
        string qualifiedTableName,
        IReadOnlyList<BulkColumn> insertColumns,
        IReadOnlyList<BulkColumn> keyColumns,
        IReadOnlyList<BulkColumn> scopeColumns,
        IReadOnlyList<BulkColumn> updateColumns)
    {
        QualifiedTableName = qualifiedTableName;
        InsertColumns = insertColumns;
        KeyColumns = keyColumns;
        ScopeColumns = scopeColumns;
        UpdateColumns = updateColumns;
    }

    public string QualifiedTableName { get; }

    public IReadOnlyList<BulkColumn> InsertColumns { get; }

    public IReadOnlyList<BulkColumn> KeyColumns { get; }

    public IReadOnlyList<BulkColumn> ScopeColumns { get; }

    public IReadOnlyList<BulkColumn> UpdateColumns { get; }

    public static BulkTableModel Create(IEntityType entityType, BulkOperationOptions options)
    {
        var tableName = entityType.GetTableName() ?? throw new InvalidOperationException($"Entity '{entityType.Name}' is not mapped to a table.");
        var schema = entityType.GetSchema();
        var storeObject = StoreObjectIdentifier.Table(tableName, schema);
        var properties = entityType.GetProperties()
            .Where(property => !property.IsShadowProperty() && property.PropertyInfo is not null)
            .Where(property => property.GetColumnName(storeObject) is not null)
            .ToDictionary(property => property.Name, StringComparer.OrdinalIgnoreCase);

        var keyColumns = options.KeyProperties
            .Select(keyProperty => CreateColumn(keyProperty, properties, storeObject))
            .ToList();

        var scopeColumns = options.ScopeProperties
            .Select(scopeProperty => CreateColumn(scopeProperty, properties, storeObject))
            .ToList();

        var insertColumns = properties.Values
            .Select(property => CreateColumn(property, storeObject))
            .Where(column => ShouldIncludeOnInsert(column, options))
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
            .Where(column => column.Property.GetAfterSaveBehavior() == PropertySaveBehavior.Save)
            .ToList();

        var qualifiedTableName = string.IsNullOrWhiteSpace(schema)
            ? QuoteIdentifier(tableName)
            : $"{QuoteIdentifier(schema)}.{QuoteIdentifier(tableName)}";

        return new BulkTableModel(qualifiedTableName, insertColumns, keyColumns, scopeColumns, updateColumns);
    }

    private static bool ShouldIncludeOnInsert(BulkColumn column, BulkOperationOptions options)
    {
        if (options.IgnoredPropertiesOnInsert.Contains(column.Property.Name, StringComparer.OrdinalIgnoreCase))
        {
            return false;
        }

        if (column.Property.ValueGenerated == ValueGenerated.OnAddOrUpdate)
        {
            return false;
        }

        if (column.Property.IsPrimaryKey() && column.Property.ClrType != typeof(Guid) && column.Property.ValueGenerated == ValueGenerated.OnAdd)
        {
            return false;
        }

        return column.Property.GetBeforeSaveBehavior() == PropertySaveBehavior.Save
            || (column.Property.IsPrimaryKey() && column.Property.ClrType == typeof(Guid));
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
        var columnName = property.GetColumnName(storeObject)
            ?? throw new InvalidOperationException($"Property '{property.Name}' is not mapped to store object '{storeObject.Name}'.");

        return new BulkColumn(
            property,
            propertyInfo,
            columnName,
            QuoteIdentifier(columnName),
            property.GetRelationalTypeMapping().StoreTypeNameBase);
    }

    internal static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"") }\"";
    }
}

internal sealed class BulkColumn(IProperty property, PropertyInfo propertyInfo, string columnName, string sqlColumnName, string storeTypeName)
{
    public IProperty Property { get; } = property;

    public PropertyInfo PropertyInfo { get; } = propertyInfo;

    public string ColumnName { get; } = columnName;

    public string SqlColumnName { get; } = sqlColumnName;

    public string StoreTypeName { get; } = storeTypeName;

    public bool ShouldGenerateGuid => Property.IsPrimaryKey() && Property.ClrType == typeof(Guid);

    public object? GetValue(object entity)
    {
        return PropertyInfo.GetValue(entity);
    }

    public object? GetWriteValue(object entity)
    {
        var value = PropertyInfo.GetValue(entity);
        if (ShouldGenerateGuid && value is Guid guid && guid == Guid.Empty)
        {
            value = Guid.NewGuid();
            PropertyInfo.SetValue(entity, value);
        }

        return value;
    }

    public void SetValue(object entity, object? value)
    {
        PropertyInfo.SetValue(entity, value);
    }
}
