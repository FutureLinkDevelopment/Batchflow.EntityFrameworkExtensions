using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkModelValidator
{
    public static void ValidateConfiguredModel(IEntityType entityType, BulkTableModel table, BulkOperationOptions options)
    {
        if ((options.OperationType == BulkOperationType.Merge || options.OperationType == BulkOperationType.Synchronize) &&
            !HasCompatibleUniqueConstraint(entityType, table.KeyColumns))
        {
            throw new InvalidOperationException(
                $"The configured key properties '{string.Join(", ", options.KeyProperties)}' on entity '{entityType.DisplayName()}' do not match a primary key or unique index. Bulk merge and synchronize require a unique database constraint for ON CONFLICT handling.");
        }
    }

    public static void ValidateNoDuplicateSourceKeys<TEntity>(
        IReadOnlyList<BulkColumn> keyColumns,
        IReadOnlyCollection<TEntity> entities,
        string operationName)
        where TEntity : class
    {
        if (keyColumns.Count == 0 || entities.Count <= 1)
        {
            return;
        }

        var seen = new HashSet<CompositeValueKey>();

        foreach (var entity in entities)
        {
            var key = new CompositeValueKey(keyColumns.Select(column => column.GetValue(entity)).ToArray());
            if (!seen.Add(key))
            {
                throw new InvalidOperationException(
                    $"Duplicate source key detected for {operationName}. The payload contains multiple rows with the same configured key values for '{string.Join(", ", keyColumns.Select(column => column.Property.Name))}'.");
            }
        }
    }

    public static void ValidateNoNullSourceKeys<TEntity>(
        IReadOnlyList<BulkColumn> keyColumns,
        IReadOnlyCollection<TEntity> entities,
        string operationName)
        where TEntity : class
    {
        if (keyColumns.Count == 0 || entities.Count == 0)
        {
            return;
        }

        foreach (var entity in entities)
        {
            foreach (var column in keyColumns)
            {
                if (column.GetValue(entity) is null)
                {
                    throw new InvalidOperationException(
                        $"Null key value detected for {operationName}. The configured key property '{column.Property.Name}' cannot be null in the source payload.");
                }
            }
        }
    }

    public static void ValidateSynchronizeScope<TEntity>(
        BulkTableModel table,
        IReadOnlyCollection<TEntity> entities,
        BulkOperationOptions options)
        where TEntity : class
    {
        if (options.OperationType == BulkOperationType.Synchronize &&
            options.DeleteMissingRows &&
            table.ScopeColumns.Count > 0 &&
            entities.Count == 0)
        {
            throw new InvalidOperationException(
                "Scoped synchronize cannot infer which scope to delete from an empty payload. Provide at least one source row per scope, or use a more explicit delete operation.");
        }
    }

    private static bool HasCompatibleUniqueConstraint(IEntityType entityType, IReadOnlyList<BulkColumn> keyColumns)
    {
        if (keyColumns.Count == 0)
        {
            return false;
        }

        var keyPropertyNames = new HashSet<string>(keyColumns.Select(column => column.Property.Name), StringComparer.OrdinalIgnoreCase);

        return Matches(entityType.FindPrimaryKey()?.Properties, keyPropertyNames)
            || entityType.GetKeys().Any(key => Matches(key.Properties, keyPropertyNames))
            || entityType.GetIndexes().Where(index => index.IsUnique).Any(index => Matches(index.Properties, keyPropertyNames));
    }

    private static bool Matches(IReadOnlyList<IProperty>? properties, HashSet<string> keyPropertyNames)
    {
        return properties is not null
            && properties.Count == keyPropertyNames.Count
            && properties.All(property => keyPropertyNames.Contains(property.Name));
    }
}
