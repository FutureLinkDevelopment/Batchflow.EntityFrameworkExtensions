namespace Batchflow.EntityFrameworkExtensions.Abstractions;

/// <summary>
/// Validates bulk operation configuration before execution starts.
/// </summary>
public static class BulkOperationOptionsValidator
{
    /// <summary>
    /// Validates a <see cref="BulkOperationOptions"/> instance.
    /// </summary>
    /// <param name="options">The options to validate.</param>
    public static void Validate(BulkOperationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        EnsureDistinct(options.KeyProperties, nameof(options.KeyProperties));
        EnsureDistinct(options.ScopeProperties, nameof(options.ScopeProperties));

        if (options.OperationType != BulkOperationType.Insert && options.KeyProperties.Count == 0)
        {
            throw new ArgumentException("At least one key property must be configured.", nameof(options));
        }

        if (options.BatchSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "BatchSize must be greater than zero when specified.");
        }
    }

    private static void EnsureDistinct(IEnumerable<string> propertyNames, string propertyGroupName)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var propertyName in propertyNames)
        {
            if (!seen.Add(propertyName))
            {
                throw new ArgumentException($"{propertyGroupName} contains duplicate property '{propertyName}'.", propertyGroupName);
            }
        }
    }
}
