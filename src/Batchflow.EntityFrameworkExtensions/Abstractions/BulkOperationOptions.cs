namespace Batchflow.EntityFrameworkExtensions.Abstractions;

/// <summary>
/// Configuration used to describe how a bulk operation should match and process rows.
/// </summary>
public sealed class BulkOperationOptions
{
    /// <summary>
    /// Gets the ordered list of entity properties that make up the comparison key.
    /// </summary>
    public IList<string> KeyProperties { get; } = new List<string>();

    /// <summary>
    /// Gets the properties that define the synchronization scope for delete-missing operations.
    /// </summary>
    public IList<string> ScopeProperties { get; } = new List<string>();

    /// <summary>
    /// Gets the properties that should be excluded from insert statements.
    /// </summary>
    public IList<string> IgnoredPropertiesOnInsert { get; } = new List<string>();

    /// <summary>
    /// Gets the properties that should be excluded from update statements.
    /// </summary>
    public IList<string> IgnoredPropertiesOnUpdate { get; } = new List<string>();

    /// <summary>
    /// Gets or sets the requested operation type.
    /// </summary>
    public BulkOperationType OperationType { get; set; } = BulkOperationType.Merge;

    /// <summary>
    /// Gets or sets whether rows missing from the source payload should be deleted.
    /// </summary>
    public bool DeleteMissingRows { get; set; }

    /// <summary>
    /// Gets or sets the optional batch size for staged writes.
    /// </summary>
    public int? BatchSize { get; set; }
}
