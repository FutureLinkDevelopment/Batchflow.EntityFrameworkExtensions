namespace Batchflow.EntityFrameworkExtensions.Abstractions;

/// <summary>
/// Describes the kind of bulk operation that should be executed.
/// </summary>
public enum BulkOperationType
{
    /// <summary>
    /// Inserts rows without attempting to match existing records.
    /// </summary>
    Insert = 0,

    /// <summary>
    /// Inserts new rows and updates matching rows.
    /// </summary>
    Merge = 1,

    /// <summary>
    /// Inserts new rows, updates matching rows, and optionally deletes rows missing from the input set.
    /// </summary>
    Synchronize = 2,

    /// <summary>
    /// Deletes rows by a configured business key.
    /// </summary>
    DeleteByKey = 3
}
