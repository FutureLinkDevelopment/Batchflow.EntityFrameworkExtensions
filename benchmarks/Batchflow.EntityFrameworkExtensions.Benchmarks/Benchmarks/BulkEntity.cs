namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

internal sealed class BulkEntity
{
    public Guid Id { get; set; }

    public string ImportKey { get; set; } = string.Empty;

    public string ExternalNumber { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public bool IsActive { get; set; }

    public DateTime UpdatedUtc { get; set; }
}
