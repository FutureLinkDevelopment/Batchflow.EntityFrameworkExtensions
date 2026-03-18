using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Internal;

internal static class BulkBatchPlanner
{
    private const int SqliteMaxParameterCount = 999;
    private const int PostgreSqlMaxParameterCount = 32767;

    public static int DetermineBatchSize(
        DbContext dbContext,
        int? requestedBatchSize,
        int parametersPerRow,
        int fixedParameterCount = 0)
    {
        if (requestedBatchSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(requestedBatchSize), "Batch size must be greater than zero when specified.");
        }

        if (parametersPerRow < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(parametersPerRow), "Parameters per row cannot be negative.");
        }

        if (fixedParameterCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fixedParameterCount), "Fixed parameter count cannot be negative.");
        }

        if (parametersPerRow == 0)
        {
            return requestedBatchSize ?? int.MaxValue;
        }

        var availableParameterCount = GetMaxParameterCount(dbContext) - fixedParameterCount;
        if (availableParameterCount <= 0)
        {
            throw new InvalidOperationException("The configured bulk command leaves no room for row parameters on the current provider.");
        }

        var providerBatchSize = Math.Max(1, availableParameterCount / parametersPerRow);
        return requestedBatchSize.HasValue
            ? Math.Min(requestedBatchSize.Value, providerBatchSize)
            : providerBatchSize;
    }

    private static int GetMaxParameterCount(DbContext dbContext)
    {
        var provider = dbContext.Database.ProviderName;

        return provider switch
        {
            not null when provider.Contains("Sqlite", StringComparison.OrdinalIgnoreCase) => SqliteMaxParameterCount,
            not null when provider.Contains("Npgsql", StringComparison.OrdinalIgnoreCase) => PostgreSqlMaxParameterCount,
            _ => throw new NotSupportedException("Bulk operations currently support PostgreSQL and SQLite providers only.")
        };
    }
}
