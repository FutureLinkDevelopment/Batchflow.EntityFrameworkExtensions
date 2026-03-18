namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

internal static class BenchmarkDataFactory
{
    public static List<BulkEntity> CreateSeedBatch(int batchSize)
    {
        return Enumerable.Range(0, batchSize)
            .Select(index => CreateEntity(index, suffix: "seed"))
            .ToList();
    }

    public static List<BulkEntity> CreateMixedBatch(int batchSize)
    {
        var overlapCount = batchSize / 2;
        var insertCount = batchSize - overlapCount;
        var results = new List<BulkEntity>(batchSize);

        for (var index = 0; index < overlapCount; index++)
        {
            results.Add(CreateEntity(index, suffix: "updated"));
        }

        for (var index = 0; index < insertCount; index++)
        {
            results.Add(CreateEntity(batchSize + index, suffix: "new"));
        }

        return results;
    }

    public static List<BulkEntity> CreateDeleteKeyBatch(int batchSize)
    {
        return Enumerable.Range(0, batchSize)
            .Select(index => new BulkEntity
            {
                ImportKey = $"entity:{index}"
            })
            .ToList();
    }

    private static BulkEntity CreateEntity(int index, string suffix)
    {
        return new BulkEntity
        {
            Id = Guid.NewGuid(),
            ImportKey = $"entity:{index}",
            ExternalNumber = $"EXT-{index:000000}",
            Name = $"Entity {index} ({suffix})",
            IsActive = index % 3 != 0,
            UpdatedUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMinutes(index)
        };
    }
}
