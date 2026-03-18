using Batchflow.EntityFrameworkExtensions;
using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(iterationCount: 10, warmupCount: 3)]
public class PostgreSqlUpsertBenchmarks
{
    private PostgreSqlBenchmarkFixture? _fixture;

    [Params(100, 250, 1_000, 5_000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _fixture = PostgreSqlBenchmarkFixture.CreateAsync().GetAwaiter().GetResult();
        _fixture.ResetDatabaseAsync().GetAwaiter().GetResult();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        Fixture.ResetDatabaseAsync().GetAwaiter().GetResult();
        Fixture.SeedExistingRowsAsync(BatchSize).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _fixture?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public async Task CurrentTrackedMerge_PartialLoad()
    {
        var entities = BenchmarkDataFactory.CreateMixedBatch(BatchSize);
        var keys = entities.Select(x => x.ImportKey).ToHashSet(StringComparer.OrdinalIgnoreCase);

        await using var dbContext = Fixture.CreateDbContext();

        var existingRows = await dbContext.Entities
            .Where(x => keys.Contains(x.ImportKey))
            .ToListAsync();

        var existingByKey = existingRows.ToDictionary(x => x.ImportKey, StringComparer.OrdinalIgnoreCase);

        foreach (var candidate in entities)
        {
            if (existingByKey.TryGetValue(candidate.ImportKey, out var existing))
            {
                existing.Name = candidate.Name;
                existing.ExternalNumber = candidate.ExternalNumber;
                existing.IsActive = candidate.IsActive;
                existing.UpdatedUtc = candidate.UpdatedUtc;
            }
            else
            {
                dbContext.Entities.Add(candidate);
            }
        }

        await dbContext.SaveChangesAsync();
    }

    [Benchmark]
    public async Task CurrentTrackedSynchronize_FullSnapshot()
    {
        var entities = BenchmarkDataFactory.CreateMixedBatch(BatchSize);
        var keys = entities.Select(x => x.ImportKey).ToHashSet(StringComparer.OrdinalIgnoreCase);

        await using var dbContext = Fixture.CreateDbContext();

        var existingRows = await dbContext.Entities.ToListAsync();
        var existingByKey = existingRows.ToDictionary(x => x.ImportKey, StringComparer.OrdinalIgnoreCase);

        foreach (var candidate in entities)
        {
            if (existingByKey.TryGetValue(candidate.ImportKey, out var existing))
            {
                existing.Name = candidate.Name;
                existing.ExternalNumber = candidate.ExternalNumber;
                existing.IsActive = candidate.IsActive;
                existing.UpdatedUtc = candidate.UpdatedUtc;
            }
            else
            {
                dbContext.Entities.Add(candidate);
            }
        }

        var rowsToDelete = existingRows
            .Where(x => !keys.Contains(x.ImportKey))
            .ToList();

        if (rowsToDelete.Count > 0)
        {
            dbContext.Entities.RemoveRange(rowsToDelete);
        }

        await dbContext.SaveChangesAsync();
    }

    [Benchmark]
    public async Task PackageBulkMerge()
    {
        var entities = BenchmarkDataFactory.CreateMixedBatch(BatchSize);

        await using var dbContext = Fixture.CreateDbContext();
        await dbContext.BulkMergeAsync(
            entities,
            options => options.KeyProperties.Add(nameof(BulkEntity.ImportKey)));
    }

    [Benchmark]
    public async Task PackageBulkSynchronize()
    {
        var entities = BenchmarkDataFactory.CreateMixedBatch(BatchSize);

        await using var dbContext = Fixture.CreateDbContext();
        await dbContext.BulkSynchronizeAsync(
            entities,
            options => options.KeyProperties.Add(nameof(BulkEntity.ImportKey)));
    }

    private PostgreSqlBenchmarkFixture Fixture => _fixture ?? throw new InvalidOperationException("Benchmark fixture has not been initialized.");
}
