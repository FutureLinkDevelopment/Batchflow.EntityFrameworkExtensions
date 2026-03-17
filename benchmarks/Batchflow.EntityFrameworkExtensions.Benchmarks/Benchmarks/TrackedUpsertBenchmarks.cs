using Batchflow.EntityFrameworkExtensions;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(iterationCount: 10, warmupCount: 3)]
public class TrackedUpsertBenchmarks
{
    private readonly SqliteConnection _connection;

    [Params(100, 1_000, 5_000)]
    public int BatchSize { get; set; }

    public TrackedUpsertBenchmarks()
    {
        _connection = new SqliteConnection("Data Source=:memory:");
        _connection.Open();
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        using var dbContext = CreateDbContext();
        dbContext.Database.EnsureCreated();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        ResetDatabaseAsync().GetAwaiter().GetResult();
        SeedExistingRowsAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _connection.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task CurrentTrackedMerge_PartialLoad()
    {
        var entities = BenchmarkDataFactory.CreateMixedBatch(BatchSize);
        var keys = entities.Select(x => x.ImportKey).ToHashSet(StringComparer.OrdinalIgnoreCase);

        await using var dbContext = CreateDbContext();

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

        await using var dbContext = CreateDbContext();

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

        await using var dbContext = CreateDbContext();
        await dbContext.BulkMergeAsync(
            entities,
            options => options.KeyProperties.Add(nameof(BulkEntity.ImportKey)));
    }

    private BulkBenchmarkDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<BulkBenchmarkDbContext>()
            .UseSqlite(_connection)
            .EnableDetailedErrors(false)
            .EnableSensitiveDataLogging(false)
            .Options;

        return new BulkBenchmarkDbContext(options);
    }

    private async Task ResetDatabaseAsync()
    {
        await using var dbContext = CreateDbContext();
        await dbContext.Database.EnsureDeletedAsync();
        await dbContext.Database.EnsureCreatedAsync();
    }

    private async Task SeedExistingRowsAsync()
    {
        await using var dbContext = CreateDbContext();

        var existingRows = BenchmarkDataFactory.CreateSeedBatch(BatchSize);
        await dbContext.Entities.AddRangeAsync(existingRows);
        await dbContext.SaveChangesAsync();
    }
}
