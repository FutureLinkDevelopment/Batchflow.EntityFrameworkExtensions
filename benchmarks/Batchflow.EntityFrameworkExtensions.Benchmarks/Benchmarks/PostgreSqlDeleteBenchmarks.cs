using Batchflow.EntityFrameworkExtensions;
using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(iterationCount: 10, warmupCount: 3)]
public class PostgreSqlDeleteBenchmarks
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
        Fixture.SeedExistingRowsAsync(BatchSize * 2).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _fixture?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public async Task CurrentTrackedDeleteByKey()
    {
        var deletePayload = BenchmarkDataFactory.CreateDeleteKeyBatch(BatchSize);
        var keys = deletePayload.Select(x => x.ImportKey).ToHashSet(StringComparer.OrdinalIgnoreCase);

        await using var dbContext = Fixture.CreateDbContext();
        var rowsToDelete = await dbContext.Entities
            .Where(x => keys.Contains(x.ImportKey))
            .ToListAsync();

        dbContext.Entities.RemoveRange(rowsToDelete);
        await dbContext.SaveChangesAsync();
    }

    [Benchmark]
    public async Task PackageBulkDeleteByKey()
    {
        var deletePayload = BenchmarkDataFactory.CreateDeleteKeyBatch(BatchSize);

        await using var dbContext = Fixture.CreateDbContext();
        await dbContext.BulkDeleteByKeyAsync(
            deletePayload,
            options => options.KeyProperties.Add(nameof(BulkEntity.ImportKey)));
    }

    private PostgreSqlBenchmarkFixture Fixture => _fixture ?? throw new InvalidOperationException("Benchmark fixture has not been initialized.");
}
