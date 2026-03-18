using Batchflow.EntityFrameworkExtensions;
using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(iterationCount: 10, warmupCount: 3)]
public class PostgreSqlInsertBenchmarks
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
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _fixture?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public async Task CurrentTrackedInsert()
    {
        var entities = BenchmarkDataFactory.CreateSeedBatch(BatchSize);

        await using var dbContext = Fixture.CreateDbContext();
        await dbContext.Entities.AddRangeAsync(entities);
        await dbContext.SaveChangesAsync();
    }

    [Benchmark]
    public async Task PackageBulkInsert()
    {
        var entities = BenchmarkDataFactory.CreateSeedBatch(BatchSize);

        await using var dbContext = Fixture.CreateDbContext();
        await dbContext.BulkInsertAsync(entities);
    }

    private PostgreSqlBenchmarkFixture Fixture => _fixture ?? throw new InvalidOperationException("Benchmark fixture has not been initialized.");
}
