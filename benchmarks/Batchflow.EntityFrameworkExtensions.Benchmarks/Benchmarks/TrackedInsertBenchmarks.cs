using Batchflow.EntityFrameworkExtensions;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(iterationCount: 10, warmupCount: 3)]
public class TrackedInsertBenchmarks
{
    private readonly SqliteConnection _connection;

    [Params(100, 1_000, 5_000)]
    public int BatchSize { get; set; }

    public TrackedInsertBenchmarks()
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
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _connection.Dispose();
    }

    [Benchmark(Baseline = true)]
    public async Task CurrentTrackedInsert()
    {
        var entities = BenchmarkDataFactory.CreateSeedBatch(BatchSize);

        await using var dbContext = CreateDbContext();
        await dbContext.Entities.AddRangeAsync(entities);
        await dbContext.SaveChangesAsync();
    }

    [Benchmark]
    public async Task PackageBulkInsert()
    {
        var entities = BenchmarkDataFactory.CreateSeedBatch(BatchSize);

        await using var dbContext = CreateDbContext();
        await dbContext.BulkInsertAsync(entities);
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
}
