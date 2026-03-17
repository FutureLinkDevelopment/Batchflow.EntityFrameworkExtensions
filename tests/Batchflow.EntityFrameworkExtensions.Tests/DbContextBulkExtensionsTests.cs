using Batchflow.EntityFrameworkExtensions.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Tests;

public class DbContextBulkExtensionsTests
{
    [Fact]
    public async Task BulkMergeAsync_Throws_NotImplemented_When_Options_Are_Valid()
    {
        await using var dbContext = CreateDbContext();

        var exception = await Assert.ThrowsAsync<NotImplementedException>(() =>
            dbContext.BulkMergeAsync(
                Array.Empty<TestEntity>(),
                options => options.KeyProperties.Add("ImportKey")));

        Assert.Contains(nameof(BulkOperationType.Merge), exception.Message, StringComparison.Ordinal);
    }

    private static TestDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString("N"))
            .Options;

        return new TestDbContext(options);
    }

    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();
    }

    private sealed class TestEntity
    {
        public Guid Id { get; set; }

        public string ImportKey { get; set; } = string.Empty;
    }
}
