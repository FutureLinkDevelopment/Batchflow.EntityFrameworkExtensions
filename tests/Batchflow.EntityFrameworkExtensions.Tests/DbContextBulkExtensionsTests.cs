using Batchflow.EntityFrameworkExtensions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Tests;

public class DbContextBulkExtensionsTests
{
    [Fact]
    public async Task BulkMergeAsync_Upserts_By_Configured_Key()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.Entities.Add(new TestEntity
            {
                Id = Guid.NewGuid(),
                ImportKey = "existing",
                Name = "before",
                ExternalNumber = "001"
            });
            await setupContext.SaveChangesAsync();
        }

        await using (var mergeContext = CreateDbContext(connection))
        {
            var payload = new[]
            {
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    ImportKey = "existing",
                    Name = "after",
                    ExternalNumber = "001-updated"
                },
                new TestEntity
                {
                    Id = Guid.NewGuid(),
                    ImportKey = "new",
                    Name = "created",
                    ExternalNumber = "002"
                }
            };

            await mergeContext.BulkMergeAsync(payload, options => options.KeyProperties.Add("ImportKey"));
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.Entities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(2, entities.Count);
        Assert.Equal("after", entities[0].Name);
        Assert.Equal("001-updated", entities[0].ExternalNumber);
        Assert.Equal("created", entities[1].Name);
    }

    private static TestDbContext CreateDbContext(SqliteConnection connection)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(connection)
            .Options;

        return new TestDbContext(options);
    }

    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ApplyConfiguration(new TestEntityConfiguration());
        }
    }

    private sealed class TestEntity
    {
        public Guid Id { get; set; }

        public string ImportKey { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        public string ExternalNumber { get; set; } = string.Empty;
    }

    private sealed class TestEntityConfiguration : IEntityTypeConfiguration<TestEntity>
    {
        public void Configure(Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<TestEntity> builder)
        {
            builder.ToTable("TestEntities");
            builder.HasKey(entity => entity.Id);
            builder.HasIndex(entity => entity.ImportKey).IsUnique();
            builder.Property(entity => entity.ImportKey).HasMaxLength(255);
            builder.Property(entity => entity.Name).HasMaxLength(255);
            builder.Property(entity => entity.ExternalNumber).HasMaxLength(255);
        }
    }
}
