using Batchflow.EntityFrameworkExtensions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Tests;

public class DbContextBulkExtensionsTests
{
    [Fact]
    public async Task BulkInsertAsync_Inserts_New_Rows_Without_Configured_Keys()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        var payload = new[]
        {
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "a", Name = "alpha", ExternalNumber = "001" },
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "b", Name = "beta", ExternalNumber = "002" }
        };

        await using (var insertContext = CreateDbContext(connection))
        {
            await insertContext.BulkInsertAsync(payload);
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(["a", "b"], entities.Select(entity => entity.ImportKey).ToArray());
        Assert.Equal(["alpha", "beta"], entities.Select(entity => entity.Name).ToArray());
    }

    [Fact]
    public async Task BulkMergeAsync_Upserts_By_Configured_Key()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.FlatEntities.Add(new FlatTestEntity
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
                new FlatTestEntity
                {
                    Id = Guid.NewGuid(),
                    ImportKey = "existing",
                    Name = "after",
                    ExternalNumber = "001-updated"
                },
                new FlatTestEntity
                {
                    Id = Guid.NewGuid(),
                    ImportKey = "new",
                    Name = "created",
                    ExternalNumber = "002"
                }
            };

            await mergeContext.BulkMergeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(2, entities.Count);
        Assert.Equal("after", entities[0].Name);
        Assert.Equal("001-updated", entities[0].ExternalNumber);
        Assert.Equal("created", entities[1].Name);
    }

    [Fact]
    public async Task BulkMergeAsync_Respects_IgnoredPropertiesOnUpdate()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.FlatEntities.Add(new FlatTestEntity
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
                new FlatTestEntity
                {
                    Id = Guid.NewGuid(),
                    ImportKey = "existing",
                    Name = "after",
                    ExternalNumber = "should-not-change"
                }
            };

            await mergeContext.BulkMergeAsync(
                payload,
                options =>
                {
                    options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey));
                    options.IgnoredPropertiesOnUpdate.Add(nameof(FlatTestEntity.ExternalNumber));
                });
        }

        await using var assertionContext = CreateDbContext(connection);
        var entity = await assertionContext.FlatEntities.SingleAsync();

        Assert.Equal("after", entity.Name);
        Assert.Equal("001", entity.ExternalNumber);
    }

    [Fact]
    public async Task BulkMergeAsync_Generates_Guid_Key_For_New_Entities()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        var payload = new[]
        {
            new FlatTestEntity
            {
                Id = Guid.Empty,
                ImportKey = "generated",
                Name = "created",
                ExternalNumber = "001"
            }
        };

        await using (var mergeContext = CreateDbContext(connection))
        {
            await mergeContext.BulkMergeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        var entity = await assertionContext.FlatEntities.SingleAsync();

        Assert.NotEqual(Guid.Empty, payload[0].Id);
        Assert.Equal(payload[0].Id, entity.Id);
    }

    [Fact]
    public async Task BulkInsertAsync_Splits_Across_Multiple_Batches_When_Parameter_Limit_Is_Exceeded()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        var payload = CreateFlatEntities(0, 400, "inserted");

        await using (var insertContext = CreateDbContext(connection))
        {
            await insertContext.BulkInsertAsync(payload);
        }

        await using var assertionContext = CreateDbContext(connection);
        Assert.Equal(400, await assertionContext.FlatEntities.CountAsync());
    }

    [Fact]
    public async Task BulkInsertAsync_Rolls_Back_All_Batches_When_A_Later_Batch_Fails()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        var payload = new[]
        {
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "valid", Name = "valid", ExternalNumber = "001" },
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "invalid", Name = null!, ExternalNumber = "002" }
        };

        await using (var insertContext = CreateDbContext(connection))
        {
            await Assert.ThrowsAsync<SqliteException>(() =>
                insertContext.BulkInsertAsync(
                    payload,
                    options => options.BatchSize = 1));
        }

        await using var assertionContext = CreateDbContext(connection);
        Assert.Empty(await assertionContext.FlatEntities.ToListAsync());
    }

    [Fact]
    public async Task BulkMergeAsync_Splits_Across_Multiple_Batches_When_Parameter_Limit_Is_Exceeded()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            await setupContext.FlatEntities.AddRangeAsync(CreateFlatEntities(0, 200, "before"));
            await setupContext.SaveChangesAsync();
        }

        var payload = CreateFlatEntities(0, 400, "after");

        await using (var mergeContext = CreateDbContext(connection))
        {
            await mergeContext.BulkMergeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(400, entities.Count);
        Assert.Equal("after-0", entities[0].Name);
        Assert.Equal("after-399", entities[^1].Name);
    }

    [Fact]
    public async Task BulkMergeAsync_Throws_When_Key_Does_Not_Map_To_Unique_Constraint()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        await using var mergeContext = CreateDbContext(connection);

        var payload = new[]
        {
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "a", Name = "alpha", ExternalNumber = "001" }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mergeContext.BulkMergeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.Name))));

        Assert.Contains("unique", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BulkMergeAsync_Throws_When_SourcePayload_Contains_Duplicate_Keys()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        await using var mergeContext = CreateDbContext(connection);

        var payload = new[]
        {
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "dup", Name = "first", ExternalNumber = "001" },
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "dup", Name = "second", ExternalNumber = "002" }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mergeContext.BulkMergeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey))));

        Assert.Contains("duplicate source key", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BulkMergeAsync_Throws_When_Duplicate_Keys_Span_Multiple_Batches()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        await using var mergeContext = CreateDbContext(connection);

        var payload = new[]
        {
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "dup", Name = "first", ExternalNumber = "001" },
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "unique", Name = "second", ExternalNumber = "002" },
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "dup", Name = "third", ExternalNumber = "003" }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mergeContext.BulkMergeAsync(
                payload,
                options =>
                {
                    options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey));
                    options.BatchSize = 1;
                }));

        Assert.Contains("duplicate source key", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BulkMergeAsync_Throws_When_SourcePayload_Contains_Null_Key_Value()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        await using var mergeContext = CreateDbContext(connection);

        var payload = new[]
        {
            new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = null!, Name = "alpha", ExternalNumber = "001" }
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            mergeContext.BulkMergeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey))));

        Assert.Contains("null key value", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BulkSynchronizeAsync_Deletes_Rows_Missing_From_Full_Snapshot()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.FlatEntities.AddRange(
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "a", Name = "before-a", ExternalNumber = "001" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "b", Name = "before-b", ExternalNumber = "002" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "c", Name = "before-c", ExternalNumber = "003" });
            await setupContext.SaveChangesAsync();
        }

        await using (var synchronizeContext = CreateDbContext(connection))
        {
            var payload = new[]
            {
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "a", Name = "after-a", ExternalNumber = "001-updated" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "c", Name = "after-c", ExternalNumber = "003-updated" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "d", Name = "created-d", ExternalNumber = "004" }
            };

            await synchronizeContext.BulkSynchronizeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(["a", "c", "d"], entities.Select(entity => entity.ImportKey).ToArray());
        Assert.Equal("after-a", entities[0].Name);
        Assert.Equal("after-c", entities[1].Name);
        Assert.Equal("created-d", entities[2].Name);
    }

    [Fact]
    public async Task BulkSynchronizeAsync_Deletes_Rows_Missing_Within_Configured_Scope()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.ScopedEntities.AddRange(
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "1", Name = "alpha-one" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "2", Name = "alpha-two" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "beta", ImportKey = "1", Name = "beta-one" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "beta", ImportKey = "2", Name = "beta-two" });
            await setupContext.SaveChangesAsync();
        }

        await using (var synchronizeContext = CreateDbContext(connection))
        {
            var payload = new[]
            {
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "1", Name = "alpha-one-updated" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "3", Name = "alpha-three" }
            };

            await synchronizeContext.BulkSynchronizeAsync(
                payload,
                options =>
                {
                    options.KeyProperties.Add(nameof(ScopedTestEntity.Section));
                    options.KeyProperties.Add(nameof(ScopedTestEntity.ImportKey));
                    options.ScopeProperties.Add(nameof(ScopedTestEntity.Section));
                });
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.ScopedEntities
            .OrderBy(entity => entity.Section)
            .ThenBy(entity => entity.ImportKey)
            .ToListAsync();

        Assert.Equal(
            ["alpha:1", "alpha:3", "beta:1", "beta:2"],
            entities.Select(entity => $"{entity.Section}:{entity.ImportKey}").ToArray());
        Assert.Equal("alpha-one-updated", entities.Single(entity => entity.Section == "alpha" && entity.ImportKey == "1").Name);
    }

    [Fact]
    public async Task BulkSynchronizeAsync_Deletes_All_Rows_For_Empty_Full_Snapshot()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.FlatEntities.AddRange(
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "a", Name = "before-a", ExternalNumber = "001" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "b", Name = "before-b", ExternalNumber = "002" });
            await setupContext.SaveChangesAsync();
        }

        await using (var synchronizeContext = CreateDbContext(connection))
        {
            await synchronizeContext.BulkSynchronizeAsync(Array.Empty<FlatTestEntity>(), options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        Assert.Empty(await assertionContext.FlatEntities.ToListAsync());
    }

    [Fact]
    public async Task BulkSynchronizeAsync_Splits_Merge_And_DeleteMissing_When_Parameter_Limit_Is_Exceeded()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            await setupContext.FlatEntities.AddRangeAsync(CreateFlatEntities(0, 1_100, "before"));
            await setupContext.SaveChangesAsync();
        }

        var payload = CreateFlatEntities(0, 1_000, "after");

        await using (var synchronizeContext = CreateDbContext(connection))
        {
            await synchronizeContext.BulkSynchronizeAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(1_000, entities.Count);
        Assert.DoesNotContain(entities, entity => entity.ImportKey == "key-1000");
        Assert.Equal("after-999", entities[^1].Name);
    }

    [Fact]
    public async Task BulkSynchronizeAsync_Throws_For_Empty_Scoped_Snapshot()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        await using var synchronizeContext = CreateDbContext(connection);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            synchronizeContext.BulkSynchronizeAsync(
                Array.Empty<ScopedTestEntity>(),
                options =>
                {
                    options.KeyProperties.Add(nameof(ScopedTestEntity.Section));
                    options.KeyProperties.Add(nameof(ScopedTestEntity.ImportKey));
                    options.ScopeProperties.Add(nameof(ScopedTestEntity.Section));
                }));

        Assert.Contains("empty payload", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BulkDeleteByKeyAsync_Deletes_Rows_By_Configured_Key()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.FlatEntities.AddRange(
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "a", Name = "alpha", ExternalNumber = "001" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "b", Name = "beta", ExternalNumber = "002" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "c", Name = "gamma", ExternalNumber = "003" });
            await setupContext.SaveChangesAsync();
        }

        await using (var deleteContext = CreateDbContext(connection))
        {
            var payload = new[]
            {
                new FlatTestEntity { ImportKey = "a" },
                new FlatTestEntity { ImportKey = "c" }
            };

            await deleteContext.BulkDeleteByKeyAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(["b"], entities.Select(entity => entity.ImportKey).ToArray());
    }

    [Fact]
    public async Task BulkDeleteByKeyAsync_Deletes_Rows_By_Composite_Key()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            setupContext.ScopedEntities.AddRange(
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "1", Name = "alpha-one" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "2", Name = "alpha-two" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "beta", ImportKey = "1", Name = "beta-one" });
            await setupContext.SaveChangesAsync();
        }

        await using (var deleteContext = CreateDbContext(connection))
        {
            var payload = new[]
            {
                new ScopedTestEntity { Section = "alpha", ImportKey = "2" },
                new ScopedTestEntity { Section = "beta", ImportKey = "1" }
            };

            await deleteContext.BulkDeleteByKeyAsync(
                payload,
                options =>
                {
                    options.KeyProperties.Add(nameof(ScopedTestEntity.Section));
                    options.KeyProperties.Add(nameof(ScopedTestEntity.ImportKey));
                });
        }

        await using var assertionContext = CreateDbContext(connection);
        var entities = await assertionContext.ScopedEntities
            .OrderBy(entity => entity.Section)
            .ThenBy(entity => entity.ImportKey)
            .ToListAsync();

        Assert.Equal(["alpha:1"], entities.Select(entity => $"{entity.Section}:{entity.ImportKey}").ToArray());
    }

    [Fact]
    public async Task BulkDeleteByKeyAsync_Splits_Across_Multiple_Batches_When_Parameter_Limit_Is_Exceeded()
    {
        await using var connection = await OpenConnectionAsync();

        await using (var setupContext = CreateDbContext(connection))
        {
            await setupContext.Database.EnsureCreatedAsync();
            await setupContext.FlatEntities.AddRangeAsync(CreateFlatEntities(0, 1_200, "before"));
            await setupContext.SaveChangesAsync();
        }

        var payload = Enumerable.Range(0, 1_050)
            .Select(index => new FlatTestEntity { ImportKey = $"key-{index:0000}" })
            .ToArray();

        await using (var deleteContext = CreateDbContext(connection))
        {
            await deleteContext.BulkDeleteByKeyAsync(payload, options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(connection);
        var remainingKeys = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).Select(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(150, remainingKeys.Count);
        Assert.Equal("key-1050", remainingKeys[0]);
        Assert.Equal("key-1199", remainingKeys[^1]);
    }

    private static async Task<SqliteConnection> OpenConnectionAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        return connection;
    }

    private static TestDbContext CreateDbContext(SqliteConnection connection)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(connection)
            .Options;

        return new TestDbContext(options);
    }

    private static FlatTestEntity[] CreateFlatEntities(int startInclusive, int count, string namePrefix)
    {
        return Enumerable.Range(startInclusive, count)
            .Select(index => new FlatTestEntity
            {
                Id = Guid.NewGuid(),
                ImportKey = $"key-{index:0000}",
                Name = $"{namePrefix}-{index}",
                ExternalNumber = $"ext-{index:0000}"
            })
            .ToArray();
    }

    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
    {
        public DbSet<FlatTestEntity> FlatEntities => Set<FlatTestEntity>();

        public DbSet<ScopedTestEntity> ScopedEntities => Set<ScopedTestEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<FlatTestEntity>(entity =>
            {
                entity.ToTable("FlatEntities");
                entity.HasKey(item => item.Id);
                entity.HasIndex(item => item.ImportKey).IsUnique();
                entity.Property(item => item.ImportKey).HasMaxLength(255);
                entity.Property(item => item.Name).HasMaxLength(255);
                entity.Property(item => item.ExternalNumber).HasMaxLength(255);
            });

            modelBuilder.Entity<ScopedTestEntity>(entity =>
            {
                entity.ToTable("ScopedEntities");
                entity.HasKey(item => item.Id);
                entity.HasIndex(item => new { item.Section, item.ImportKey }).IsUnique();
                entity.Property(item => item.Section).HasMaxLength(255);
                entity.Property(item => item.ImportKey).HasMaxLength(255);
                entity.Property(item => item.Name).HasMaxLength(255);
            });
        }
    }

    private sealed class FlatTestEntity
    {
        public Guid Id { get; set; }

        public string ImportKey { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        public string ExternalNumber { get; set; } = string.Empty;
    }

    private sealed class ScopedTestEntity
    {
        public Guid Id { get; set; }

        public string Section { get; set; } = string.Empty;

        public string ImportKey { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;
    }
}
