using System.Text.Json;
using Batchflow.EntityFrameworkExtensions;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Batchflow.EntityFrameworkExtensions.Tests;

public class PostgreSqlBulkOperationsTests
{
    [Fact]
    [Trait("Category", "PostgreSqlIntegration")]
    public async Task BulkOperations_EndToEnd_Work_On_PostgreSql()
    {
        await using var database = await PostgreSqlTestDatabase.TryCreateAsync();
        if (database is null)
        {
            return;
        }

        await using (var setupContext = CreateDbContext(database.ConnectionString))
        {
            await setupContext.Database.EnsureCreatedAsync();

            setupContext.FlatEntities.AddRange(
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "existing", Name = "before", ExternalNumber = "001" },
                new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "remove-me", Name = "to-remove", ExternalNumber = "002" });

            setupContext.ScopedEntities.AddRange(
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "1", Name = "alpha-one" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "2", Name = "alpha-two" },
                new ScopedTestEntity { Id = Guid.NewGuid(), Section = "beta", ImportKey = "1", Name = "beta-one" });

            await setupContext.SaveChangesAsync();
        }

        await using (var operationContext = CreateDbContext(database.ConnectionString))
        {
            await operationContext.BulkInsertAsync(
                new[]
                {
                    new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "inserted", Name = "created", ExternalNumber = "003" }
                });

            await operationContext.BulkMergeAsync(
                new[]
                {
                    new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "existing", Name = "after", ExternalNumber = "001-updated" },
                    new FlatTestEntity { Id = Guid.NewGuid(), ImportKey = "merged", Name = "merged-new", ExternalNumber = "004" }
                },
                options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));

            await operationContext.BulkSynchronizeAsync(
                new[]
                {
                    new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "1", Name = "alpha-one-updated" },
                    new ScopedTestEntity { Id = Guid.NewGuid(), Section = "alpha", ImportKey = "3", Name = "alpha-three" }
                },
                options =>
                {
                    options.KeyProperties.Add(nameof(ScopedTestEntity.Section));
                    options.KeyProperties.Add(nameof(ScopedTestEntity.ImportKey));
                    options.ScopeProperties.Add(nameof(ScopedTestEntity.Section));
                });

            await operationContext.BulkDeleteByKeyAsync(
                new[]
                {
                    new FlatTestEntity { ImportKey = "remove-me" }
                },
                options => options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey)));
        }

        await using var assertionContext = CreateDbContext(database.ConnectionString);

        var flatEntities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();
        var scopedEntities = await assertionContext.ScopedEntities
            .OrderBy(entity => entity.Section)
            .ThenBy(entity => entity.ImportKey)
            .ToListAsync();

        Assert.Equal(["existing", "inserted", "merged"], flatEntities.Select(entity => entity.ImportKey).ToArray());
        Assert.Equal("after", flatEntities.Single(entity => entity.ImportKey == "existing").Name);
        Assert.Equal("001-updated", flatEntities.Single(entity => entity.ImportKey == "existing").ExternalNumber);

        Assert.Equal(
            ["alpha:1", "alpha:3", "beta:1"],
            scopedEntities.Select(entity => $"{entity.Section}:{entity.ImportKey}").ToArray());
        Assert.Equal("alpha-one-updated", scopedEntities.Single(entity => entity.Section == "alpha" && entity.ImportKey == "1").Name);
    }

    [Fact]
    [Trait("Category", "PostgreSqlIntegration")]
    public async Task BulkOperations_Handle_Large_PostgreSql_Payloads()
    {
        await using var database = await PostgreSqlTestDatabase.TryCreateAsync();
        if (database is null)
        {
            return;
        }

        await using (var setupContext = CreateDbContext(database.ConnectionString))
        {
            await setupContext.Database.EnsureCreatedAsync();
        }

        var insertPayload = CreateFlatEntities(0, 1_200, "inserted");

        await using (var insertContext = CreateDbContext(database.ConnectionString))
        {
            await insertContext.BulkInsertAsync(insertPayload);
        }

        var mergePayload = CreateFlatEntities(0, 1_400, "merged");

        await using (var mergeContext = CreateDbContext(database.ConnectionString))
        {
            await mergeContext.BulkMergeAsync(
                mergePayload,
                options =>
                {
                    options.KeyProperties.Add(nameof(FlatTestEntity.ImportKey));
                    options.BatchSize = 1;
                });
        }

        await using var assertionContext = CreateDbContext(database.ConnectionString);
        var entities = await assertionContext.FlatEntities.OrderBy(entity => entity.ImportKey).ToListAsync();

        Assert.Equal(1_400, entities.Count);
        Assert.Equal("merged-0", entities[0].Name);
        Assert.Equal("merged-1399", entities[^1].Name);
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

    private static PostgreSqlBulkTestDbContext CreateDbContext(string connectionString)
    {
        var options = new DbContextOptionsBuilder<PostgreSqlBulkTestDbContext>()
            .UseNpgsql(connectionString)
            .Options;

        return new PostgreSqlBulkTestDbContext(options);
    }

    private sealed class PostgreSqlBulkTestDbContext(DbContextOptions<PostgreSqlBulkTestDbContext> options) : DbContext(options)
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

    private sealed class PostgreSqlTestDatabase : IAsyncDisposable
    {
        private readonly string _adminConnectionString;

        private PostgreSqlTestDatabase(string adminConnectionString, string connectionString, string databaseName)
        {
            _adminConnectionString = adminConnectionString;
            ConnectionString = connectionString;
            DatabaseName = databaseName;
        }

        public string ConnectionString { get; }

        public string DatabaseName { get; }

        public static async Task<PostgreSqlTestDatabase?> TryCreateAsync()
        {
            var baseConnectionString = TryResolveConnectionString();
            if (string.IsNullOrWhiteSpace(baseConnectionString))
            {
                return null;
            }

            var databaseName = $"batchflow_efext_{Guid.NewGuid():N}";
            var appConnectionBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString)
            {
                Database = databaseName
            };

            var adminConnectionBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString)
            {
                Database = string.IsNullOrWhiteSpace(new NpgsqlConnectionStringBuilder(baseConnectionString).Database)
                    ? "postgres"
                    : new NpgsqlConnectionStringBuilder(baseConnectionString).Database
            };

            await using var connection = new NpgsqlConnection(adminConnectionBuilder.ConnectionString);
            await connection.OpenAsync();

            await using (var command = connection.CreateCommand())
            {
                command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
                await command.ExecuteNonQueryAsync();
            }

            return new PostgreSqlTestDatabase(adminConnectionBuilder.ConnectionString, appConnectionBuilder.ConnectionString, databaseName);
        }

        public async ValueTask DisposeAsync()
        {
            await using var connection = new NpgsqlConnection(_adminConnectionString);
            await connection.OpenAsync();

            await using (var terminateCommand = connection.CreateCommand())
            {
                terminateCommand.CommandText = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = @databaseName AND pid <> pg_backend_pid();";
                terminateCommand.Parameters.AddWithValue("databaseName", DatabaseName);
                await terminateCommand.ExecuteNonQueryAsync();
            }

            await using (var dropCommand = connection.CreateCommand())
            {
                dropCommand.CommandText = $"DROP DATABASE IF EXISTS \"{DatabaseName}\"";
                await dropCommand.ExecuteNonQueryAsync();
            }
        }

        private static string? TryResolveConnectionString()
        {
            var environmentConnectionString = Environment.GetEnvironmentVariable("BATCHFLOW_EFEXT_POSTGRES_CONNECTION");
            if (!string.IsNullOrWhiteSpace(environmentConnectionString))
            {
                return environmentConnectionString;
            }

            var appSettingsPath = TryFindDevelopmentAppSettingsPath();
            if (appSettingsPath is null)
            {
                return null;
            }

            using var document = JsonDocument.Parse(File.ReadAllText(appSettingsPath));
            if (!document.RootElement.TryGetProperty("ConnectionStrings", out var connectionStrings))
            {
                return null;
            }

            return connectionStrings.TryGetProperty("MasterConnection", out var connectionStringElement)
                ? connectionStringElement.GetString()
                : null;
        }

        private static string? TryFindDevelopmentAppSettingsPath()
        {
            for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
            {
                var candidate = Path.Combine(directory.FullName, "BatchFlow.Backend", "BatchFlow.API", "appsettings.Development.json");
                if (File.Exists(candidate))
                {
                    return candidate;
                }
            }

            return null;
        }
    }
}
