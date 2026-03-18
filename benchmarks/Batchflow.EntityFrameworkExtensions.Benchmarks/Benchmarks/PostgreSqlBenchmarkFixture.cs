using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

internal sealed class PostgreSqlBenchmarkFixture : IAsyncDisposable
{
    private readonly PostgreSqlBenchmarkDatabase _database;

    private PostgreSqlBenchmarkFixture(PostgreSqlBenchmarkDatabase database)
    {
        _database = database;
    }

    public static async Task<PostgreSqlBenchmarkFixture> CreateAsync()
    {
        var database = await PostgreSqlBenchmarkDatabase.CreateAsync();
        return new PostgreSqlBenchmarkFixture(database);
    }

    public BulkBenchmarkDbContext CreateDbContext()
    {
        var options = new DbContextOptionsBuilder<BulkBenchmarkDbContext>()
            .UseNpgsql(_database.ConnectionString)
            .EnableDetailedErrors(false)
            .EnableSensitiveDataLogging(false)
            .Options;

        return new BulkBenchmarkDbContext(options);
    }

    public async Task ResetDatabaseAsync()
    {
        await using var dbContext = CreateDbContext();
        await dbContext.Database.EnsureDeletedAsync();
        await dbContext.Database.EnsureCreatedAsync();
    }

    public async Task SeedExistingRowsAsync(int batchSize)
    {
        await using var dbContext = CreateDbContext();
        await dbContext.Entities.AddRangeAsync(BenchmarkDataFactory.CreateSeedBatch(batchSize));
        await dbContext.SaveChangesAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
    }
}

internal sealed class PostgreSqlBenchmarkDatabase : IAsyncDisposable
{
    private readonly string _adminConnectionString;

    private PostgreSqlBenchmarkDatabase(string adminConnectionString, string connectionString, string databaseName)
    {
        _adminConnectionString = adminConnectionString;
        ConnectionString = connectionString;
        DatabaseName = databaseName;
    }

    public string ConnectionString { get; }

    public string DatabaseName { get; }

    public static async Task<PostgreSqlBenchmarkDatabase> CreateAsync()
    {
        var baseConnectionString = ResolveConnectionString();
        var databaseName = $"bfefext_bench_{Guid.NewGuid():N}";
        var baseBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString);

        var appConnectionBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Database = databaseName
        };

        var adminConnectionBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Database = string.IsNullOrWhiteSpace(baseBuilder.Database) ? "postgres" : baseBuilder.Database
        };

        await using var connection = new NpgsqlConnection(adminConnectionBuilder.ConnectionString);
        await connection.OpenAsync();

        await using (var command = connection.CreateCommand())
        {
            command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
            await command.ExecuteNonQueryAsync();
        }

        return new PostgreSqlBenchmarkDatabase(adminConnectionBuilder.ConnectionString, appConnectionBuilder.ConnectionString, databaseName);
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

    private static string ResolveConnectionString()
    {
        var environmentConnectionString = Environment.GetEnvironmentVariable("BATCHFLOW_EFEXT_POSTGRES_CONNECTION");
        if (!string.IsNullOrWhiteSpace(environmentConnectionString))
        {
            return environmentConnectionString;
        }

        var appSettingsPath = TryFindDevelopmentAppSettingsPath();
        if (appSettingsPath is null)
        {
            throw new InvalidOperationException(
                "No PostgreSQL benchmark connection string was found. Set BATCHFLOW_EFEXT_POSTGRES_CONNECTION or ensure BatchFlow.Backend/BatchFlow.API/appsettings.Development.json exists.");
        }

        using var document = JsonDocument.Parse(File.ReadAllText(appSettingsPath));
        if (!document.RootElement.TryGetProperty("ConnectionStrings", out var connectionStrings) ||
            !connectionStrings.TryGetProperty("MasterConnection", out var connectionStringElement) ||
            string.IsNullOrWhiteSpace(connectionStringElement.GetString()))
        {
            throw new InvalidOperationException("PostgreSQL benchmark connection string could not be resolved from appsettings.Development.json.");
        }

        return connectionStringElement.GetString()!;
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
