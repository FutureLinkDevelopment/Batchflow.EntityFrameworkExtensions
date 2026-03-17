using Microsoft.EntityFrameworkCore;

namespace Batchflow.EntityFrameworkExtensions.Benchmarks.Benchmarks;

internal sealed class BulkBenchmarkDbContext(DbContextOptions<BulkBenchmarkDbContext> options) : DbContext(options)
{
    public DbSet<BulkEntity> Entities => Set<BulkEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BulkEntity>(entity =>
        {
            entity.ToTable("BenchmarkEntities");
            entity.HasKey(x => x.Id);
            entity.HasIndex(x => x.ImportKey).IsUnique();
            entity.Property(x => x.ImportKey).HasMaxLength(255);
            entity.Property(x => x.ExternalNumber).HasMaxLength(255);
            entity.Property(x => x.Name).HasMaxLength(255);
        });
    }
}
