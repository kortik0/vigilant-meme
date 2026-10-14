using Microsoft.EntityFrameworkCore;
using PromoOS.Api.Domain;

namespace PromoOS.Api.Data;

public class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TaskItem>(entity =>
        {
            entity.HasKey(t => t.Id);

            entity.Property(t => t.Title)
                .IsRequired()
                .HasMaxLength(200);

            entity.Property(t => t.Priority)
                .HasConversion<string>();

            // TODO: always NULL on PostgreSQL. Would drop this column in a new migration.
            entity.Property(t => t.RowVersion).IsRowVersion();
            
            entity.Property(t => t.IsCompleted).IsConcurrencyToken();
        });
    }
}