using Microsoft.EntityFrameworkCore;
using PromoOS.Api.Data;
using PromoOS.Api.Domain;
using PromoOS.Api.Messaging;
using PromoOS.Api.Requests;

namespace PromoOS.Api.Endpoints;

public static class TaskEndpoints
{
    public static void MapTaskEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/tasks").WithTags("Tasks");

        group.MapPost("/", CreateTask);
        group.MapGet("/", GetAllTasks);
        group.MapPut("/{id:guid}/complete", CompleteTask);
        group.MapDelete("/{id:guid}", DeleteTask);
    }

    private static async Task<IResult> CreateTask(
        CreateTaskRequest request,
        AppDbContext db,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.Title))
        {
            return Results.BadRequest(new { error = "Title is required." });
        }

        if (request.Title.Length > 200)
        {
            return Results.BadRequest(new { error = "Title too long." });
        }

        if (!Enum.IsDefined(request.Priority))
        {
            return Results.BadRequest(new { error = "Priority must be Low, Medium or High." });
        }

        var task = new TaskItem
        {
            Id = Guid.NewGuid(),
            Title = request.Title.Trim(),
            IsCompleted = false,
            Priority = request.Priority,
            CreatedAt = DateTimeOffset.UtcNow
        };

        db.Tasks.Add(task);
        await db.SaveChangesAsync(ct);

        return Results.Created($"/tasks/{task.Id}", task);
    }

    private static async Task<IResult> GetAllTasks(AppDbContext db, CancellationToken ct)
    {
        // TODO: pagination
        var tasks = await db.Tasks
            .OrderByDescending(t => t.CreatedAt)
            .ToListAsync(ct);

        return Results.Ok(tasks);
    }

    private static async Task<IResult> CompleteTask(
        Guid id,
        AppDbContext db,
        IRabbitMqPublisher publisher,
        CancellationToken ct)
    {
        TaskItem? task = await db.Tasks.FindAsync([id], ct);
        if (task is null)
        {
            return Results.NotFound();
        }

        if (task.IsCompleted)
        {
            return Results.Conflict(new { error = "Task already completed." });
        }

        task.IsCompleted = true;
        task.CompletedAt = DateTimeOffset.UtcNow;

        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateConcurrencyException)
        {
            return Results.Conflict(new { error = "Task was already completed." });
        }

        publisher.PublishTaskCompleted(new TaskCompletedMessage(
            TaskId: task.Id,
            Title: task.Title,
            CompletedAt: task.CompletedAt.Value,
            Priority: task.Priority.ToString()
        ));

        return Results.Ok(task);
    }

    private static async Task<IResult> DeleteTask(Guid id, AppDbContext db, CancellationToken ct)
    {
        TaskItem? task = await db.Tasks.FindAsync([id], ct);
        if (task is null)
        {
            return Results.NotFound();
        }

        db.Tasks.Remove(task);
        await db.SaveChangesAsync(ct);

        return Results.NoContent();
    }
}