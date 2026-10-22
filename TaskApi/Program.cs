using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using TaskApi;

var builder = WebApplication.CreateBuilder(args);

#region Services
builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.Converters.Add(new JsonStringEnumConverter(allowIntegerValues: false)));

builder.Services.AddDbContext<TaskDbContext>((sp, o) =>
    o.UseNpgsql(sp.GetRequiredService<IConfiguration>().GetConnectionString("Postgres")));

builder.Services.AddSingleton(TimeProvider.System);

builder.Services.AddOptions<RabbitMqOptions>().BindConfiguration(RabbitMqOptions.SectionName);
builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddSingleton<IEventPublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RabbitMqPublisher>());

builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(15));

builder.Services.AddProblemDetails();
#endregion

var app = builder.Build();

app.UseExceptionHandler(); // unhandled exception => 500 application/problem+json

#region Database init

// HACK: EnsureCreated instead of migrations
// I would complete it like this: `dotnet ef migrations add Initial` and then `await db.Database.MigrateAsync()`
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
    await db.Database.EnsureCreatedAsync();
}

#endregion

#region Endpoints

// POST /tasks
app.MapPost("/tasks", async (CreateTaskRequest request, TaskDbContext db, TimeProvider clock, CancellationToken ct) =>
{
    var errors = request.Validate();
    if (errors.Count > 0)
        return Results.ValidationProblem(errors); 

    var task = new TaskItem
    {
        Id = Guid.NewGuid(),
        Title = request.Title!.Trim(),
        Priority = request.Priority ?? Priority.Medium,
        IsCompleted = false,
        CreatedAt = clock.GetUtcNow()
    };

    db.Tasks.Add(task);
    await db.SaveChangesAsync(ct);

    return Results.Json(TaskResponse.From(task), statusCode: StatusCodes.Status201Created);
});

// GET /tasks
// HACK: no pagination according to technical specifications. I would complete it: ?page=&pageSize= + keyset by (CreatedAt, Id)
app.MapGet("/tasks", async (TaskDbContext db, CancellationToken ct) =>
{
    var tasks = await db.Tasks
        .AsNoTracking()
        .OrderBy(t => t.CreatedAt)
        .Select(t => new TaskResponse(t.Id, t.Title, t.IsCompleted, t.CreatedAt, t.CompletedAt, t.Priority))
        .ToListAsync(ct);

    return Results.Ok(tasks);
});

// PUT /tasks/{id}/complete
app.MapPut("/tasks/{id:guid}/complete", async (
    Guid id, TaskDbContext db, IEventPublisher publisher, TimeProvider clock, CancellationToken ct) =>
{
    var task = await db.Tasks.FirstOrDefaultAsync(t => t.Id == id, ct);
    if (task is null)
        return Results.NotFound();

    if (task.IsCompleted)
        return Results.Conflict(new { error = "Task is already completed." });

    task.Complete(clock.GetUtcNow());

    try
    {
        await db.SaveChangesAsync(ct);
    }
    catch (DbUpdateConcurrencyException)
    {
        return Results.Conflict(new { error = "Task was modified or completed concurrently." });
    }

    publisher.TryPublishTaskCompleted(new TaskCompletedEvent(
        task.Id, task.Title, task.CompletedAt!.Value.UtcDateTime, task.Priority));

    return Results.Ok(TaskResponse.From(task));
});

// DELETE /tasks/{id}
app.MapDelete("/tasks/{id:guid}", async (Guid id, TaskDbContext db, CancellationToken ct) =>
{
    var deleted = await db.Tasks.Where(t => t.Id == id).ExecuteDeleteAsync(ct);
    return deleted == 0 ? Results.NotFound() : Results.NoContent();
});

#endregion

app.Run();

public partial class Program;
