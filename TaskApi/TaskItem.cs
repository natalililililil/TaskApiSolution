namespace TaskApi;

public enum Priority
{
    Low,
    Medium,
    High
}

public class TaskItem
{
    public const int TitleMaxLength = 200;

    public Guid Id { get; set; }

    public string Title { get; set; } = string.Empty;

    public bool IsCompleted { get; set; }

    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }

    public Priority Priority { get; set; } = Priority.Medium;

    public uint RowVersion { get; set; }

    public void Complete(DateTimeOffset now)
    {
        if (IsCompleted)
            throw new InvalidOperationException("Task is already completed.");

        IsCompleted = true;
        CompletedAt = now;
    }
}
