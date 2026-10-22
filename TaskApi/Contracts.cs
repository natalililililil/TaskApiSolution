namespace TaskApi;

#region Requests

public record CreateTaskRequest(string? Title, Priority? Priority)
{
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();

        var title = Title?.Trim();
        if (string.IsNullOrEmpty(title))
            errors["title"] = ["Title is required and must not be empty or whitespace."];
        else if (title.Length > TaskItem.TitleMaxLength)
            errors["title"] = [$"Title must be at most {TaskItem.TitleMaxLength} characters."];

        if (Priority is { } p && !Enum.IsDefined(p))
            errors["priority"] = ["Priority must be one of: Low, Medium, High."];

        return errors;
    }
}

#endregion

#region Responses

public record TaskResponse(
    Guid Id,
    string Title,
    bool IsCompleted,
    DateTimeOffset CreatedAt,
    DateTimeOffset? CompletedAt,
    Priority Priority)
{
    public static TaskResponse From(TaskItem t) =>
        new(t.Id, t.Title, t.IsCompleted, t.CreatedAt, t.CompletedAt, t.Priority);
}

#endregion
