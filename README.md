# TaskApi

Test assignment: C# / .NET Backend Developer (Middle).
Stack: .NET 9, ASP.NET Core Minimal API, PostgreSQL + EF Core, RabbitMQ (publish only).

## Run

Requires the .NET 9 SDK and Docker.

```bash
docker compose up -d            # Postgres :5432, RabbitMQ :5672 (UI: http://localhost:15672, guest/guest)
dotnet run --project TaskApi    # the table is created on startup
dotnet test                     # integration tests (Testcontainers, Docker must be running)
```

Manual requests for every endpoint: `TaskApi/requests.http`.

## API

| Method | Endpoint | Result |
|---|---|---|
| POST | `/tasks` | 201; 400 on invalid input. Defaults: `IsCompleted=false`, `Priority=Medium` |
| GET | `/tasks` | 200, all tasks (no pagination) |
| PUT | `/tasks/{id}/complete` | 200; 404 if not found; 409 if already completed or completed concurrently |
| DELETE | `/tasks/{id}` | 204; 404 if not found (hard delete) |

## Project layout

| File | Purpose |
|---|---|
| `Program.cs` | DI and endpoints (split by `#region`) |
| `TaskItem.cs` | entity, `Priority` enum, `Complete()` |
| `Contracts.cs` | request/response DTOs, Title validation |
| `TaskDbContext.cs` | EF mapping, RowVersion |
| `RabbitMqService.cs` | background publisher and graceful shutdown |
| `TaskApi.Tests/` | integration tests |

## Key decisions

- **CompletedAt** is set only in `TaskItem.Complete()`; it is always `null` on creation.
- **Validation**: Title is trimmed; empty/whitespace or longer than 200 chars returns 400. Enums are accepted as strings only; unknown values return 400.
- **Concurrency**: `uint RowVersion` with `IsRowVersion()`, which Npgsql maps to the `xmin` system column. A concurrent second `complete` affects 0 rows, EF throws `DbUpdateConcurrencyException`, and the API returns 409. A sequential repeat is caught by the `IsCompleted` check.
- **RabbitMQ**: the message is published only after a successful `SaveChanges`. The handler enqueues it into an in-memory `Channel<T>` and returns immediately; a `BackgroundService` publishes it to exchange `task.events` (topic, durable) with routing key `task.completed`. If the broker is down, the task is still completed and the failure is logged together with the message body.
- **Graceful shutdown**: `StopAsync` stops accepting new messages, drains the queue (5 s timeout, `RabbitMq:ShutdownDrainTimeoutSeconds`), then closes the channel and the connection.

## Tests

Testcontainers start real Postgres and RabbitMQ. Main test: create a task, complete it, and check that a `TestConsumer` bound to `task.events` / `task.completed` receives the message. Extra tests: concurrent `complete` (one 200, one 409) and Title validation.

## Verified

[Keep only what you actually checked]
- Endpoints via `requests.http`
- Message received in a RabbitMQ queue bound to `task.events`
- `dotnet test` passes

## HACK / not done, and what I would do instead

- `// HACK:` **No EF migrations.** `EnsureCreated` is used on startup. Next step: `dotnet ef migrations add Initial` and `Database.MigrateAsync()`.
- `// HACK:` **In-memory queue.** If the process crashes after `SaveChanges` and before publishing, the message is lost. Proper fix: transactional outbox (an outbox row in the same transaction plus a worker that publishes it).
- `// HACK:` **No publisher confirms.** "Published" means "written to the socket", not "accepted by the broker". Fix: create the channel with `publisherConfirmationsEnabled: true`.
- `// HACK:` **Simple retries.** Two attempts per message, no backoff. Fix: exponential backoff.
- **No pagination** in `GET /tasks`, as allowed by the assignment. Fix: keyset pagination on `(CreatedAt, Id)`.
- **No `GET /tasks/{id}`** and no `Location` header on `POST`; they were not in the assignment.