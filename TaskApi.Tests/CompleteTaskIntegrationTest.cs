using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using Xunit;

namespace TaskApi.Tests;

#region Fixture: Real Postgres and RabbitMQ in Docker (Testcontainers) + In-Memory App
public sealed class AppFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
    private readonly RabbitMqContainer _rabbit = new RabbitMqBuilder().WithImage("rabbitmq:3.13-alpine").Build();

    public WebApplicationFactory<Program> Factory { get; private set; } = null!;
    public string RabbitUri => _rabbit.GetConnectionString();

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _rabbit.StartAsync());

        Factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("ConnectionStrings:Postgres", _postgres.GetConnectionString());
            b.UseSetting("RabbitMq:Uri", _rabbit.GetConnectionString());
        });
    }

    public async Task DisposeAsync()
    {
        await Factory.DisposeAsync();
        await _postgres.DisposeAsync();
        await _rabbit.DisposeAsync();
    }
}
#endregion

#region TestConsumer: Listens to task.events / task.completed
public sealed class TestConsumer : IAsyncDisposable
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;
    private readonly TaskCompletionSource<string> _firstMessage = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private TestConsumer(IConnection connection, IChannel channel) => (_connection, _channel) = (connection, channel);

    public Task<string> FirstMessage => _firstMessage.Task;

    public static async Task<TestConsumer> StartAsync(string uri)
    {
        var factory = new ConnectionFactory { Uri = new Uri(uri) };
        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync("task.events", ExchangeType.Topic, durable: true);
        var queue = await channel.QueueDeclareAsync();
        await channel.QueueBindAsync(queue.QueueName, "task.events", "task.completed");

        var consumer = new TestConsumer(connection, channel);
        var asyncConsumer = new AsyncEventingBasicConsumer(channel);
        asyncConsumer.ReceivedAsync += (_, ea) =>
        {
            consumer._firstMessage.TrySetResult(Encoding.UTF8.GetString(ea.Body.Span));
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer: asyncConsumer);

        return consumer;
    }

    public async ValueTask DisposeAsync()
    {
        await _channel.DisposeAsync();
        await _connection.DisposeAsync();
    }
}
#endregion

public class CompleteTaskIntegrationTest(AppFixture fixture) : IClassFixture<AppFixture>
{
    // ---------- Main test from the specifications: create -> complete -> message received in RabbitMQ ----------
    [Fact]
    public async Task Complete_task_publishes_message_to_rabbitmq()
    {
        var client = fixture.Factory.CreateClient();
        await using var consumer = await TestConsumer.StartAsync(fixture.RabbitUri);

        // 1. Create a task
        var create = await client.PostAsJsonAsync("/tasks", new { title = "Buy milk", priority = "High" });
        Assert.Equal(HttpStatusCode.Created, create.StatusCode);
        var created = await create.Content.ReadFromJsonAsync<JsonElement>();
        var id = created.GetProperty("id").GetGuid();
        Assert.False(created.GetProperty("isCompleted").GetBoolean());
        Assert.Equal(JsonValueKind.Null, created.GetProperty("completedAt").ValueKind); 

        // 2. Complete it
        var complete = await client.PutAsync($"/tasks/{id}/complete", content: null);
        Assert.Equal(HttpStatusCode.OK, complete.StatusCode);

        // 3. RabbitMQ received the message
        var body = await consumer.FirstMessage.WaitAsync(TimeSpan.FromSeconds(15));
        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        Assert.Equal(id, root.GetProperty("taskId").GetGuid());
        Assert.Equal("Buy milk", root.GetProperty("title").GetString());
        Assert.Equal("High", root.GetProperty("priority").GetString());
        Assert.EndsWith("Z", root.GetProperty("completedAt").GetString()); 
    }

    // ---------- Two concurrent completion requests: one 200, the other 409 ----------
    [Fact]
    public async Task Concurrent_complete_returns_one_200_and_one_409()
    {
        var client = fixture.Factory.CreateClient();
        var create = await client.PostAsJsonAsync("/tasks", new { title = "Race" });
        var id = (await create.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("id").GetGuid();

        var responses = await Task.WhenAll(
            client.PutAsync($"/tasks/{id}/complete", null),
            client.PutAsync($"/tasks/{id}/complete", null));

        var codes = responses.Select(r => r.StatusCode).OrderBy(c => (int)c).ToArray();
        Assert.Equal(new[] { HttpStatusCode.OK, HttpStatusCode.Conflict }, codes);
    }

    // ---------- Title validation ----------
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public async Task Create_with_blank_title_returns_400(string? title)
    {
        var client = fixture.Factory.CreateClient();
        var response = await client.PostAsJsonAsync("/tasks", new { title });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Create_with_too_long_title_returns_400()
    {
        var client = fixture.Factory.CreateClient();
        var response = await client.PostAsJsonAsync("/tasks", new { title = new string('x', 201) });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
}