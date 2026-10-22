using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace TaskApi;

#region Options & contracts

public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public string Uri { get; set; } = "amqp://guest:guest@localhost:5672/";
    public string Exchange { get; set; } = "task.events";
    public int ShutdownDrainTimeoutSeconds { get; set; } = 5;
}

public record TaskCompletedEvent(Guid TaskId, string Title, DateTime CompletedAt, Priority Priority);

public interface IEventPublisher
{
    bool TryPublishTaskCompleted(TaskCompletedEvent evt);
}

#endregion

// HACK: queue in memory => if the process crashes, messages are lost. That's right - transactional outbox
public sealed class RabbitMqPublisher : BackgroundService, IEventPublisher
{
    public const string TaskCompletedRoutingKey = "task.completed";

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly record struct Outgoing(string RoutingKey, byte[] Body);

    private readonly Channel<Outgoing> _queue = Channel.CreateBounded<Outgoing>(
        new BoundedChannelOptions(1000) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });

    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly CancellationTokenSource _abortCts = new();

    private IConnection? _connection;
    private IChannel? _channel;

    public RabbitMqPublisher(IOptions<RabbitMqOptions> options, ILogger<RabbitMqPublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    #region Producer side (вызывается из HTTP-хендлеров)

    public bool TryPublishTaskCompleted(TaskCompletedEvent evt)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(evt, Json);

        if (_queue.Writer.TryWrite(new Outgoing(TaskCompletedRoutingKey, body)))
            return true;

        _logger.LogError("RabbitMQ queue is full or closed, message dropped. RoutingKey={RoutingKey} Body={Body}",
            TaskCompletedRoutingKey, Encoding.UTF8.GetString(body));
        return false;
    }

    #endregion

    #region Consumer side (фоновый цикл)

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try { await GetChannelAsync(_abortCts.Token); }
        catch (Exception ex) { _logger.LogWarning(ex, "RabbitMQ is unavailable on startup, will retry on first publish."); }

        // We do NOT intentionally use stoppingToken: when shutdown, the loop must read the queue to the end. 
        // Stopping occurs via Writer.TryComplete() in StopAsync.
        await foreach (var msg in _queue.Reader.ReadAllAsync(CancellationToken.None))
        {
            await PublishAsync(msg, _abortCts.Token);
        }
    }

    private async Task PublishAsync(Outgoing msg, CancellationToken ct)
    {
        for (var attempt = 1; attempt <= 2; attempt++)
        {
            try
            {
                var channel = await GetChannelAsync(ct);
                var props = new BasicProperties
                {
                    ContentType = "application/json",
                    DeliveryMode = DeliveryModes.Persistent
                };

                // HACK: without publisher confirms - “sent to the socket”, but not “received by the broker” 
                // Would complete: CreateChannelOptions(publisherConfirmationsEnabled: true, tracking: true)
                await channel.BasicPublishAsync(_options.Exchange, msg.RoutingKey, mandatory: false,
                    basicProperties: props, body: msg.Body, cancellationToken: ct);
                return;
            }
            catch (Exception ex)
            {
                await DisposeConnectionAsync(CancellationToken.None);

                if (attempt == 2 || ct.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Failed to publish to RabbitMQ, message lost. RoutingKey={RoutingKey} Body={Body}",
                        msg.RoutingKey, Encoding.UTF8.GetString(msg.Body));
                    return;
                }
            }
        }
    }

    private async Task<IChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true })
            return _channel;

        await DisposeConnectionAsync(CancellationToken.None);

        var factory = new ConnectionFactory
        {
            Uri = new Uri(_options.Uri),
            AutomaticRecoveryEnabled = false,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(3)
        };

        _connection = await factory.CreateConnectionAsync(ct);
        _channel = await _connection.CreateChannelAsync(cancellationToken: ct);
        await _channel.ExchangeDeclareAsync(_options.Exchange, ExchangeType.Topic, durable: true, cancellationToken: ct);

        _logger.LogInformation("Connected to RabbitMQ, exchange '{Exchange}' declared.", _options.Exchange);
        return _channel;
    }

    #endregion

    #region Graceful shutdown

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _queue.Writer.TryComplete();

        try
        {
            if (ExecuteTask is { } task)
                await task.WaitAsync(TimeSpan.FromSeconds(_options.ShutdownDrainTimeoutSeconds), cancellationToken);
        }
        catch (Exception ex) when (ex is TimeoutException or OperationCanceledException)
        {
            _logger.LogWarning("RabbitMQ drain timeout, aborting pending publishes (they are logged as lost).");
            _abortCts.Cancel();
        }

        await base.StopAsync(cancellationToken);

        await DisposeConnectionAsync(CancellationToken.None);
        _logger.LogInformation("RabbitMQ channel and connection closed.");
    }

    private async Task DisposeConnectionAsync(CancellationToken _)
    {
        var channel = _channel;
        var connection = _connection;
        _channel = null;
        _connection = null;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try { if (channel is { IsOpen: true }) await channel.CloseAsync(cts.Token); } catch (Exception ex) { _logger.LogDebug(ex, "Channel close failed."); }
        try { if (connection is { IsOpen: true }) await connection.CloseAsync(cts.Token); } catch (Exception ex) { _logger.LogDebug(ex, "Connection close failed."); }
        try { channel?.Dispose(); } catch { /* ignore */ }
        try { connection?.Dispose(); } catch { /* ignore */ }
    }

    public override void Dispose()
    {
        _abortCts.Dispose();
        base.Dispose();
    }

    #endregion
}
