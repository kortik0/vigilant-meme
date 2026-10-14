using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace PromoOS.Api.Messaging;

public class RabbitMqOptions
{
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string Username { get; set; } = "guest";
    public string Password { get; set; } = "guest";
}

public class RabbitMqPublisher : IRabbitMqPublisher, IHostedService
{
    private const string ExchangeName = "task.events";
    private const string RoutingKey = "task.completed";

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private IConnection? _connection;
    private IChannel? _channel;

    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private readonly ConcurrentDictionary<Guid, Task> _inFlight = new();

    public RabbitMqPublisher(IOptions<RabbitMqOptions> options, ILogger<RabbitMqPublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            var factory = new ConnectionFactory
            {
                HostName = _options.Host,
                Port = _options.Port,
                UserName = _options.Username,
                Password = _options.Password,
            };

            _connection = await factory.CreateConnectionAsync(cancellationToken);
            _channel = await _connection.CreateChannelAsync(cancellationToken: cancellationToken);

            await _channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
                type: ExchangeType.Topic,
                durable: true,
                autoDelete: false,
                cancellationToken: cancellationToken);

            _logger.LogInformation("Rabbit connected, exchange {name}", ExchangeName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Can't reach RabbitMQ on start, events won't be send");
        }
    }

    public void PublishTaskCompleted(TaskCompletedMessage message)
    {
        var id = Guid.NewGuid();
        var task = PublishAsync(message, id);
        _inFlight[id] = task;
    }

    private async Task PublishAsync(TaskCompletedMessage message, Guid id)
    {
        await Task.Yield();
        await _publishLock.WaitAsync();

        try
        {
            if (_channel is null)
            {
                _logger.LogWarning("No rabbit channel, skipping event for {TaskId}", message.TaskId);
                return;
            }

            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, _jsonOptions));
            await _channel.BasicPublishAsync(ExchangeName, RoutingKey, body);

            _logger.LogInformation("task.completed sent for {TaskId}", message.TaskId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Couldnt send task.completed for {TaskId}", message.TaskId);
        }
        finally
        {
            _publishLock.Release();
            _inFlight.TryRemove(id, out _);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var pending = Task.WhenAll(_inFlight.Values);
        if (await Task.WhenAny(pending, Task.Delay(TimeSpan.FromSeconds(5))) != pending)
        {
            _logger.LogWarning("Some RabbitMQ publishes did not finish within 5s and may be lost.");
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        try
        {
            if (_channel is not null)
            {
                await _channel.CloseAsync(cts.Token);
                await _channel.DisposeAsync();
            }

            if (_connection is not null)
            {
                await _connection.CloseAsync(cts.Token);
                await _connection.DisposeAsync();
            }

            _logger.LogInformation("Rabbit connection closed");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Rabbit didn't close in time, moving on");
        }
    }
}