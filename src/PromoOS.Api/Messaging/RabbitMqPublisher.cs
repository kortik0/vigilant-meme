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

            _logger.LogInformation("RabbitMq is started, {name} ready", ExchangeName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RabbitMQ unavailable on start. Messages will be dropped silently.");
        }
    }

    public void PublishTaskCompleted(TaskCompletedMessage message)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                if (_channel is null)
                {
                    _logger.LogWarning("RabbitMQ channel not available. Dropping message for {TaskId}.",
                        message.TaskId);
                    return;
                }

                ReadOnlyMemory<byte> body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, _jsonOptions));

                await _channel.BasicPublishAsync(
                    exchange: ExchangeName,
                    routingKey: RoutingKey,
                    body: body);

                _logger.LogInformation("Published task.completed for task {TaskId}.", message.TaskId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish task.completed for task {TaskId}.",
                    message.TaskId);
            }
        });
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
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

            _logger.LogInformation("RabbitMQ connection closed gracefully.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RabbitMQ did not shut down cleanly within the timeout.");
        }
    }
}