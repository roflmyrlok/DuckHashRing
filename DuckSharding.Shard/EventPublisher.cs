using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using DuckSharding.Shared.Models;

namespace DuckSharding.Shard;

public class EventPublisher : IAsyncDisposable
{
    private IConnection? _connection;
    private IChannel? _channel;
    private string? _exchangeName;
    private long _sequenceNumber;
    private readonly object _lock = new();

    public static async Task<EventPublisher> CreateAsync(IConfiguration configuration)
    {
        var publisher = new EventPublisher();
        await publisher.InitializeAsync(configuration);
        return publisher;
    }

    private EventPublisher()
    {
        _sequenceNumber = 0;
    }

    private async Task InitializeAsync(IConfiguration configuration)
    {
        var shardId = configuration["Shard:ShardId"] ?? "shard-0";
        var rabbitHost = configuration["RabbitMQ:Host"] ?? "rabbitmq";
        var rabbitPort = int.Parse(configuration["RabbitMQ:Port"] ?? "5672");

        _exchangeName = $"{shardId}-events";

        var factory = new ConnectionFactory
        {
            HostName = rabbitHost,
            Port = rabbitPort
        };

        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        await _channel.ExchangeDeclareAsync(
            exchange: _exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false);
    }

    public async Task PublishEventAsync(ReplicationEvent replicationEvent)
    {
        if (_channel == null || _exchangeName == null)
            throw new InvalidOperationException("EventPublisher not initialized");

        lock (_lock)
        {
            replicationEvent.SequenceNumber = ++_sequenceNumber;
        }

        var json = JsonSerializer.Serialize(replicationEvent);
        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            Persistent = true
        };

        await _channel.BasicPublishAsync(
            exchange: _exchangeName,
            routingKey: string.Empty,
            mandatory: false,
            basicProperties: properties,
            body: body);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel != null)
        {
            await _channel.CloseAsync();
            await _channel.DisposeAsync();
        }
        
        if (_connection != null)
        {
            await _connection.CloseAsync();
            await _connection.DisposeAsync();
        }
    }
}