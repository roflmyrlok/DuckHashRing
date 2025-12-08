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
    private readonly ReplicationLogRepository _replicationLog;
    private readonly ILogger<EventPublisher> _logger;
    private readonly object _lock = new();

    public static async Task<EventPublisher> CreateAsync(
        IConfiguration configuration, 
        ReplicationLogRepository replicationLog,
        ILogger<EventPublisher> logger)
    {
        var publisher = new EventPublisher(replicationLog, logger);
        await publisher.InitializeAsync(configuration);
        return publisher;
    }

    private EventPublisher(ReplicationLogRepository replicationLog, ILogger<EventPublisher> logger)
    {
        _replicationLog = replicationLog;
        _logger = logger;
    }

    private async Task InitializeAsync(IConfiguration configuration)
    {
        var shardId = configuration["Shard:ShardId"] ?? "shard-0";
        var rabbitHost = configuration["RabbitMQ:Host"] ?? "rabbitmq";
        var rabbitPort = int.Parse(configuration["RabbitMQ:Port"] ?? "5672");
        
        var baseShardId = shardId.Contains("-leader") || shardId.Contains("-follower") 
            ? shardId.Split(new[] { "-leader", "-follower" }, StringSplitOptions.None)[0]
            : shardId;

        _exchangeName = $"{baseShardId}-replication";

        _logger.LogInformation($"Initializing EventPublisher for exchange: {_exchangeName}");

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

        _logger.LogInformation($"EventPublisher initialized for shard: {baseShardId}");
    }

    public async Task PublishEventAsync(ReplicationEvent replicationEvent)
    {
        if (_channel == null || _exchangeName == null)
            throw new InvalidOperationException("EventPublisher not initialized");
        
        lock (_lock)
        {
            var latestSequence = _replicationLog.GetLatestSequenceNumber();
            replicationEvent.SequenceNumber = latestSequence + 1;
            _replicationLog.AppendEvent(replicationEvent);
        }

        var json = JsonSerializer.Serialize(replicationEvent);
        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            Persistent = true,
            DeliveryMode = DeliveryModes.Persistent
        };

        await _channel.BasicPublishAsync(
            exchange: _exchangeName,
            routingKey: string.Empty,
            mandatory: false,
            basicProperties: properties,
            body: body);

        _logger.LogDebug($"Published event seq={replicationEvent.SequenceNumber} to {_exchangeName}");
    }

    public long GetLatestSequenceNumber()
    {
        return _replicationLog.GetLatestSequenceNumber();
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