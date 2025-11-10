using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using DuckSharding.Shared.Models;

namespace DuckSharding.Shard;

public class EventConsumer : BackgroundService
{
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly GenericRepository _repository;
    private readonly ILogger<EventConsumer> _logger;
    private readonly IConfiguration _configuration;
    private string? _queueName;
    private long _lastProcessedSequence;

    public EventConsumer(
        IConfiguration configuration,
        GenericRepository repository,
        ILogger<EventConsumer> logger)
    {
        _configuration = configuration;
        _repository = repository;
        _logger = logger;
        _lastProcessedSequence = 0;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var shardId = _configuration["Shard:ShardId"] ?? "shard-0";
        var replicaId = _configuration["Shard:ReplicaId"] ?? "follower-1";
        var rabbitHost = _configuration["RabbitMQ:Host"] ?? "rabbitmq";
        var rabbitPort = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672");

        var exchangeName = $"shard-{shardId}-events";
        _queueName = $"shard-{shardId}-{replicaId}";

        var factory = new ConnectionFactory
        {
            HostName = rabbitHost,
            Port = rabbitPort
        };

        _connection = await factory.CreateConnectionAsync(stoppingToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await _channel.QueueDeclareAsync(
            queue: _queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        await _channel.QueueBindAsync(
            queue: _queueName,
            exchange: exchangeName,
            routingKey: string.Empty,
            arguments: null,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        
        consumer.ReceivedAsync += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var json = Encoding.UTF8.GetString(body);
                var replicationEvent = JsonSerializer.Deserialize<ReplicationEvent>(json);

                if (replicationEvent != null)
                {
                    await ApplyEventAsync(replicationEvent);
                    await _channel.BasicAckAsync(ea.DeliveryTag, false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing replication event");
                await _channel.BasicNackAsync(ea.DeliveryTag, false, true);
            }

            await Task.Yield();
        };

        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ApplyEventAsync(ReplicationEvent replicationEvent)
    {
        if (replicationEvent.SequenceNumber <= _lastProcessedSequence)
        {
            _logger.LogWarning($"Skipping duplicate sequence: {replicationEvent.SequenceNumber}");
            return;
        }

        if (replicationEvent.SequenceNumber > _lastProcessedSequence + 1)
        {
            _logger.LogWarning($"Sequence gap detected: expected {_lastProcessedSequence + 1}, got {replicationEvent.SequenceNumber}");
        }

        try
        {
            switch (replicationEvent.OperationType)
            {
                case OperationType.TableRegistration:
                    var tableDef = JsonSerializer.Deserialize<TableDefinition>(
                        JsonSerializer.Serialize(replicationEvent.Data));
                    if (tableDef != null)
                    {
                        _repository.RegisterTable(tableDef);
                        _logger.LogInformation($"Applied TableRegistration: {tableDef.TableName}");
                    }
                    break;

                case OperationType.Create:
                    await _repository.CreateAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation($"Applied Create: {replicationEvent.TableName}");
                    break;

                case OperationType.Update:
                    await _repository.UpdateAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation($"Applied Update: {replicationEvent.TableName}");
                    break;

                case OperationType.Delete:
                    await _repository.DeleteAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation($"Applied Delete: {replicationEvent.TableName}");
                    break;
            }

            _lastProcessedSequence = replicationEvent.SequenceNumber;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to apply event {replicationEvent.SequenceNumber}");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_channel != null)
        {
            await _channel.CloseAsync(cancellationToken);
            await _channel.DisposeAsync();
        }
        
        if (_connection != null)
        {
            await _connection.CloseAsync(cancellationToken);
            await _connection.DisposeAsync();
        }

        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        base.Dispose();
    }
}