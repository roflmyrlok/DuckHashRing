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
    private readonly ReplicationLogRepository _replicationLog;
    private readonly ILogger<EventConsumer> _logger;
    private readonly IConfiguration _configuration;
    private string? _queueName;
    private string? _exchangeName;
    private readonly IHttpClientFactory _httpClientFactory;
    private string? _leaderBaseUrl;
    private string? _replicaId;
    private string? _baseShardId;

    public EventConsumer(
        IConfiguration configuration,
        GenericRepository repository,
        ReplicationLogRepository replicationLog,
        ILogger<EventConsumer> logger,
        IHttpClientFactory httpClientFactory)
    {
        _configuration = configuration;
        _repository = repository;
        _replicationLog = replicationLog;
        _logger = logger;
        _httpClientFactory = httpClientFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var shardId = _configuration["Shard:ShardId"] ?? "shard-0";
        _replicaId = _configuration["Shard:ReplicaId"] ?? "follower-0";
        var rabbitHost = _configuration["RabbitMQ:Host"] ?? "rabbitmq";
        var rabbitPort = int.Parse(_configuration["RabbitMQ:Port"] ?? "5672");
        var k8sNamespace = _configuration["Kubernetes:Namespace"] ?? "default";
        
        _baseShardId = shardId.Contains("-leader") || shardId.Contains("-follower") 
            ? shardId.Split(new[] { "-leader", "-follower" }, StringSplitOptions.None)[0]
            : shardId;

        _leaderBaseUrl = $"http://{_baseShardId}.shard.{k8sNamespace}.svc.cluster.local:8080";
        _exchangeName = $"{_baseShardId}-replication";
        
        _queueName = $"{_baseShardId}-{_replicaId}";
        
        _logger.LogInformation($"EventConsumer starting for replica {_replicaId} on shard {_baseShardId}");
        _logger.LogInformation($"Binding to exchange: {_exchangeName}, queue: {_queueName}");
        
        await CatchupWithLeaderAsync(stoppingToken);

        var factory = new ConnectionFactory
        {
            HostName = rabbitHost,
            Port = rabbitPort
        };

        _connection = await factory.CreateConnectionAsync(stoppingToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);

        // Declare the shared fan-out exchange (idempotent)
        await _channel.ExchangeDeclareAsync(
            exchange: _exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        // Declare this replica's exclusive queue
        await _channel.QueueDeclareAsync(
            queue: _queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        // Bind the queue to the fan-out exchange
        await _channel.QueueBindAsync(
            queue: _queueName,
            exchange: _exchangeName,
            routingKey: string.Empty,
            arguments: null,
            cancellationToken: stoppingToken);

        _logger.LogInformation($"Queue {_queueName} bound to exchange {_exchangeName}");

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

        _logger.LogInformation($"EventConsumer started consuming from queue: {_queueName}");

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("EventConsumer stopping...");
        }
    }

    private async Task CatchupWithLeaderAsync(CancellationToken cancellationToken)
    {
        try
        {
            var currentSequence = _replicationLog.GetLatestSequenceNumber();
            
            _logger.LogInformation($"Starting catchup from sequence {currentSequence}");

            var client = _httpClientFactory.CreateClient();
            var response = await client.GetAsync(
                $"{_leaderBaseUrl}/internal/replication/latest-sequence", 
                cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Could not get latest sequence from leader, skipping catchup");
                return;
            }

            var responseJson = await response.Content.ReadAsStringAsync(cancellationToken);
            var latestSequenceResponse = JsonSerializer.Deserialize<LatestSequenceResponse>(
                responseJson, 
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (latestSequenceResponse == null)
            {
                _logger.LogWarning("Invalid response from leader, skipping catchup");
                return;
            }

            var leaderSequence = latestSequenceResponse.LatestSequence;

            if (leaderSequence <= currentSequence)
            {
                _logger.LogInformation($"Replica is up to date (current: {currentSequence}, leader: {leaderSequence})");
                return;
            }

            _logger.LogInformation($"Catchup needed: current={currentSequence}, leader={leaderSequence}, gap={leaderSequence - currentSequence}");
            
            var catchupResponse = await client.GetAsync(
                $"{_leaderBaseUrl}/internal/replication/events?fromSequence={currentSequence}",
                cancellationToken);

            if (!catchupResponse.IsSuccessStatusCode)
            {
                _logger.LogError("Failed to fetch catchup events from leader");
                return;
            }

            var catchupJson = await catchupResponse.Content.ReadAsStringAsync(cancellationToken);
            var events = JsonSerializer.Deserialize<List<ReplicationEvent>>(
                catchupJson,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

            if (events == null || events.Count == 0)
            {
                _logger.LogWarning("No catchup events returned from leader");
                return;
            }

            _logger.LogInformation($"Applying {events.Count} catchup events");

            foreach (var evt in events.OrderBy(e => e.SequenceNumber))
            {
                await ApplyEventAsync(evt, isCatchup: true);
            }

            _logger.LogInformation($"Catchup complete. New sequence: {_replicationLog.GetLatestSequenceNumber()}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during catchup with leader");
        }
    }

    private async Task ApplyEventAsync(ReplicationEvent replicationEvent, bool isCatchup = false)
    {
        var currentSequence = _replicationLog.GetLatestSequenceNumber();
        
        if (replicationEvent.SequenceNumber <= currentSequence)
        {
            _logger.LogWarning($"Skipping duplicate/old sequence: {replicationEvent.SequenceNumber} (current: {currentSequence})");
            return;
        }

        var expectedSequence = currentSequence + 1;
        if (replicationEvent.SequenceNumber > expectedSequence)
        {
            _logger.LogWarning($"Sequence gap detected: expected {expectedSequence}, got {replicationEvent.SequenceNumber}");
            
            if (!isCatchup)
            { 
                _logger.LogWarning("Gap will be filled on next catchup cycle");
            }
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
                        _repository.ApplyTableRegistration(tableDef);
                        _logger.LogInformation($"Applied TableRegistration: {tableDef.TableName} (seq: {replicationEvent.SequenceNumber})");
                    }
                    break;

                case OperationType.Create:
                    await _repository.ApplyCreateAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation($"Applied Create: {replicationEvent.TableName} (seq: {replicationEvent.SequenceNumber})");
                    break;

                case OperationType.Update:
                    await _repository.ApplyUpdateAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation($"Applied Update: {replicationEvent.TableName} (seq: {replicationEvent.SequenceNumber})");
                    break;

                case OperationType.Delete:
                    await _repository.ApplyDeleteAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation($"Applied Delete: {replicationEvent.TableName} (seq: {replicationEvent.SequenceNumber})");
                    break;
            }
            _replicationLog.AppendEvent(replicationEvent);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Failed to apply event {replicationEvent.SequenceNumber}");
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("EventConsumer stopping...");
        
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

    private class LatestSequenceResponse
    {
        public long LatestSequence { get; set; }
    }
}