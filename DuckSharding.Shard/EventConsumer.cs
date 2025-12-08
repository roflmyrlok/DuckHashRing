using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using DuckSharding.Shared.Models;
using DuckSharding.Shared.Metrics;

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
    private DateTime _lastEventTime = DateTime.UtcNow;

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
        
        _logger.LogInformation("EventConsumer starting for replica {ReplicaId} on shard {ShardId}", _replicaId, _baseShardId);
        _logger.LogInformation("Binding to exchange: {ExchangeName}, queue: {QueueName}", _exchangeName, _queueName);
        
        // Start replication lag monitoring
        _ = MonitorReplicationLagAsync(stoppingToken);
        
        await CatchupWithLeaderAsync(stoppingToken);

        var factory = new ConnectionFactory
        {
            HostName = rabbitHost,
            Port = rabbitPort
        };

        _connection = await factory.CreateConnectionAsync(stoppingToken);
        _channel = await _connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await _channel.ExchangeDeclareAsync(
            exchange: _exchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        await _channel.QueueDeclareAsync(
            queue: _queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: stoppingToken);

        await _channel.QueueBindAsync(
            queue: _queueName,
            exchange: _exchangeName,
            routingKey: string.Empty,
            arguments: null,
            cancellationToken: stoppingToken);

        _logger.LogInformation("Queue {QueueName} bound to exchange {ExchangeName}", _queueName, _exchangeName);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var status = "success";
            try
            {
                var body = ea.Body.ToArray();
                var json = Encoding.UTF8.GetString(body);
                var replicationEvent = JsonSerializer.Deserialize<ReplicationEvent>(json);

                if (replicationEvent != null)
                {
                    await ApplyEventAsync(replicationEvent);
                    await _channel.BasicAckAsync(ea.DeliveryTag, false);
                    _lastEventTime = DateTime.UtcNow;
                    
                    MetricsRegistry.RabbitMqMessagesConsumed
                        .WithLabels(_queueName ?? "unknown", _baseShardId ?? "unknown", status)
                        .Inc();
                }
            }
            catch (Exception ex)
            {
                status = "error";
                _logger.LogError(ex, "Error processing replication event");
                await _channel.BasicNackAsync(ea.DeliveryTag, false, true);
                
                MetricsRegistry.RabbitMqMessagesConsumed
                    .WithLabels(_queueName ?? "unknown", _baseShardId ?? "unknown", status)
                    .Inc();
            }

            await Task.Yield();
        };

        await _channel.BasicConsumeAsync(
            queue: _queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        _logger.LogInformation("EventConsumer started consuming from queue: {QueueName}", _queueName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("EventConsumer stopping...");
        }
    }

    private async Task MonitorReplicationLagAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var lag = (DateTime.UtcNow - _lastEventTime).TotalSeconds;
                MetricsRegistry.ShardReplicationLag
                    .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown")
                    .Set(lag);

                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error monitoring replication lag");
            }
        }
    }

    private async Task CatchupWithLeaderAsync(CancellationToken cancellationToken)
    {
        try
        {
            var currentSequence = _replicationLog.GetLatestSequenceNumber();
            
            MetricsRegistry.ShardReplicationSequenceNumber
                .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown", "current")
                .Set(currentSequence);
            
            _logger.LogInformation("Starting catchup from sequence {CurrentSequence}", currentSequence);

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
            
            MetricsRegistry.ShardReplicationSequenceNumber
                .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown", "leader")
                .Set(leaderSequence);

            if (leaderSequence <= currentSequence)
            {
                _logger.LogInformation("Replica is up to date (current: {Current}, leader: {Leader})", 
                    currentSequence, leaderSequence);
                return;
            }

            _logger.LogInformation("Catchup needed: current={Current}, leader={Leader}, gap={Gap}", 
                currentSequence, leaderSequence, leaderSequence - currentSequence);
            
            MetricsRegistry.ShardReplicationGapsTotal
                .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown")
                .Inc();
            
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

            _logger.LogInformation("Applying {Count} catchup events", events.Count);

            foreach (var evt in events.OrderBy(e => e.SequenceNumber))
            {
                await ApplyEventAsync(evt, isCatchup: true);
            }

            var newSequence = _replicationLog.GetLatestSequenceNumber();
            MetricsRegistry.ShardReplicationSequenceNumber
                .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown", "current")
                .Set(newSequence);

            _logger.LogInformation("Catchup complete. New sequence: {NewSequence}", newSequence);
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
            _logger.LogWarning("Skipping duplicate/old sequence: {Sequence} (current: {Current})", 
                replicationEvent.SequenceNumber, currentSequence);
            return;
        }

        var expectedSequence = currentSequence + 1;
        if (replicationEvent.SequenceNumber > expectedSequence)
        {
            _logger.LogWarning("Sequence gap detected: expected {Expected}, got {Actual}", 
                expectedSequence, replicationEvent.SequenceNumber);
            
            MetricsRegistry.ShardReplicationGapsTotal
                .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown")
                .Inc();
            
            if (!isCatchup)
            { 
                _logger.LogWarning("Gap will be filled on next catchup cycle");
            }
        }

        var status = "success";
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
                        _logger.LogInformation("Applied TableRegistration: {TableName} (seq: {Sequence})", 
                            tableDef.TableName, replicationEvent.SequenceNumber);
                    }
                    break;

                case OperationType.Create:
                    await _repository.ApplyCreateAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation("Applied Create: {TableName} (seq: {Sequence})", 
                        replicationEvent.TableName, replicationEvent.SequenceNumber);
                    break;

                case OperationType.Update:
                    await _repository.ApplyUpdateAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation("Applied Update: {TableName} (seq: {Sequence})", 
                        replicationEvent.TableName, replicationEvent.SequenceNumber);
                    break;

                case OperationType.Delete:
                    await _repository.ApplyDeleteAsync(replicationEvent.TableName, replicationEvent.Data);
                    _logger.LogInformation("Applied Delete: {TableName} (seq: {Sequence})", 
                        replicationEvent.TableName, replicationEvent.SequenceNumber);
                    break;
            }
            _replicationLog.AppendEvent(replicationEvent);
            
            MetricsRegistry.ShardReplicationSequenceNumber
                .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown", "current")
                .Set(replicationEvent.SequenceNumber);
        }
        catch (Exception ex)
        {
            status = "error";
            _logger.LogError(ex, "Failed to apply event {Sequence}", replicationEvent.SequenceNumber);
            throw;
        }
        finally
        {
            MetricsRegistry.ShardReplicationEventsTotal
                .WithLabels(_baseShardId ?? "unknown", _replicaId ?? "unknown", 
                    replicationEvent.OperationType.ToString(), status)
                .Inc();
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