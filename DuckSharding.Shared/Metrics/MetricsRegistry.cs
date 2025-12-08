using Prometheus;

namespace DuckSharding.Shared.Metrics;

public static class MetricsRegistry
{
    // Coordinator Metrics
    public static readonly Counter CoordinatorRequestsTotal = Prometheus.Metrics
        .CreateCounter("coordinator_requests_total", "Total number of requests to coordinator",
            new CounterConfiguration { LabelNames = new[] { "operation", "table", "status" } });

    public static readonly Histogram CoordinatorRequestDuration = Prometheus.Metrics
        .CreateHistogram("coordinator_request_duration_seconds", "Duration of coordinator requests",
            new HistogramConfiguration
            {
                LabelNames = new[] { "operation", "table" },
                Buckets = Histogram.ExponentialBuckets(0.001, 2, 15) // 1ms to ~16s
            });

    public static readonly Gauge CoordinatorShardsHealthy = Prometheus.Metrics
        .CreateGauge("coordinator_shards_healthy", "Number of healthy shards");

    public static readonly Gauge CoordinatorTotalShards = Prometheus.Metrics
        .CreateGauge("coordinator_total_shards", "Total number of shards");

    // Shard Metrics
    public static readonly Counter ShardOperationsTotal = Prometheus.Metrics
        .CreateCounter("shard_operations_total", "Total number of operations on shard",
            new CounterConfiguration { LabelNames = new[] { "shard_id", "operation", "table", "status" } });

    public static readonly Histogram ShardOperationDuration = Prometheus.Metrics
        .CreateHistogram("shard_operation_duration_seconds", "Duration of shard operations",
            new HistogramConfiguration
            {
                LabelNames = new[] { "shard_id", "operation", "table" },
                Buckets = Histogram.ExponentialBuckets(0.001, 2, 15)
            });

    public static readonly Gauge ShardReplicationLag = Prometheus.Metrics
        .CreateGauge("shard_replication_lag_seconds", "Replication lag in seconds",
            new GaugeConfiguration { LabelNames = new[] { "shard_id", "replica_id" } });

    public static readonly Counter ShardReplicationEventsTotal = Prometheus.Metrics
        .CreateCounter("shard_replication_events_total", "Total replication events processed",
            new CounterConfiguration { LabelNames = new[] { "shard_id", "replica_id", "operation", "status" } });

    public static readonly Gauge ShardReplicationSequenceNumber = Prometheus.Metrics
        .CreateGauge("shard_replication_sequence_number", "Current replication sequence number",
            new GaugeConfiguration { LabelNames = new[] { "shard_id", "replica_id", "type" } });

    public static readonly Counter ShardReplicationGapsTotal = Prometheus.Metrics
        .CreateCounter("shard_replication_gaps_total", "Total replication sequence gaps detected",
            new CounterConfiguration { LabelNames = new[] { "shard_id", "replica_id" } });

    // Database Metrics
    public static readonly Gauge ShardDatabaseSize = Prometheus.Metrics
        .CreateGauge("shard_database_size_bytes", "Size of shard database in bytes",
            new GaugeConfiguration { LabelNames = new[] { "shard_id" } });

    public static readonly Gauge ShardTablesCount = Prometheus.Metrics
        .CreateGauge("shard_tables_count", "Number of tables in shard",
            new GaugeConfiguration { LabelNames = new[] { "shard_id" } });

    // RabbitMQ Metrics
    public static readonly Counter RabbitMqMessagesPublished = Prometheus.Metrics
        .CreateCounter("rabbitmq_messages_published_total", "Total messages published to RabbitMQ",
            new CounterConfiguration { LabelNames = new[] { "exchange", "shard_id" } });

    public static readonly Counter RabbitMqMessagesConsumed = Prometheus.Metrics
        .CreateCounter("rabbitmq_messages_consumed_total", "Total messages consumed from RabbitMQ",
            new CounterConfiguration { LabelNames = new[] { "queue", "shard_id", "status" } });
}