using Microsoft.Data.Sqlite;
using Dapper;
using System.Text.Json;
using DuckSharding.Shared.Models;

namespace DuckSharding.Shard;

public class ReplicationLogRepository
{
    private readonly string _connectionString;
    private readonly object _lock = new();

    public ReplicationLogRepository(IConfiguration configuration)
    {
        var dbFileName = configuration["Database:FileName"] ?? "shard.db";
        _connectionString = $"Data Source={dbFileName}";
        InitializeReplicationLog();
    }

    private void InitializeReplicationLog()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var createLogTableSql = @"
            CREATE TABLE IF NOT EXISTS __ReplicationLog (
                SequenceNumber INTEGER PRIMARY KEY,
                TableName TEXT NOT NULL,
                OperationType INTEGER NOT NULL,
                DataJson TEXT NOT NULL,
                Timestamp TEXT NOT NULL
            )";

        connection.Execute(createLogTableSql);

        var createIndexSql = @"
            CREATE INDEX IF NOT EXISTS idx_replication_log_sequence 
            ON __ReplicationLog(SequenceNumber)";
        
        connection.Execute(createIndexSql);
    }

    public long GetLatestSequenceNumber()
    {
        lock (_lock)
        {
            using var connection = new SqliteConnection(_connectionString);
            var sql = "SELECT COALESCE(MAX(SequenceNumber), 0) FROM __ReplicationLog";
            return connection.ExecuteScalar<long>(sql);
        }
    }

    public void AppendEvent(ReplicationEvent replicationEvent)
    {
        lock (_lock)
        {
            using var connection = new SqliteConnection(_connectionString);
            
            var sql = @"
                INSERT INTO __ReplicationLog (SequenceNumber, TableName, OperationType, DataJson, Timestamp)
                VALUES (@SequenceNumber, @TableName, @OperationType, @DataJson, @Timestamp)";

            connection.Execute(sql, new
            {
                replicationEvent.SequenceNumber,
                replicationEvent.TableName,
                OperationType = (int)replicationEvent.OperationType,
                DataJson = JsonSerializer.Serialize(replicationEvent.Data),
                Timestamp = replicationEvent.Timestamp.ToString("o")
            });
        }
    }

    public List<ReplicationEvent> GetEventsAfter(long sequenceNumber)
    {
        lock (_lock)
        {
            using var connection = new SqliteConnection(_connectionString);
            
            var sql = @"
                SELECT SequenceNumber, TableName, OperationType, DataJson, Timestamp
                FROM __ReplicationLog
                WHERE SequenceNumber > @SequenceNumber
                ORDER BY SequenceNumber ASC";

            var results = connection.Query<ReplicationLogDto>(sql, new { SequenceNumber = sequenceNumber });

            return results.Select(dto => new ReplicationEvent
            {
                SequenceNumber = dto.SequenceNumber,
                TableName = dto.TableName,
                OperationType = (OperationType)dto.OperationType,
                Data = JsonSerializer.Deserialize<Dictionary<string, object>>(dto.DataJson) 
                       ?? new Dictionary<string, object>(),
                Timestamp = DateTime.Parse(dto.Timestamp)
            }).ToList();
        }
    }

    public ReplicationEvent? GetEvent(long sequenceNumber)
    {
        lock (_lock)
        {
            using var connection = new SqliteConnection(_connectionString);
            
            var sql = @"
                SELECT SequenceNumber, TableName, OperationType, DataJson, Timestamp
                FROM __ReplicationLog
                WHERE SequenceNumber = @SequenceNumber";

            var dto = connection.QuerySingleOrDefault<ReplicationLogDto>(sql, new { SequenceNumber = sequenceNumber });

            if (dto == null) return null;

            return new ReplicationEvent
            {
                SequenceNumber = dto.SequenceNumber,
                TableName = dto.TableName,
                OperationType = (OperationType)dto.OperationType,
                Data = JsonSerializer.Deserialize<Dictionary<string, object>>(dto.DataJson) 
                       ?? new Dictionary<string, object>(),
                Timestamp = DateTime.Parse(dto.Timestamp)
            };
        }
    }

    public bool HasSequenceGap(long currentSequence, long expectedSequence)
    {
        return currentSequence > expectedSequence;
    }

    public List<long> GetMissingSequences(long fromSequence, long toSequence)
    {
        lock (_lock)
        {
            using var connection = new SqliteConnection(_connectionString);
            
            var sql = @"
                SELECT SequenceNumber 
                FROM __ReplicationLog
                WHERE SequenceNumber > @FromSequence AND SequenceNumber <= @ToSequence
                ORDER BY SequenceNumber ASC";

            var existing = connection.Query<long>(sql, new 
            { 
                FromSequence = fromSequence, 
                ToSequence = toSequence 
            }).ToHashSet();

            var missing = new List<long>();
            for (long i = fromSequence + 1; i <= toSequence; i++)
            {
                if (!existing.Contains(i))
                {
                    missing.Add(i);
                }
            }

            return missing;
        }
    }

    private class ReplicationLogDto
    {
        public long SequenceNumber { get; set; }
        public string TableName { get; set; } = string.Empty;
        public int OperationType { get; set; }
        public string DataJson { get; set; } = string.Empty;
        public string Timestamp { get; set; } = string.Empty;
    }
}