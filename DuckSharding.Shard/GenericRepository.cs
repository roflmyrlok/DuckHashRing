namespace DuckSharding.Shard;

using Dapper;
using Microsoft.Data.Sqlite;
using Shared.Models;
using System.Text;
using System.Text.Json;

public class GenericRepository
{
    private readonly string _connectionString;
    private readonly Dictionary<string, TableDefinition> _tables = new();
    private readonly object _lock = new();
    private readonly EventPublisher? _eventPublisher;

    public GenericRepository(IConfiguration configuration, EventPublisher? eventPublisher = null)
    {
        var dbFileName = configuration["Database:FileName"] ?? "shard.db";
        _connectionString = $"Data Source={dbFileName}";
        _eventPublisher = eventPublisher;
        InitializeMetadataTable();
        LoadTableDefinitions();
    }

    private void InitializeMetadataTable()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var createMetadataTableSql = @"
            CREATE TABLE IF NOT EXISTS __TableMetadata (
                TableName TEXT PRIMARY KEY,
                PartitionKey TEXT NOT NULL,
                SortKey TEXT NOT NULL,
                ColumnsJson TEXT NOT NULL
            )";

        connection.Execute(createMetadataTableSql);
    }

    private void LoadTableDefinitions()
    {
        using var connection = new SqliteConnection(_connectionString);
        
        var sql = "SELECT TableName, PartitionKey, SortKey, ColumnsJson FROM __TableMetadata";
        var results = connection.Query<TableMetadataDto>(sql);

        foreach (var result in results)
        {
            var columns = JsonSerializer.Deserialize<List<ColumnDefinition>>(result.ColumnsJson);
            if (columns != null)
            {
                var tableDef = new TableDefinition(result.TableName, result.PartitionKey, result.SortKey, columns);
                _tables[result.TableName] = tableDef;
            }
        }
    }

    public void RegisterTable(TableDefinition tableDefinition)
    {
        tableDefinition.Validate();

        lock (_lock)
        {
            if (_tables.ContainsKey(tableDefinition.TableName))
                throw new InvalidOperationException($"Table '{tableDefinition.TableName}' already exists");

            using var connection = new SqliteConnection(_connectionString);
            connection.Open();

            using var transaction = connection.BeginTransaction();
            try
            {
                var columnsJson = JsonSerializer.Serialize(tableDefinition.Columns);
                var insertMetadataSql = @"
                    INSERT INTO __TableMetadata (TableName, PartitionKey, SortKey, ColumnsJson)
                    VALUES (@TableName, @PartitionKey, @SortKey, @ColumnsJson)";

                connection.Execute(insertMetadataSql, new
                {
                    TableName = tableDefinition.TableName,
                    PartitionKey = tableDefinition.PartitionKey,
                    SortKey = tableDefinition.SortKey,
                    ColumnsJson = columnsJson
                }, transaction);

                var createTableSql = GenerateCreateTableSql(tableDefinition);
                connection.Execute(createTableSql, transaction: transaction);

                transaction.Commit();
                _tables[tableDefinition.TableName] = tableDefinition;

                if (_eventPublisher != null)
                {
                    var eventData = new Dictionary<string, object>
                    {
                        { "TableName", tableDefinition.TableName },
                        { "PartitionKey", tableDefinition.PartitionKey },
                        { "SortKey", tableDefinition.SortKey },
                        { "Columns", tableDefinition.Columns }
                    };

                    var replicationEvent = new ReplicationEvent(0, tableDefinition.TableName, OperationType.TableRegistration, eventData);
                    _eventPublisher.PublishEventAsync(replicationEvent).Wait();
                }
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }

    public TableDefinition? GetTableDefinition(string tableName)
    {
        return _tables.GetValueOrDefault(tableName);
    }

    public List<TableDefinition> GetAllTables()
    {
        return _tables.Values.ToList();
    }

    public async Task<bool> ExistsAsync(string tableName, Dictionary<string, object> primaryKey)
    {
        var tableDef = GetTableDefinitionOrThrow(tableName);

        if (!primaryKey.ContainsKey(tableDef.PartitionKey))
            throw new ArgumentException($"Primary key must contain partition key: {tableDef.PartitionKey}");
    
        if (!primaryKey.ContainsKey(tableDef.SortKey))
            throw new ArgumentException($"Primary key must contain sort key: {tableDef.SortKey}");

        using var connection = new SqliteConnection(_connectionString);
    
        string sql;
        object parameters;
    
        if (tableDef.PartitionKey == tableDef.SortKey)
        {
            sql = $"SELECT COUNT(1) FROM {tableName} WHERE {tableDef.PartitionKey} = @KeyValue";
            parameters = new { KeyValue = ConvertToDbValue(primaryKey[tableDef.PartitionKey]) };
        }
        else
        {
            sql = $@"
                SELECT COUNT(1) FROM {tableName}
                WHERE {tableDef.PartitionKey} = @PartitionKeyValue 
                AND {tableDef.SortKey} = @SortKeyValue";
        
            parameters = new
            {
                PartitionKeyValue = ConvertToDbValue(primaryKey[tableDef.PartitionKey]),
                SortKeyValue = ConvertToDbValue(primaryKey[tableDef.SortKey])
            };
        }

        var count = await connection.ExecuteScalarAsync<int>(sql, parameters);
        return count > 0;
    }

    public async Task<bool> CreateAsync(string tableName, Dictionary<string, object> record)
    {
        var tableDef = GetTableDefinitionOrThrow(tableName);
        ValidateRecord(tableDef, record);

        var primaryKey = new Dictionary<string, object>
        {
            { tableDef.PartitionKey, record[tableDef.PartitionKey] }
        };
        
        if (tableDef.PartitionKey != tableDef.SortKey)
        {
            primaryKey[tableDef.SortKey] = record[tableDef.SortKey];
        }
        
        if (await ExistsAsync(tableName, primaryKey))
            return false;

        using var connection = new SqliteConnection(_connectionString);
        var columns = string.Join(", ", record.Keys);
        var parameters = string.Join(", ", record.Keys.Select(k => $"@{k}"));
        
        var sql = $"INSERT INTO {tableName} ({columns}) VALUES ({parameters})";
        
        var dbParams = new DynamicParameters();
        foreach (var kvp in record)
        {
            dbParams.Add(kvp.Key, ConvertToDbValue(kvp.Value));
        }

        await connection.ExecuteAsync(sql, dbParams);

        if (_eventPublisher != null)
        {
            var replicationEvent = new ReplicationEvent(0, tableName, OperationType.Create, record);
            await _eventPublisher.PublishEventAsync(replicationEvent);
        }

        return true;
    }

    public async Task<Dictionary<string, object>?> ReadAsync(string tableName, Dictionary<string, object> primaryKey)
    {
        var tableDef = GetTableDefinitionOrThrow(tableName);

        if (!primaryKey.ContainsKey(tableDef.PartitionKey))
            throw new ArgumentException($"Primary key must contain partition key: {tableDef.PartitionKey}");
    
        if (!primaryKey.ContainsKey(tableDef.SortKey))
            throw new ArgumentException($"Primary key must contain sort key: {tableDef.SortKey}");

        using var connection = new SqliteConnection(_connectionString);
    
        string sql;
        object parameters;
    
        if (tableDef.PartitionKey == tableDef.SortKey)
        {
            sql = $"SELECT * FROM {tableName} WHERE {tableDef.PartitionKey} = @KeyValue";
            parameters = new { KeyValue = ConvertToDbValue(primaryKey[tableDef.PartitionKey]) };
        }
        else
        {
            sql = $@"
                SELECT * FROM {tableName}
                WHERE {tableDef.PartitionKey} = @PartitionKeyValue 
                AND {tableDef.SortKey} = @SortKeyValue";
        
            parameters = new
            {
                PartitionKeyValue = ConvertToDbValue(primaryKey[tableDef.PartitionKey]),
                SortKeyValue = ConvertToDbValue(primaryKey[tableDef.SortKey])
            };
        }

        var result = await connection.QuerySingleOrDefaultAsync(sql, parameters);

        if (result == null) return null;

        return DapperRowToDictionary(result, tableDef);
    }

    public async Task<bool> UpdateAsync(string tableName, Dictionary<string, object> record)
    {
        var tableDef = GetTableDefinitionOrThrow(tableName);
        ValidateRecord(tableDef, record);
        
        var primaryKey = new Dictionary<string, object>
        {
            { tableDef.PartitionKey, record[tableDef.PartitionKey] }
        };
    
        if (tableDef.SortKey != tableDef.PartitionKey)
        {
            primaryKey[tableDef.SortKey] = record[tableDef.SortKey];
        }
    
        if (!await ExistsAsync(tableName, primaryKey))
            return false;

        using var connection = new SqliteConnection(_connectionString);
    
        var updates = string.Join(", ", record.Keys
            .Where(k => k != tableDef.PartitionKey && k != tableDef.SortKey)
            .Select(k => $"{k} = @{k}"));

        var sql = $@"
            UPDATE {tableName} 
            SET {updates}
            WHERE {tableDef.PartitionKey} = @PartitionKeyValue 
            AND {tableDef.SortKey} = @SortKeyValue";

        var dbParams = new DynamicParameters();
        foreach (var kvp in record)
        {
            dbParams.Add(kvp.Key, ConvertToDbValue(kvp.Value));
        }
    
        if (!dbParams.ParameterNames.Contains("PartitionKeyValue"))
            dbParams.Add("PartitionKeyValue", ConvertToDbValue(record[tableDef.PartitionKey]));
        if (!dbParams.ParameterNames.Contains("SortKeyValue"))
            dbParams.Add("SortKeyValue", ConvertToDbValue(record[tableDef.SortKey]));

        var rowsAffected = await connection.ExecuteAsync(sql, dbParams);

        if (rowsAffected > 0 && _eventPublisher != null)
        {
            var replicationEvent = new ReplicationEvent(0, tableName, OperationType.Update, record);
            await _eventPublisher.PublishEventAsync(replicationEvent);
        }

        return rowsAffected > 0;
    }

    public async Task<bool> DeleteAsync(string tableName, Dictionary<string, object> primaryKey)
    {
        var tableDef = GetTableDefinitionOrThrow(tableName);

        if (!primaryKey.ContainsKey(tableDef.PartitionKey))
            throw new ArgumentException($"Primary key must contain partition key: {tableDef.PartitionKey}");
    
        if (!primaryKey.ContainsKey(tableDef.SortKey))
            throw new ArgumentException($"Primary key must contain sort key: {tableDef.SortKey}");

        using var connection = new SqliteConnection(_connectionString);
    
        string sql;
        object parameters;
    
        if (tableDef.PartitionKey == tableDef.SortKey)
        {
            sql = $"DELETE FROM {tableName} WHERE {tableDef.PartitionKey} = @KeyValue";
            parameters = new { KeyValue = ConvertToDbValue(primaryKey[tableDef.PartitionKey]) };
        }
        else
        {
            sql = $@"
                DELETE FROM {tableName}
                WHERE {tableDef.PartitionKey} = @PartitionKeyValue 
                AND {tableDef.SortKey} = @SortKeyValue";
        
            parameters = new
            {
                PartitionKeyValue = ConvertToDbValue(primaryKey[tableDef.PartitionKey]),
                SortKeyValue = ConvertToDbValue(primaryKey[tableDef.SortKey])
            };
        }

        var rowsAffected = await connection.ExecuteAsync(sql, parameters);

        if (rowsAffected > 0 && _eventPublisher != null)
        {
            var replicationEvent = new ReplicationEvent(0, tableName, OperationType.Delete, primaryKey);
            await _eventPublisher.PublishEventAsync(replicationEvent);
        }

        return rowsAffected > 0;
    }

    private string GenerateCreateTableSql(TableDefinition tableDef)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE IF NOT EXISTS {tableDef.TableName} (");

        var columnDefs = new List<string>();
        foreach (var column in tableDef.Columns)
        {
            var nullConstraint = column.IsNullable ? "" : "NOT NULL";
            columnDefs.Add($"    {column.ColumnName} {column.GetSqlType()} {nullConstraint}");
        }

        sb.AppendLine(string.Join(",\n", columnDefs) + ",");
        sb.AppendLine($"    PRIMARY KEY ({tableDef.PartitionKey}, {tableDef.SortKey})");
        sb.Append(")");

        return sb.ToString();
    }

    private TableDefinition GetTableDefinitionOrThrow(string tableName)
    {
        if (!_tables.TryGetValue(tableName, out var tableDef))
            throw new InvalidOperationException($"Table '{tableName}' does not exist");
        return tableDef;
    }

    private void ValidateRecord(TableDefinition tableDef, Dictionary<string, object> record)
    {
        var missingColumns = tableDef.Columns
            .Where(c => !c.IsNullable && !record.ContainsKey(c.ColumnName))
            .Select(c => c.ColumnName)
            .ToList();

        if (missingColumns.Any())
            throw new ArgumentException($"Missing required columns: {string.Join(", ", missingColumns)}");

        var unknownColumns = record.Keys
            .Where(k => !tableDef.Columns.Any(c => c.ColumnName == k))
            .ToList();

        if (unknownColumns.Any())
            throw new ArgumentException($"Unknown columns: {string.Join(", ", unknownColumns)}");
    }

    private object ConvertToDbValue(object value)
    {
        if (value is JsonElement je)
        {
            if (je.ValueKind == JsonValueKind.Number)
                return je.GetInt64();
            if (je.ValueKind == JsonValueKind.Null)
                return DBNull.Value;
        }
        return Convert.ToInt64(value);
    }

    private Dictionary<string, object> DapperRowToDictionary(dynamic row, TableDefinition tableDef)
    {
        var dict = new Dictionary<string, object>();
        var rowDict = (IDictionary<string, object>)row;

        foreach (var column in tableDef.Columns)
        {
            if (rowDict.TryGetValue(column.ColumnName, out var value))
            {
                dict[column.ColumnName] = ConvertFromDbValue(value, column.ColumnType);
            }
        }

        return dict;
    }

    private object ConvertFromDbValue(object value, ColumnType columnType)
    {
        if (value == null || value is DBNull)
            return null!;

        return Convert.ToInt64(value);
    }

    private class TableMetadataDto
    {
        public string TableName { get; set; } = string.Empty;
        public string PartitionKey { get; set; } = string.Empty;
        public string SortKey { get; set; } = string.Empty;
        public string ColumnsJson { get; set; } = string.Empty;
    }
}