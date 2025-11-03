namespace DuckSharding.Shared.Models;

public class TableDefinition
{
    public string TableName { get; set; } = string.Empty;
    public string PartitionKey { get; set; } = string.Empty;
    public string SortKey { get; set; } = string.Empty;
    public List<ColumnDefinition> Columns { get; set; } = new();

    public TableDefinition()
    {
    }

    public TableDefinition(string tableName, string partitionKey, string sortKey, List<ColumnDefinition> columns)
    {
        TableName = tableName;
        PartitionKey = partitionKey;
        SortKey = sortKey;
        Columns = columns;
    }

    public string GetCompositePrimaryKey(Dictionary<string, object> record)
    {
        if (!record.ContainsKey(PartitionKey))
            throw new ArgumentException($"Record must contain partition key: {PartitionKey}");
        if (!record.ContainsKey(SortKey))
            throw new ArgumentException($"Record must contain sort key: {SortKey}");

        return $"{record[PartitionKey]}#{record[SortKey]}";
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(TableName))
            throw new ArgumentException("Table name cannot be empty");

        if (string.IsNullOrWhiteSpace(PartitionKey))
            throw new ArgumentException("Partition key cannot be empty");

        if (string.IsNullOrWhiteSpace(SortKey))
            throw new ArgumentException("Sort key cannot be empty");

        if (Columns == null || Columns.Count == 0)
            throw new ArgumentException("Table must have at least one column");

        if (!Columns.Any(c => c.ColumnName == PartitionKey))
            throw new ArgumentException($"Partition key '{PartitionKey}' must be defined in columns");

        if (!Columns.Any(c => c.ColumnName == SortKey))
            throw new ArgumentException($"Sort key '{SortKey}' must be defined in columns");

        var duplicates = Columns.GroupBy(c => c.ColumnName).Where(g => g.Count() > 1).ToList();
        if (duplicates.Any())
            throw new ArgumentException($"Duplicate column names found: {string.Join(", ", duplicates.Select(d => d.Key))}");
    }
}

public class ColumnDefinition
{
    public string ColumnName { get; set; } = string.Empty;
    public ColumnType ColumnType { get; set; }
    public bool IsNullable { get; set; }

    public ColumnDefinition()
    {
    }

    public ColumnDefinition(string columnName, ColumnType columnType, bool isNullable = false)
    {
        ColumnName = columnName;
        ColumnType = columnType;
        IsNullable = isNullable;
    }

    public string GetSqlType()
    {
        return "INTEGER";
    }
}

public enum ColumnType
{
    Integer
}