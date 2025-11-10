namespace DuckSharding.Shared.Models;

public class ReplicationEvent
{
	public long SequenceNumber { get; set; }
	public string TableName { get; set; } = string.Empty;
	public OperationType OperationType { get; set; }
	public Dictionary<string, object> Data { get; set; } = new();
	public DateTime Timestamp { get; set; }

	public ReplicationEvent()
	{
		Timestamp = DateTime.UtcNow;
	}

	public ReplicationEvent(long sequenceNumber, string tableName, OperationType operationType, Dictionary<string, object> data)
	{
		SequenceNumber = sequenceNumber;
		TableName = tableName;
		OperationType = operationType;
		Data = data;
		Timestamp = DateTime.UtcNow;
	}
}

public enum OperationType
{
	Create,
	Update,
	Delete,
	TableRegistration
}