namespace DuckSharding.Shared.Models;

public class ShardInfo
{
	public string ShardId { get; set; } = string.Empty;
	public string Host { get; set; } = string.Empty;
	public int Port { get; set; }
	public bool IsHealthy { get; set; } = true;

	public ShardInfo()
	{
	}

	public ShardInfo(string shardId, string host, int port, bool isHealthy = true)
	{
		ShardId = shardId;
		Host = host;
		Port = port;
		IsHealthy = isHealthy;
	}

	public string GetBaseUrl()
	{
		return $"http://{Host}:{Port}";
	}

	public override string ToString()
	{
		return $"{ShardId} ({Host}:{Port}) - {(IsHealthy ? "Healthy" : "Unhealthy")}";
	}
}