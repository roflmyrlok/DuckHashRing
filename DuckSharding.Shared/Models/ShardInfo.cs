namespace DuckSharding.Shared.Models;

public class ShardInfo
{
	public string ShardId { get; set; } = string.Empty;
	public string Host { get; set; } = string.Empty;
	public int Port { get; set; }
	public bool IsHealthy { get; set; } = true;
	public bool IsLeader { get; set; } = false;

	public ShardInfo()
	{
	}

	public ShardInfo(string shardId, string host, int port, bool isHealthy = true, bool isLeader = false)
	{
		ShardId = shardId;
		Host = host;
		Port = port;
		IsHealthy = isHealthy;
		IsLeader = isLeader;
	}

	public string GetBaseUrl()
	{
		return $"http://{Host}:{Port}";
	}

	public override string ToString()
	{
		var role = IsLeader ? "Leader" : "Follower";
		return $"{ShardId} ({Host}:{Port}) - {role} - {(IsHealthy ? "Healthy" : "Unhealthy")}";
	}
}