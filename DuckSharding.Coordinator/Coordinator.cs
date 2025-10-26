using DuckSharding.Shared.Models;
using DuckSharding.Shared.Hasing;

namespace DuckSharding.Coordinator;

public class Coordinator
{
	private readonly HashRing _hashRing;

	public Coordinator()
	{
		var hashFunction = new SimpleHash();
		_hashRing = new HashRing(hashFunction);
	}

	public void AddShard(ShardInfo shard)
	{
		_hashRing.AddNode(shard);
	}

	public bool RemoveShard(string shardId)
	{
		var shard = _hashRing.GetAllShards().FirstOrDefault(s => s.ShardId == shardId);
		if (shard == null) return false;
        
		_hashRing.RemoveNode(shardId);
		return true;
	}

	public ShardInfo? GetShardForKey(string key)
	{
		return _hashRing.GetNode(key);
	}

	public IReadOnlyCollection<ShardInfo> GetAllShards()
	{
		return _hashRing.GetAllShards();
	}

	public IReadOnlyDictionary<uint, string> GetRingState()
	{
		return _hashRing.GetRingState();
	}
}