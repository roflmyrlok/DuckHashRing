namespace DuckSharding.Shared.Hasing;
using System.Collections.Concurrent;
using DuckSharding.Shared.Models;

public class HashRing
{
    private readonly IHashFunction _hashFunction;
    private readonly SortedDictionary<uint, string> _ring;
    private readonly ConcurrentDictionary<string, ShardInfo> _shards;
    private readonly object _lock = new();

    public HashRing(IHashFunction hashFunction, int virtualNodesPerShard = 150)
    {
        _hashFunction = hashFunction ?? throw new ArgumentNullException(nameof(hashFunction));
        _ring = new SortedDictionary<uint, string>();
        _shards = new ConcurrentDictionary<string, ShardInfo>();
    }
    
    public void AddNode(ShardInfo shard)
    {
        if (shard == null)
            throw new ArgumentNullException(nameof(shard));

        lock (_lock)
        {
            _shards[shard.ShardId] = shard;
            var hash = _hashFunction.ComputeHash(shard.ShardId);
            _ring[hash] = shard.ShardId;
        }
    }

    public void RemoveNode(string shardId)
    {
        if (string.IsNullOrEmpty(shardId))
            throw new ArgumentException("ShardId cannot be null or empty", nameof(shardId));

        lock (_lock)
        {
            if (!_shards.TryRemove(shardId, out _))
                return;
            
            var hash = _hashFunction.ComputeHash(shardId);
            _ring.Remove(hash);
        }
    }
    
    public ShardInfo? GetNode(string key)
    {
        if (string.IsNullOrEmpty(key))
            throw new ArgumentException("Key cannot be null or empty", nameof(key));

        lock (_lock)
        {
            if (_ring.Count == 0)
                return null;

            var hash = _hashFunction.ComputeHash(key);
            foreach (var kvp in _ring)
            {
                if (kvp.Key >= hash)
                {
                    return _shards.GetValueOrDefault(kvp.Value);
                }
            }
            var firstShardId = _ring.First().Value;
            return _shards.GetValueOrDefault(firstShardId);
        }
    }
    
    public IReadOnlyCollection<ShardInfo> GetAllShards()
    {
        return _shards.Values.ToList();
    }
    
    public IReadOnlyDictionary<uint, string> GetRingState()
    {
        lock (_lock)
        {
            return new Dictionary<uint, string>(_ring);
        }
    }

    public int ShardCount()
    {
        return _shards.Count;
    }
}