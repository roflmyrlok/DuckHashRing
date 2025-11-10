using DuckSharding.Shared.Models;

namespace DuckSharding.Coordinator;

public class ReplicaAwareCoordinator
{
    private readonly Coordinator _coordinator;
    private readonly Dictionary<string, ShardReplicaSet> _replicaSets = new();
    private readonly Dictionary<string, int> _readCounters = new();
    private readonly object _lock = new();

    public ReplicaAwareCoordinator(Coordinator coordinator)
    {
        _coordinator = coordinator;
    }

    public void RegisterReplicaSet(string shardId, ShardInfo leader, List<ShardInfo> followers)
    {
        lock (_lock)
        {
            var allReplicas = new List<ShardInfo> { leader };
            allReplicas.AddRange(followers);

            _replicaSets[shardId] = new ShardReplicaSet(shardId, leader, allReplicas);
            _readCounters[shardId] = 0;
        }
    }

    public ShardInfo? GetLeaderForKey(string key)
    {
        var shard = _coordinator.GetShardForKey(key);
        if (shard == null) return null;

        lock (_lock)
        {
            if (_replicaSets.TryGetValue(shard.ShardId, out var replicaSet))
            {
                return replicaSet.Leader;
            }
        }

        return null;
    }

    public ShardInfo? GetReplicaForRead(string key)
    {
        var shard = _coordinator.GetShardForKey(key);
        if (shard == null) return null;

        lock (_lock)
        {
            if (_replicaSets.TryGetValue(shard.ShardId, out var replicaSet))
            {
                var counter = _readCounters[shard.ShardId];
                var replica = replicaSet.AllReplicas[counter % replicaSet.AllReplicas.Count];
                _readCounters[shard.ShardId] = (counter + 1) % replicaSet.AllReplicas.Count;
                return replica;
            }
        }

        return null;
    }

    public ShardInfo? GetAnyReplicaFromShard(string shardId)
    {
        lock (_lock)
        {
            if (_replicaSets.TryGetValue(shardId, out var replicaSet))
            {
                return replicaSet.AllReplicas.FirstOrDefault();
            }
        }

        return null;
    }

    public ShardInfo? GetLeaderFromShard(string shardId)
    {
        lock (_lock)
        {
            if (_replicaSets.TryGetValue(shardId, out var replicaSet))
            {
                return replicaSet.Leader;
            }
        }

        return null;
    }

    public List<string> GetAllShardIds()
    {
        lock (_lock)
        {
            return _replicaSets.Keys.ToList();
        }
    }
}

public class ShardReplicaSet
{
    public string ShardId { get; set; }
    public ShardInfo Leader { get; set; }
    public List<ShardInfo> AllReplicas { get; set; }

    public ShardReplicaSet(string shardId, ShardInfo leader, List<ShardInfo> allReplicas)
    {
        ShardId = shardId;
        Leader = leader;
        AllReplicas = allReplicas;
    }
}