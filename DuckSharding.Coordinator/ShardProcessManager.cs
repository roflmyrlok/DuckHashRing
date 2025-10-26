using System.Diagnostics;
using DuckSharding.Shared.Models;

namespace DuckSharding.Coordinator;

public class ShardProcessManager
{
    private readonly Dictionary<string, Process> _processes = new();
    private readonly Coordinator _coordinator;
    private int _nextPort = 5001;

    public ShardProcessManager(Coordinator coordinator)
    {
        _coordinator = coordinator;
    }

    public async Task<ShardInfo> StartShardAsync(string? shardId = null)
    {
        shardId ??= $"shard-{Guid.NewGuid().ToString()[..8]}";
        var port = _nextPort++;
        var dbFileName = $"shard{port}.db";
        
        var shardProjectPath = Path.Combine(
            Directory.GetCurrentDirectory(),
            "..",
            "DuckSharding.Shard",
            "DuckSharding.Shard.csproj"
        );

        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = $"run --project \"{shardProjectPath}\" -- {port} {shardId} {dbFileName}",
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            }
        };

        process.Start();
        _processes[shardId] = process;

        await Task.Delay(3000);
        var shardInfo = new ShardInfo(shardId, "localhost", port);
        _coordinator.AddShard(shardInfo);

        return shardInfo;
    }

    public bool StopShard(string shardId)
    {
        if (!_processes.TryGetValue(shardId, out var process))
            return false;

        try
        {
            process.Kill(true);
            process.Dispose();
            _processes.Remove(shardId);
            _coordinator.RemoveShard(shardId);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public IReadOnlyDictionary<string, bool> GetShardProcesses()
    {
        return _processes.ToDictionary(
            kvp => kvp.Key,
            kvp => !kvp.Value.HasExited
        );
    }
}