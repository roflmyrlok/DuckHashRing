using Microsoft.AspNetCore.Mvc;
using DuckSharding.Shared.Models;

namespace DuckSharding.Coordinator;

[ApiController]
[Route("api")]
public class SimpleController : ControllerBase
{
    private readonly Coordinator _coordinator;
    private readonly ShardClient _shardClient;
    private readonly ShardProcessManager _processManager;

    public SimpleController(
        Coordinator coordinator, 
        ShardClient shardClient,
        ShardProcessManager processManager)
    {
        _coordinator = coordinator;
        _shardClient = shardClient;
        _processManager = processManager;
    }

    // shard coord part
    
    [HttpPost("shards/start")]
    public async Task<IActionResult> StartShard([FromQuery] string? shardId = null)
    {
        var shard = await _processManager.StartShardAsync(shardId);
        return Ok(new 
        { 
            message = "Shard started successfully", 
            shard = shard 
        });
    }

    [HttpPost("shards/stop/{shardId}")]
    public async Task<IActionResult> StopShard(string shardId)
    {
        var stopped = await _processManager.StopShardAsync(shardId);
        if (!stopped) return NotFound();
    
        return Ok(new { message = $"Shard {shardId} stopped successfully" });
    }

    [HttpGet("shards/processes")]
    public async Task<IActionResult> GetShardProcesses()
    {
        var processes = await _processManager.GetShardProcessesAsync();
        return Ok(processes);
    }


    [HttpPost("shards/register")]
    public IActionResult RegisterShard([FromBody] ShardInfo shard)
    {
        _coordinator.AddShard(shard);
        return Ok(new { message = $"Shard {shard.ShardId} registered successfully" });
    }

    [HttpDelete("shards/{shardId}")]
    public IActionResult RemoveShard(string shardId)
    {
        var removed = _coordinator.RemoveShard(shardId);
        if (!removed) return NotFound();
        
        return Ok(new { message = $"Shard {shardId} removed successfully" });
    }

    [HttpGet("shards")]
    public IActionResult GetAllShards()
    {
        var shards = _coordinator.GetAllShards();
        return Ok(shards);
    }
    // duck part

    [HttpPut("ducks")]
    public async Task<IActionResult> CreateDuck([FromBody] Duck duck)
    {
        var compositeKey = duck.GetCompositeKey();
        var shard = _coordinator.GetShardForKey(compositeKey);
    
        if (shard == null)
            return StatusCode(503, new {error = "No shards available"});

        var response = await _shardClient.UpsertDuckAsync(shard, duck);
    
        if (!response.IsSuccessStatusCode)
            return StatusCode((int)response.StatusCode);

        return Ok(new { message = "Duck created successfully", shard = shard.ShardId });
    }

    [HttpGet("ducks")]
    public async Task<IActionResult> GetDuck([FromQuery] string species, [FromQuery] DateTime birthDate)
    {
        var compositeKey = Duck.GetCompositeKey(species, birthDate);
        var shard = _coordinator.GetShardForKey(compositeKey);
    
        if (shard == null)
            return StatusCode(503, new {error = "No shards available"});

        var duck = await _shardClient.GetDuckAsync(shard, species, birthDate);
    
        if (duck == null)
            return NotFound();

        return Ok(duck);
    }

    [HttpHead("ducks")]
    public async Task<IActionResult> DuckExists([FromQuery] string species, [FromQuery] DateTime birthDate)
    {
        var compositeKey = Duck.GetCompositeKey(species, birthDate);
        var shard = _coordinator.GetShardForKey(compositeKey);
    
        if (shard == null)
            return StatusCode(503, new {error = "No shards available"});

        var exists = await _shardClient.DuckExistsAsync(shard, species, birthDate);
    
        return exists ? Ok() : NotFound();
    }

    [HttpDelete("ducks")]
    public async Task<IActionResult> DeleteDuck([FromQuery] string species, [FromQuery] DateTime birthDate)
    {
        var compositeKey = Duck.GetCompositeKey(species, birthDate);
        var shard = _coordinator.GetShardForKey(compositeKey);
    
        if (shard == null)
            return StatusCode(503, new {error = "No shards available"});

        var deleted = await _shardClient.DeleteDuckAsync(shard, species, birthDate);
    
        if (!deleted)
            return NotFound();

        return Ok(new { message = "Duck deleted successfully" });
    }

    // view ring

    [HttpGet("ring")]
    public IActionResult GetRing()
    {
        var ringState = _coordinator.GetRingState();
        var shards = _coordinator.GetAllShards();

        return Ok(new
        {
            shardCount = shards.Count,
            nodeCount = ringState.Count,
            shards = shards.Select(s => new
            {
                shardId = s.ShardId,
                host = s.Host,
                port = s.Port,
                isHealthy = s.IsHealthy
            }),
            ring = ringState.Select(kvp => new
            {
                hash = kvp.Key,
                shardId = kvp.Value
            }).OrderBy(x => x.hash)
        });
    }
}