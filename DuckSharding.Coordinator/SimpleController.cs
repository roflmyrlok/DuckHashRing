using Microsoft.AspNetCore.Mvc;
using DuckSharding.Shared.Models;

namespace DuckSharding.Coordinator;

[ApiController]
[Route("api")]
public class SimpleController : ControllerBase
{
    private readonly Coordinator _coordinator;
    private readonly ShardClient _shardClient;

    public SimpleController(
        Coordinator coordinator, 
        ShardClient shardClient)
    {
        _coordinator = coordinator;
        _shardClient = shardClient;
    }
    
    [HttpGet("shards")]
    public IActionResult GetAllShards()
    {
        var shards = _coordinator.GetAllShards();
        return Ok(shards);
    }

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