using Microsoft.AspNetCore.Mvc;
using DuckSharding.Shared.Models;

namespace DuckSharding.Coordinator;

[ApiController]
[Route("api/tables")]
public class GenericTableController : ControllerBase
{
    private readonly Coordinator _coordinator;
    private readonly GenericTableClient _tableClient;

    public GenericTableController(Coordinator coordinator, GenericTableClient tableClient)
    {
        _coordinator = coordinator;
        _tableClient = tableClient;
    }

    [HttpPost("register")]
    public async Task<IActionResult> RegisterTable([FromBody] TableDefinition tableDefinition)
    {
        try
        {
            tableDefinition.Validate();
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }

        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var tasks = shards.Select(shard => _tableClient.RegisterTableAsync(shard, tableDefinition)).ToList();
        var results = await Task.WhenAll(tasks);

        var failedShards = results
            .Select((result, index) => new { result, shard = shards.ElementAt(index) })
            .Where(x => !x.result.IsSuccessStatusCode)
            .Select(x => x.shard.ShardId)
            .ToList();

        if (failedShards.Any())
        {
            return StatusCode(500, new
            {
                error = "Failed to register table on some shards",
                failedShards = failedShards
            });
        }

        return Ok(new { message = $"Table '{tableDefinition.TableName}' registered successfully on all shards" });
    }

    [HttpGet("{tableName}")]
    public async Task<IActionResult> GetTableDefinition(string tableName)
    {
        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var shard = shards.First();
        var tableDef = await _tableClient.GetTableDefinitionAsync(shard, tableName);

        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });

        return Ok(tableDef);
    }

    [HttpGet]
    public async Task<IActionResult> GetAllTables()
    {
        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var shard = shards.First();
        var tables = await _tableClient.GetAllTablesAsync(shard);

        return Ok(tables);
    }

    [HttpPost("{tableName}/exists")]
    public async Task<IActionResult> Exists(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstShard = shards.First();
        var tableDef = await _tableClient.GetTableDefinitionAsync(firstShard, tableName);
        
        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });

        string compositeKey;
        try
        {
            compositeKey = tableDef.GetCompositePrimaryKey(primaryKey);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }

        var shard = _coordinator.GetShardForKey(compositeKey);
        if (shard == null)
            return StatusCode(503, new { error = "No shards available" });

        var exists = await _tableClient.ExistsAsync(shard, tableName, primaryKey);

        return exists ? Ok() : NotFound();
    }

    [HttpPost("{tableName}/create")]
    public async Task<IActionResult> Create(string tableName, [FromBody] Dictionary<string, object> record)
    {
        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstShard = shards.First();
        var tableDef = await _tableClient.GetTableDefinitionAsync(firstShard, tableName);
        
        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });

        string compositeKey;
        try
        {
            compositeKey = tableDef.GetCompositePrimaryKey(record);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }

        var shard = _coordinator.GetShardForKey(compositeKey);
        if (shard == null)
            return StatusCode(503, new { error = "No shards available" });

        var response = await _tableClient.CreateAsync(shard, tableName, record);

        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            return StatusCode((int)response.StatusCode, new { error = errorContent });
        }

        return Ok(new { message = "Record created successfully", shard = shard.ShardId });
    }

    [HttpPost("{tableName}/read")]
    public async Task<IActionResult> Read(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstShard = shards.First();
        var tableDef = await _tableClient.GetTableDefinitionAsync(firstShard, tableName);
        
        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });

        string compositeKey;
        try
        {
            compositeKey = tableDef.GetCompositePrimaryKey(primaryKey);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }

        var shard = _coordinator.GetShardForKey(compositeKey);
        if (shard == null)
            return StatusCode(503, new { error = "No shards available" });

        var record = await _tableClient.ReadAsync(shard, tableName, primaryKey);

        if (record == null)
            return NotFound();

        return Ok(record);
    }

    [HttpPost("{tableName}/update")]
    public async Task<IActionResult> Update(string tableName, [FromBody] Dictionary<string, object> record)
    {
        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstShard = shards.First();
        var tableDef = await _tableClient.GetTableDefinitionAsync(firstShard, tableName);
        
        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });

        string compositeKey;
        try
        {
            compositeKey = tableDef.GetCompositePrimaryKey(record);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }

        var shard = _coordinator.GetShardForKey(compositeKey);
        if (shard == null)
            return StatusCode(503, new { error = "No shards available" });

        var response = await _tableClient.UpdateAsync(shard, tableName, record);

        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            return StatusCode((int)response.StatusCode, new { error = errorContent });
        }

        return Ok(new { message = "Record updated successfully", shard = shard.ShardId });
    }

    [HttpPost("{tableName}/delete")]
    public async Task<IActionResult> Delete(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var shards = _coordinator.GetAllShards();
        if (shards.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstShard = shards.First();
        var tableDef = await _tableClient.GetTableDefinitionAsync(firstShard, tableName);
        
        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });

        string compositeKey;
        try
        {
            compositeKey = tableDef.GetCompositePrimaryKey(primaryKey);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }

        var shard = _coordinator.GetShardForKey(compositeKey);
        if (shard == null)
            return StatusCode(503, new { error = "No shards available" });

        var deleted = await _tableClient.DeleteAsync(shard, tableName, primaryKey);

        if (!deleted)
            return NotFound();

        return Ok(new { message = "Record deleted successfully" });
    }
}