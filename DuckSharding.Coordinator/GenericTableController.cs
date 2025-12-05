using Microsoft.AspNetCore.Mvc;
using DuckSharding.Shared.Models;

namespace DuckSharding.Coordinator;

[ApiController]
[Route("api/tables")]
public class GenericTableController : ControllerBase
{
    private readonly ReplicaAwareCoordinator _replicaCoordinator;
    private readonly GenericTableClient _tableClient;

    public GenericTableController(ReplicaAwareCoordinator replicaCoordinator, GenericTableClient tableClient)
    {
        _replicaCoordinator = replicaCoordinator;
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

        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var tasks = new List<Task<HttpResponseMessage>>();
        var targetLeaders = new List<ShardInfo>();

        foreach (var shardId in shardIds)
        {
            var leader = _replicaCoordinator.GetLeaderFromShard(shardId);
            if (leader != null)
            {
                tasks.Add(_tableClient.RegisterTableAsync(leader, tableDefinition));
                targetLeaders.Add(leader);
            }
        }

        var results = await Task.WhenAll(tasks);

        var failedLeaders = results
            .Select((result, index) => new { result, leader = targetLeaders[index] })
            .Where(x => !x.result.IsSuccessStatusCode)
            .Select(x => x.leader.ShardId)
            .ToList();

        if (failedLeaders.Any())
        {
            return StatusCode(500, new
            {
                error = "Failed to register table on some leaders",
                failedLeaders = failedLeaders
            });
        }

        return Ok(new { message = $"Table '{tableDefinition.TableName}' registered successfully on all shard leaders" });
    }

    [HttpGet("{tableName}")]
    public async Task<IActionResult> GetTableDefinition(string tableName)
    {
        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var replica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
        if (replica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var tableDef = await _tableClient.GetTableDefinitionAsync(replica, tableName);

        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });

        return Ok(tableDef);
    }

    [HttpGet]
    public async Task<IActionResult> GetAllTables()
    {
        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var replica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
        if (replica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var tables = await _tableClient.GetAllTablesAsync(replica);

        return Ok(tables);
    }

    [HttpPost("{tableName}/exists")]
    public async Task<IActionResult> Exists(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
        if (firstReplica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
        
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

        var replica = _replicaCoordinator.GetReplicaForRead(compositeKey);
        if (replica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var exists = await _tableClient.ExistsAsync(replica, tableName, primaryKey);

        return exists ? Ok() : NotFound();
    }

    [HttpPost("{tableName}/create")]
    public async Task<IActionResult> Create(string tableName, [FromBody] Dictionary<string, object> record)
    {
        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
        if (firstReplica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
        
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

        var leader = _replicaCoordinator.GetLeaderForKey(compositeKey);
        if (leader == null)
            return StatusCode(503, new { error = "No leader available" });

        var response = await _tableClient.CreateAsync(leader, tableName, record);

        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            return StatusCode((int)response.StatusCode, new { error = errorContent });
        }

        return Ok(new { 
            message = "Record created successfully", 
            shard = leader.ShardId,
            replica = leader.ShardId,
            isLeader = leader.IsLeader
        });
    }

    [HttpPost("{tableName}/read")]
    public async Task<IActionResult> Read(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
        if (firstReplica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
        
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

        var replica = _replicaCoordinator.GetReplicaForRead(compositeKey);
        if (replica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var record = await _tableClient.ReadAsync(replica, tableName, primaryKey);

        if (record == null)
            return NotFound();

        // Add metadata about which replica served the request
        var response = new Dictionary<string, object>
        {
            { "data", record },
            { "metadata", new {
                replica = replica.ShardId,
                replicaHost = replica.Host,
                isLeader = replica.IsLeader,
                shard = replica.ShardId.Split('-')[0] + "-" + replica.ShardId.Split('-')[1]
            }}
        };

        return Ok(response);
    }

    [HttpPost("{tableName}/update")]
    public async Task<IActionResult> Update(string tableName, [FromBody] Dictionary<string, object> record)
    {
        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
        if (firstReplica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
        
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

        var leader = _replicaCoordinator.GetLeaderForKey(compositeKey);
        if (leader == null)
            return StatusCode(503, new { error = "No leader available" });

        var response = await _tableClient.UpdateAsync(leader, tableName, record);

        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            return StatusCode((int)response.StatusCode, new { error = errorContent });
        }

        return Ok(new { 
            message = "Record updated successfully", 
            shard = leader.ShardId,
            replica = leader.ShardId,
            isLeader = leader.IsLeader
        });
    }

    [HttpPost("{tableName}/delete")]
    public async Task<IActionResult> Delete(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
            return StatusCode(503, new { error = "No shards available" });

        var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
        if (firstReplica == null)
            return StatusCode(503, new { error = "No replicas available" });

        var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
        
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

        var leader = _replicaCoordinator.GetLeaderForKey(compositeKey);
        if (leader == null)
            return StatusCode(503, new { error = "No leader available" });

        var deleted = await _tableClient.DeleteAsync(leader, tableName, primaryKey);

        if (!deleted)
            return NotFound();

        return Ok(new { 
            message = "Record deleted successfully",
            shard = leader.ShardId,
            replica = leader.ShardId,
            isLeader = leader.IsLeader
        });
    }
}