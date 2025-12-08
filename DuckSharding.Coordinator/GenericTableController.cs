using Microsoft.AspNetCore.Mvc;
using DuckSharding.Shared.Models;
using DuckSharding.Shared.Metrics;
using System.Diagnostics;

namespace DuckSharding.Coordinator;

[ApiController]
[Route("api/tables")]
public class GenericTableController : ControllerBase
{
    private readonly ReplicaAwareCoordinator _replicaCoordinator;
    private readonly GenericTableClient _tableClient;
    private readonly ILogger<GenericTableController> _logger;

    public GenericTableController(
        ReplicaAwareCoordinator replicaCoordinator, 
        GenericTableClient tableClient,
        ILogger<GenericTableController> logger)
    {
        _replicaCoordinator = replicaCoordinator;
        _tableClient = tableClient;
        _logger = logger;
    }

    [HttpPost("register")]
    public async Task<IActionResult> RegisterTable([FromBody] TableDefinition tableDefinition)
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Registering table {TableName}", tableDefinition.TableName);
            
            tableDefinition.Validate();
        }
        catch (ArgumentException ex)
        {
            status = "validation_error";
            _logger.LogWarning(ex, "Table validation failed for {TableName}", tableDefinition.TableName);
            return BadRequest(new { error = ex.Message });
        }
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("register_table", tableDefinition.TableName ?? "unknown")
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("register_table", tableDefinition.TableName ?? "unknown", status)
                .Inc();
        }

        var shardIds = _replicaCoordinator.GetAllShardIds();
        if (shardIds.Count == 0)
        {
            status = "no_shards";
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("register_table", tableDefinition.TableName, status)
                .Inc();
            return StatusCode(503, new { error = "No shards available" });
        }

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
            status = "partial_failure";
            _logger.LogError("Failed to register table {TableName} on leaders: {FailedLeaders}", 
                tableDefinition.TableName, string.Join(", ", failedLeaders));
            
            return StatusCode(500, new
            {
                error = "Failed to register table on some leaders",
                failedLeaders = failedLeaders
            });
        }

        _logger.LogInformation("Successfully registered table {TableName} on all shard leaders", tableDefinition.TableName);
        return Ok(new { message = $"Table '{tableDefinition.TableName}' registered successfully on all shard leaders" });
    }

    [HttpGet("{tableName}")]
    public async Task<IActionResult> GetTableDefinition(string tableName)
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Getting table definition for {TableName}", tableName);

            var shardIds = _replicaCoordinator.GetAllShardIds();
            if (shardIds.Count == 0)
            {
                status = "no_shards";
                return StatusCode(503, new { error = "No shards available" });
            }

            var replica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
            if (replica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var tableDef = await _tableClient.GetTableDefinitionAsync(replica, tableName);

            if (tableDef == null)
            {
                status = "not_found";
                return NotFound(new { error = $"Table '{tableName}' not found" });
            }

            return Ok(tableDef);
        }
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("get_table", tableName)
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("get_table", tableName, status)
                .Inc();
        }
    }

    [HttpGet]
    public async Task<IActionResult> GetAllTables()
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Getting all tables");

            var shardIds = _replicaCoordinator.GetAllShardIds();
            if (shardIds.Count == 0)
            {
                status = "no_shards";
                return StatusCode(503, new { error = "No shards available" });
            }

            var replica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
            if (replica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var tables = await _tableClient.GetAllTablesAsync(replica);

            return Ok(tables);
        }
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("get_all_tables", "all")
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("get_all_tables", "all", status)
                .Inc();
        }
    }

    [HttpPost("{tableName}/exists")]
    public async Task<IActionResult> Exists(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Checking existence in table {TableName}", tableName);

            var shardIds = _replicaCoordinator.GetAllShardIds();
            if (shardIds.Count == 0)
            {
                status = "no_shards";
                return StatusCode(503, new { error = "No shards available" });
            }

            var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
            if (firstReplica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
            
            if (tableDef == null)
            {
                status = "not_found";
                return NotFound(new { error = $"Table '{tableName}' not found" });
            }

            string compositeKey;
            try
            {
                compositeKey = tableDef.GetCompositePrimaryKey(primaryKey);
            }
            catch (ArgumentException ex)
            {
                status = "bad_request";
                return BadRequest(new { error = ex.Message });
            }

            var replica = _replicaCoordinator.GetReplicaForRead(compositeKey);
            if (replica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var exists = await _tableClient.ExistsAsync(replica, tableName, primaryKey);

            status = exists ? "exists" : "not_found";
            return exists ? Ok() : NotFound();
        }
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("exists", tableName)
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("exists", tableName, status)
                .Inc();
        }
    }

    [HttpPost("{tableName}/create")]
    public async Task<IActionResult> Create(string tableName, [FromBody] Dictionary<string, object> record)
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Creating record in table {TableName}", tableName);

            var shardIds = _replicaCoordinator.GetAllShardIds();
            if (shardIds.Count == 0)
            {
                status = "no_shards";
                return StatusCode(503, new { error = "No shards available" });
            }

            var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
            if (firstReplica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
            
            if (tableDef == null)
            {
                status = "not_found";
                return NotFound(new { error = $"Table '{tableName}' not found" });
            }

            string compositeKey;
            try
            {
                compositeKey = tableDef.GetCompositePrimaryKey(record);
            }
            catch (ArgumentException ex)
            {
                status = "bad_request";
                return BadRequest(new { error = ex.Message });
            }

            var leader = _replicaCoordinator.GetLeaderForKey(compositeKey);
            if (leader == null)
            {
                status = "no_leader";
                return StatusCode(503, new { error = "No leader available" });
            }

            var response = await _tableClient.CreateAsync(leader, tableName, record);

            if (!response.IsSuccessStatusCode)
            {
                status = "error";
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
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("create", tableName)
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("create", tableName, status)
                .Inc();
        }
    }

    [HttpPost("{tableName}/read")]
    public async Task<IActionResult> Read(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Reading from table {TableName}", tableName);

            var shardIds = _replicaCoordinator.GetAllShardIds();
            if (shardIds.Count == 0)
            {
                status = "no_shards";
                return StatusCode(503, new { error = "No shards available" });
            }

            var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
            if (firstReplica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
            
            if (tableDef == null)
            {
                status = "not_found";
                return NotFound(new { error = $"Table '{tableName}' not found" });
            }

            string compositeKey;
            try
            {
                compositeKey = tableDef.GetCompositePrimaryKey(primaryKey);
            }
            catch (ArgumentException ex)
            {
                status = "bad_request";
                return BadRequest(new { error = ex.Message });
            }

            var replica = _replicaCoordinator.GetReplicaForRead(compositeKey);
            if (replica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var record = await _tableClient.ReadAsync(replica, tableName, primaryKey);

            if (record == null)
            {
                status = "not_found";
                return NotFound();
            }

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
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("read", tableName)
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("read", tableName, status)
                .Inc();
        }
    }

    [HttpPost("{tableName}/update")]
    public async Task<IActionResult> Update(string tableName, [FromBody] Dictionary<string, object> record)
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Updating record in table {TableName}", tableName);

            var shardIds = _replicaCoordinator.GetAllShardIds();
            if (shardIds.Count == 0)
            {
                status = "no_shards";
                return StatusCode(503, new { error = "No shards available" });
            }

            var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
            if (firstReplica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
            
            if (tableDef == null)
            {
                status = "not_found";
                return NotFound(new { error = $"Table '{tableName}' not found" });
            }

            string compositeKey;
            try
            {
                compositeKey = tableDef.GetCompositePrimaryKey(record);
            }
            catch (ArgumentException ex)
            {
                status = "bad_request";
                return BadRequest(new { error = ex.Message });
            }

            var leader = _replicaCoordinator.GetLeaderForKey(compositeKey);
            if (leader == null)
            {
                status = "no_leader";
                return StatusCode(503, new { error = "No leader available" });
            }

            var response = await _tableClient.UpdateAsync(leader, tableName, record);

            if (!response.IsSuccessStatusCode)
            {
                status = "error";
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
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("update", tableName)
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("update", tableName, status)
                .Inc();
        }
    }

    [HttpPost("{tableName}/delete")]
    public async Task<IActionResult> Delete(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        var stopwatch = Stopwatch.StartNew();
        var status = "success";

        try
        {
            _logger.LogInformation("Deleting from table {TableName}", tableName);

            var shardIds = _replicaCoordinator.GetAllShardIds();
            if (shardIds.Count == 0)
            {
                status = "no_shards";
                return StatusCode(503, new { error = "No shards available" });
            }

            var firstReplica = _replicaCoordinator.GetAnyReplicaFromShard(shardIds.First());
            if (firstReplica == null)
            {
                status = "no_replicas";
                return StatusCode(503, new { error = "No replicas available" });
            }

            var tableDef = await _tableClient.GetTableDefinitionAsync(firstReplica, tableName);
            
            if (tableDef == null)
            {
                status = "not_found";
                return NotFound(new { error = $"Table '{tableName}' not found" });
            }

            string compositeKey;
            try
            {
                compositeKey = tableDef.GetCompositePrimaryKey(primaryKey);
            }
            catch (ArgumentException ex)
            {
                status = "bad_request";
                return BadRequest(new { error = ex.Message });
            }

            var leader = _replicaCoordinator.GetLeaderForKey(compositeKey);
            if (leader == null)
            {
                status = "no_leader";
                return StatusCode(503, new { error = "No leader available" });
            }

            var deleted = await _tableClient.DeleteAsync(leader, tableName, primaryKey);

            if (!deleted)
            {
                status = "not_found";
                return NotFound();
            }

            return Ok(new { 
                message = "Record deleted successfully",
                shard = leader.ShardId,
                replica = leader.ShardId,
                isLeader = leader.IsLeader
            });
        }
        finally
        {
            stopwatch.Stop();
            MetricsRegistry.CoordinatorRequestDuration
                .WithLabels("delete", tableName)
                .Observe(stopwatch.Elapsed.TotalSeconds);
            MetricsRegistry.CoordinatorRequestsTotal
                .WithLabels("delete", tableName, status)
                .Inc();
        }
    }
}