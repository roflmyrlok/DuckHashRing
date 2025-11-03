using Microsoft.AspNetCore.Mvc;
using DuckSharding.Shared.Models;

namespace DuckSharding.Shard;

[ApiController]
[Route("internal/tables")]
public class InternalTablesController : ControllerBase
{
    private readonly GenericRepository _repository;

    public InternalTablesController(GenericRepository repository)
    {
        _repository = repository;
    }

    [HttpPost("register")]
    public IActionResult RegisterTable([FromBody] TableDefinition tableDefinition)
    {
        try
        {
            _repository.RegisterTable(tableDefinition);
            return Ok(new { message = $"Table '{tableDefinition.TableName}' registered successfully" });
        }
        catch (InvalidOperationException ex)
        {
            return Conflict(new { error = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpGet]
    public IActionResult GetAllTables()
    {
        var tables = _repository.GetAllTables();
        return Ok(tables);
    }

    [HttpGet("{tableName}")]
    public IActionResult GetTableDefinition(string tableName)
    {
        var tableDef = _repository.GetTableDefinition(tableName);
        if (tableDef == null)
            return NotFound(new { error = $"Table '{tableName}' not found" });
        
        return Ok(tableDef);
    }

    [HttpPost("{tableName}/exists")]
    public async Task<IActionResult> Exists(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        try
        {
            var exists = await _repository.ExistsAsync(tableName, primaryKey);
            return exists ? Ok() : NotFound();
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(new { error = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("{tableName}/create")]
    public async Task<IActionResult> Create(string tableName, [FromBody] Dictionary<string, object> record)
    {
        try
        {
            var created = await _repository.CreateAsync(tableName, record);
            if (!created)
                return Conflict(new { error = "Record already exists" });
            
            return Ok(new { message = "Record created successfully" });
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(new { error = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("{tableName}/read")]
    public async Task<IActionResult> Read(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        try
        {
            var record = await _repository.ReadAsync(tableName, primaryKey);
            if (record == null)
                return NotFound();
            
            return Ok(record);
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(new { error = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("{tableName}/update")]
    public async Task<IActionResult> Update(string tableName, [FromBody] Dictionary<string, object> record)
    {
        try
        {
            var updated = await _repository.UpdateAsync(tableName, record);
            if (!updated)
                return NotFound(new { error = "Record not found" });
            
            return Ok(new { message = "Record updated successfully" });
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(new { error = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    [HttpPost("{tableName}/delete")]
    public async Task<IActionResult> Delete(string tableName, [FromBody] Dictionary<string, object> primaryKey)
    {
        try
        {
            var deleted = await _repository.DeleteAsync(tableName, primaryKey);
            if (!deleted)
                return NotFound();
            
            return Ok(new { message = "Record deleted successfully" });
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(new { error = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }
}