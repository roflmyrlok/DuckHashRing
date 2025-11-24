using Microsoft.AspNetCore.Mvc;
namespace DuckSharding.Shard;

[ApiController]
[Route("internal/replication")]
public class InternalReplicationController : ControllerBase
{
    private readonly ReplicationLogRepository _replicationLog;
    private readonly ILogger<InternalReplicationController> _logger;

    public InternalReplicationController(
        ReplicationLogRepository replicationLog,
        ILogger<InternalReplicationController> logger)
    {
        _replicationLog = replicationLog;
        _logger = logger;
    }

    [HttpGet("latest-sequence")]
    public IActionResult GetLatestSequence()
    {
        try
        {
            var latestSequence = _replicationLog.GetLatestSequenceNumber();
            return Ok(new { latestSequence });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting latest sequence");
            return StatusCode(500, new { error = "Failed to get latest sequence" });
        }
    }

    [HttpGet("events")]
    public IActionResult GetEvents([FromQuery] long fromSequence, [FromQuery] int? limit = 1000)
    {
        try
        {
            var events = _replicationLog.GetEventsAfter(fromSequence);
            
            if (limit.HasValue && limit.Value > 0)
            {
                events = events.Take(limit.Value).ToList();
            }

            _logger.LogInformation($"Serving {events.Count} catchup events from sequence {fromSequence}");
            
            return Ok(events);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting events for catchup");
            return StatusCode(500, new { error = "Failed to get catchup events" });
        }
    }

    [HttpGet("event/{sequenceNumber}")]
    public IActionResult GetEvent(long sequenceNumber)
    {
        try
        {
            var evt = _replicationLog.GetEvent(sequenceNumber);
            
            if (evt == null)
                return NotFound(new { error = $"Event {sequenceNumber} not found" });

            return Ok(evt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error getting event {sequenceNumber}");
            return StatusCode(500, new { error = "Failed to get event" });
        }
    }

    [HttpGet("check-gap")]
    public IActionResult CheckGap([FromQuery] long fromSequence, [FromQuery] long toSequence)
    {
        try
        {
            var missingSequences = _replicationLog.GetMissingSequences(fromSequence, toSequence);
            
            return Ok(new 
            { 
                hasGap = missingSequences.Any(),
                missingSequences,
                count = missingSequences.Count
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking sequence gap");
            return StatusCode(500, new { error = "Failed to check gap" });
        }
    }
}