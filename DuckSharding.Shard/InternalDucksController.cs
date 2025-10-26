using Microsoft.AspNetCore.Mvc;
using DuckSharding.Shared.Models;

namespace DuckSharding.Shard;

[ApiController]
[Route("internal/ducks")]
public class InternalDucksController : ControllerBase
{
	private readonly DuckRepository _repository;

	public InternalDucksController(DuckRepository repository)
	{
		_repository = repository;
	}

	[HttpPut]
	public async Task<IActionResult> UpsertDuck([FromBody] Duck duck)
	{
		await _repository.UpsertAsync(duck);
		return Ok();
	}

	[HttpGet]
	public async Task<IActionResult> GetDuck([FromQuery] string species, [FromQuery] DateTime birthDate)
	{
		var duck = await _repository.GetAsync(species, birthDate);
		if (duck == null) return NotFound();
		return Ok(duck);
	}

	[HttpHead]
	public async Task<IActionResult> DuckExists([FromQuery] string species, [FromQuery] DateTime birthDate)
	{
		var exists = await _repository.ExistsAsync(species, birthDate);
		return exists ? Ok() : NotFound();
	}

	[HttpDelete]
	public async Task<IActionResult> DeleteDuck([FromQuery] string species, [FromQuery] DateTime birthDate)
	{
		var deleted = await _repository.DeleteAsync(species, birthDate);
		if (!deleted) return NotFound();
		return Ok();
	}

	[HttpGet("query")]
	public async Task<IActionResult> QueryDucks([FromQuery] string species)
	{
		var ducks = await _repository.QueryBySpeciesAsync(species);
		return Ok(ducks);
	}
}