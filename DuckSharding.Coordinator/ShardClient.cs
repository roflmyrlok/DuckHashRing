using DuckSharding.Shared.Models;
using System.Text;
using System.Text.Json;

namespace DuckSharding.Coordinator;

public class ShardClient
{
    private readonly IHttpClientFactory _httpClientFactory;

    public ShardClient(IHttpClientFactory httpClientFactory)
    {
        _httpClientFactory = httpClientFactory;
    }

    public async Task<HttpResponseMessage> UpsertDuckAsync(ShardInfo shard, Duck duck)
    {
        var client = _httpClientFactory.CreateClient();
        var json = JsonSerializer.Serialize(duck);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        return await client.PutAsync($"{shard.GetBaseUrl()}/internal/ducks", content);
    }

    public async Task<Duck?> GetDuckAsync(ShardInfo shard, string species, DateTime birthDate)
    {
        var client = _httpClientFactory.CreateClient();
        var url = $"{shard.GetBaseUrl()}/internal/ducks?species={Uri.EscapeDataString(species)}&birthDate={birthDate:O}";
        
        var response = await client.GetAsync(url);
        if (!response.IsSuccessStatusCode) return null;

        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<Duck>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
    }

    public async Task<bool> DuckExistsAsync(ShardInfo shard, string species, DateTime birthDate)
    {
        var client = _httpClientFactory.CreateClient();
        var url = $"{shard.GetBaseUrl()}/internal/ducks?species={Uri.EscapeDataString(species)}&birthDate={birthDate:O}";
        
        var request = new HttpRequestMessage(HttpMethod.Head, url);
        var response = await client.SendAsync(request);
        
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> DeleteDuckAsync(ShardInfo shard, string species, DateTime birthDate)
    {
        var client = _httpClientFactory.CreateClient();
        var url = $"{shard.GetBaseUrl()}/internal/ducks?species={Uri.EscapeDataString(species)}&birthDate={birthDate:O}";
        
        var response = await client.DeleteAsync(url);
        return response.IsSuccessStatusCode;
    }

    public async Task<List<Duck>> QueryDucksBySpeciesAsync(ShardInfo shard, string species)
    {
        var client = _httpClientFactory.CreateClient();
        var url = $"{shard.GetBaseUrl()}/internal/ducks/query?species={Uri.EscapeDataString(species)}";
        
        var response = await client.GetAsync(url);
        if (!response.IsSuccessStatusCode) return new List<Duck>();

        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<List<Duck>>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) 
               ?? new List<Duck>();
    }
}