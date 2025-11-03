using DuckSharding.Shared.Models;
using System.Text;
using System.Text.Json;

namespace DuckSharding.Coordinator;

public class GenericTableClient
{
    private readonly IHttpClientFactory _httpClientFactory;

    public GenericTableClient(IHttpClientFactory httpClientFactory)
    {
        _httpClientFactory = httpClientFactory;
    }

    public async Task<HttpResponseMessage> RegisterTableAsync(ShardInfo shard, TableDefinition tableDefinition)
    {
        var client = _httpClientFactory.CreateClient();
        var json = JsonSerializer.Serialize(tableDefinition);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        return await client.PostAsync($"{shard.GetBaseUrl()}/internal/tables/register", content);
    }

    public async Task<List<TableDefinition>> GetAllTablesAsync(ShardInfo shard)
    {
        var client = _httpClientFactory.CreateClient();
        var response = await client.GetAsync($"{shard.GetBaseUrl()}/internal/tables");
        
        if (!response.IsSuccessStatusCode) return new List<TableDefinition>();

        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<List<TableDefinition>>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) 
               ?? new List<TableDefinition>();
    }

    public async Task<TableDefinition?> GetTableDefinitionAsync(ShardInfo shard, string tableName)
    {
        var client = _httpClientFactory.CreateClient();
        var response = await client.GetAsync($"{shard.GetBaseUrl()}/internal/tables/{Uri.EscapeDataString(tableName)}");
        
        if (!response.IsSuccessStatusCode) return null;

        var json = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<TableDefinition>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
    }

    public async Task<bool> ExistsAsync(ShardInfo shard, string tableName, Dictionary<string, object> primaryKey)
    {
        var client = _httpClientFactory.CreateClient();
        var json = JsonSerializer.Serialize(primaryKey);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        var response = await client.PostAsync($"{shard.GetBaseUrl()}/internal/tables/{Uri.EscapeDataString(tableName)}/exists", content);
        return response.IsSuccessStatusCode;
    }

    public async Task<HttpResponseMessage> CreateAsync(ShardInfo shard, string tableName, Dictionary<string, object> record)
    {
        var client = _httpClientFactory.CreateClient();
        var json = JsonSerializer.Serialize(record);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        return await client.PostAsync($"{shard.GetBaseUrl()}/internal/tables/{Uri.EscapeDataString(tableName)}/create", content);
    }

    public async Task<Dictionary<string, object>?> ReadAsync(ShardInfo shard, string tableName, Dictionary<string, object> primaryKey)
    {
        var client = _httpClientFactory.CreateClient();
        var json = JsonSerializer.Serialize(primaryKey);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        var response = await client.PostAsync($"{shard.GetBaseUrl()}/internal/tables/{Uri.EscapeDataString(tableName)}/read", content);
        if (!response.IsSuccessStatusCode) return null;

        var responseJson = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<Dictionary<string, object>>(responseJson, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
    }

    public async Task<HttpResponseMessage> UpdateAsync(ShardInfo shard, string tableName, Dictionary<string, object> record)
    {
        var client = _httpClientFactory.CreateClient();
        var json = JsonSerializer.Serialize(record);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        return await client.PostAsync($"{shard.GetBaseUrl()}/internal/tables/{Uri.EscapeDataString(tableName)}/update", content);
    }

    public async Task<bool> DeleteAsync(ShardInfo shard, string tableName, Dictionary<string, object> primaryKey)
    {
        var client = _httpClientFactory.CreateClient();
        var json = JsonSerializer.Serialize(primaryKey);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        
        var response = await client.PostAsync($"{shard.GetBaseUrl()}/internal/tables/{Uri.EscapeDataString(tableName)}/delete", content);
        return response.IsSuccessStatusCode;
    }
}