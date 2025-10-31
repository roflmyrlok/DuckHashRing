using DuckSharding.Coordinator;
using DuckSharding.Shared.Models;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
	c.SwaggerDoc("v1", new() { Title = "DuckSharding API", Version = "v1" });
});

builder.Services.AddHttpClient();
builder.Services.AddSingleton<Coordinator>();
builder.Services.AddSingleton<ShardClient>();

var app = builder.Build();

var coordinator = app.Services.GetRequiredService<Coordinator>();
var configuration = app.Configuration;
var k8sNamespace = configuration["Kubernetes:Namespace"] ?? "default";

for (int i = 1; i <= 3; i++)
{
	var shardId = $"shard-{i}";
	var shardInfo = new ShardInfo(
		shardId, 
		$"{shardId}.{k8sNamespace}.svc.cluster.local", 
		8080);
	coordinator.AddShard(shardInfo);
}

app.UseSwagger();
app.UseSwaggerUI(c =>
{
	c.SwaggerEndpoint("/swagger/v1/swagger.json", "DuckSharding API v1");
	c.RoutePrefix = string.Empty;
});

app.MapControllers();

app.Run();