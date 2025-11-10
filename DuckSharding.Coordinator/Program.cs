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
builder.Services.AddSingleton<ReplicaAwareCoordinator>();
builder.Services.AddSingleton<GenericTableClient>();

var app = builder.Build();

var coordinator = app.Services.GetRequiredService<Coordinator>();
var replicaCoordinator = app.Services.GetRequiredService<ReplicaAwareCoordinator>();
var configuration = app.Configuration;
var k8sNamespace = configuration["Kubernetes:Namespace"] ?? "default";

// Number of shards (leaders)
var shardCount = 3;
// Number of followers per shard
var followersPerShard = 3;

for (int i = 0; i < shardCount; i++)
{
    var shardId = $"shard-{i}";
    
    // Register shard for hash ring
    var shardInfo = new ShardInfo(
        shardId, 
        $"{shardId}.shard.{k8sNamespace}.svc.cluster.local", 
        8080);
    coordinator.AddShard(shardInfo);

    // Register leader
    var leader = new ShardInfo(
        $"{shardId}-leader",
        $"{shardId}.shard.{k8sNamespace}.svc.cluster.local",
        8080,
        isLeader: true);

    // Register followers for this shard
    var followers = new List<ShardInfo>();
    for (int j = 0; j < followersPerShard; j++)
    {
        // Calculate the global follower ordinal
        var followerOrdinal = i * followersPerShard + j;
        
        followers.Add(new ShardInfo(
            $"{shardId}-follower-{j}",
            $"follower-{followerOrdinal}.follower.{k8sNamespace}.svc.cluster.local",
            8080,
            isLeader: false));
    }

    replicaCoordinator.RegisterReplicaSet(shardId, leader, followers);
}

app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "DuckSharding API v1");
    c.RoutePrefix = string.Empty;
});

app.MapControllers();

app.Run();