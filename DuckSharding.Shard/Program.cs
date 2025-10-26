using DuckSharding.Shard;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddHttpClient();
builder.Services.AddSingleton<DuckRepository>();

// Get port from command line args
var port = args.Length > 0 ? args[0] : "5001";
var shardId = args.Length > 1 ? args[1] : $"shard-{port}";
var dbFileName = args.Length > 2 ? args[2] : $"shard{port}.db";

builder.Configuration["Shard:Port"] = port;
builder.Configuration["Shard:ShardId"] = shardId;
builder.Configuration["Database:FileName"] = dbFileName;

builder.WebHost.UseUrls($"http://localhost:{port}");

var app = builder.Build();

app.MapControllers();
app.Run();