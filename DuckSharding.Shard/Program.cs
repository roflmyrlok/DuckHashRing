using DuckSharding.Shard;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddHttpClient();

var isLeader = builder.Configuration.GetValue<bool>("Shard:IsLeader", false);
builder.Services.AddSingleton<ReplicationLogRepository>();

if (isLeader)
{
	builder.Services.AddSingleton<EventPublisher>(sp =>
	{
		var config = sp.GetRequiredService<IConfiguration>();
		var replicationLog = sp.GetRequiredService<ReplicationLogRepository>();
		return EventPublisher.CreateAsync(config, replicationLog).GetAwaiter().GetResult();
	});
    
	builder.Services.AddSingleton<GenericRepository>(sp =>
	{
		var config = sp.GetRequiredService<IConfiguration>();
		var publisher = sp.GetRequiredService<EventPublisher>();
		return new GenericRepository(config, publisher);
	});
}
else
{
	builder.Services.AddSingleton<GenericRepository>();
	builder.Services.AddHostedService<EventConsumer>();
}

var app = builder.Build();

if (isLeader)
{
	app.Services.GetRequiredService<EventPublisher>();
}

app.MapControllers();
app.Run();