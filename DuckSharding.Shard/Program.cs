using DuckSharding.Shard;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddHttpClient();

var isLeader = builder.Configuration.GetValue<bool>("Shard:IsLeader", false);

if (isLeader)
{
	builder.Services.AddSingleton<EventPublisher>(sp =>
	{
		var config = sp.GetRequiredService<IConfiguration>();
		return EventPublisher.CreateAsync(config).GetAwaiter().GetResult();
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

app.MapControllers();
app.Run();