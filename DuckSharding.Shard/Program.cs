using DuckSharding.Shard;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddHttpClient();
builder.Services.AddSingleton<DuckRepository>();

var app = builder.Build();

app.MapControllers();
app.Run();