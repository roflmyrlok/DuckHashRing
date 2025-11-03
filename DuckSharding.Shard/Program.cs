using DuckSharding.Shard;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddHttpClient();
builder.Services.AddSingleton<DuckRepository>();
builder.Services.AddSingleton<GenericRepository>();

var app = builder.Build();

app.MapControllers();
app.Run();