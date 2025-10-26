using DuckSharding.Coordinator;

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
builder.Services.AddSingleton<ShardProcessManager>();

builder.WebHost.UseUrls("http://localhost:5000");

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "DuckSharding API v1");
    c.RoutePrefix = string.Empty;
});

app.MapControllers();

app.Run();