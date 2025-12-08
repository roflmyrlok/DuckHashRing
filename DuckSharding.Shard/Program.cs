using DuckSharding.Shard;
using DuckSharding.Shared.Middleware;
using Prometheus;
using Serilog;
using Serilog.Events;

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Service", "Shard")
    .Enrich.WithMachineName()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] [{Service}] [{TraceId}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    var builder = WebApplication.CreateBuilder(args);

    builder.Host.UseSerilog();

    builder.Services.AddControllers();
    builder.Services.AddHttpClient();

    var isLeader = builder.Configuration.GetValue<bool>("Shard:IsLeader", false);
    var shardId = builder.Configuration["Shard:ShardId"] ?? "shard-0";
    var replicaId = builder.Configuration["Shard:ReplicaId"] ?? "follower-0";

    Log.Information("Starting Shard service: ShardId={ShardId}, ReplicaId={ReplicaId}, IsLeader={IsLeader}", 
        shardId, replicaId, isLeader);

    builder.Services.AddSingleton<ReplicationLogRepository>();

    if (isLeader)
    {
        builder.Services.AddSingleton<EventPublisher>(sp =>
        {
            var config = sp.GetRequiredService<IConfiguration>();
            var replicationLog = sp.GetRequiredService<ReplicationLogRepository>();
            var logger = sp.GetRequiredService<ILogger<EventPublisher>>();
            return EventPublisher.CreateAsync(config, replicationLog, logger).GetAwaiter().GetResult();
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

    app.UseTraceId();
    app.UseSerilogRequestLogging();

    // Prometheus metrics endpoint
    app.UseMetricServer();
    app.UseHttpMetrics();

    app.MapControllers();

    Log.Information("Shard service started successfully");
    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Shard service failed to start");
    throw;
}
finally
{
    Log.CloseAndFlush();
}