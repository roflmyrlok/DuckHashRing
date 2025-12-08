using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Serilog.Context;

namespace DuckSharding.Shared.Middleware;

public class TraceIdMiddleware
{
	private readonly RequestDelegate _next;
	private const string TraceIdHeader = "X-Trace-Id";

	public TraceIdMiddleware(RequestDelegate next)
	{
		_next = next;
	}

	public async Task InvokeAsync(HttpContext context)
	{
		string traceId;

		if (context.Request.Headers.TryGetValue(TraceIdHeader, out var headerValue))
		{
			traceId = headerValue.ToString();
		}
		else
		{
			traceId = Activity.Current?.Id ?? Guid.NewGuid().ToString();
		}

		context.Items["TraceId"] = traceId;
		context.Response.Headers.TryAdd(TraceIdHeader, traceId);

		using (LogContext.PushProperty("TraceId", traceId))
		{
			await _next(context);
		}
	}
}

public static class TraceIdMiddlewareExtensions
{
	public static IApplicationBuilder UseTraceId(this IApplicationBuilder builder)
	{
		return builder.UseMiddleware<TraceIdMiddleware>();
	}
}