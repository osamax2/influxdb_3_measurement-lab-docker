using System;
using Serilog;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Workload;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var startTime = DateTime.UtcNow;
        // Initialize Serilog to ensure InfluxWriter logs are emitted to container stdout
        Serilog.Log.Logger = new Serilog.LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.Console()
            .CreateLogger();

        Serilog.Log.Information("START {StartTime}", startTime.ToString("O"));
        
        try
        {
            // Initialize logging first
            string logDir = Env("WORKLOAD_LOG_DIR", "/logs/workload");
            Directory.CreateDirectory(logDir);
            LogEnvironmentVariables(logDir);

            // Configuration with robust parsing
            string scenarioMatrixPath = Env("SCENARIO_MATRIX_PATH", "/app/config/scenario_matrix.yaml");
            string scenarioName = Env("SCENARIO", required: true);
            string reportName = Env("REPORT_NAME", "run");
            
            string host = Env("INFLUX_HOST", required: true);
            int port = ParseNumberEnv("INFLUX_PORT", 8086);
            // InfluxDB 3 uses org/bucket and token
            string org = Env("INFLUX_ORG", required: true);
            string bucket = Env("INFLUX_BUCKET", "benchdb");
            // Allow token from env or from mounted /run/influx_token file (pre-persisted by influx image)
            // Token is optional when server runs with --without-auth
            string? token = Env("INFLUX_TOKEN", null);

            // If env contains JSON (e.g. {"token":"apiv3_xxx"}), extract token field via JSON parser
            if (!string.IsNullOrEmpty(token))
            {
                var trimmed = token.Trim();
                if (trimmed.StartsWith('{'))
                {
                    try
                    {
                        using var doc = JsonDocument.Parse(trimmed);
                        if (doc.RootElement.TryGetProperty("token", out var tokProp) && tokProp.ValueKind == JsonValueKind.String)
                        {
                            token = tokProp.GetString()!;
                            Console.WriteLine("[CONFIG] Extracted token from INFLUX_TOKEN env JSON");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[CONFIG WARNING] Failed to parse INFLUX_TOKEN JSON: {ex.Message}");
                    }
                }
            }

            if (string.IsNullOrEmpty(token))
            {
                // Try mounted token file (container path /run/influx_token or project ./run/influx_token)
                string[] candidates = { "/run/influx_token", "./run/influx_token" };
                foreach (var path in candidates)
                {
                    try
                    {
                        if (!File.Exists(path)) continue;
                        var content = File.ReadAllText(path).Trim();
                        if (content.StartsWith('{'))
                        {
                            try
                            {
                                using var doc = JsonDocument.Parse(content);
                                if (doc.RootElement.TryGetProperty("token", out var tokProp) && tokProp.ValueKind == JsonValueKind.String)
                                {
                                    token = tokProp.GetString()!;
                                    Console.WriteLine($"[CONFIG] Loaded token from file: {path}");
                                    break;
                                }
                            }
                            catch { /* continue to fallback */ }
                        }
                        // fallback: file may contain raw token
                        if (!string.IsNullOrEmpty(content))
                        {
                            token = content;
                            Console.WriteLine($"[CONFIG] Loaded raw token from file: {path}");
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[CONFIG WARNING] Failed to read token file {path}: {ex.Message}");
                    }
                }
            }

            if (string.IsNullOrEmpty(token))
            {
                // No token found. Run in no-auth mode (server must allow unauthenticated writes).
                Console.WriteLine("[CONFIG WARNING] No INFLUX_TOKEN provided; proceeding without authentication (server must allow writes without auth).");
                token = null;
            }
            string measStr = Env("MEASUREMENTS", "test");
            string tagStr = Env("TAGS", "");

            // Numeric parameters with special boolean handling
            int batchSize = ParseNumberEnv("BATCH_SIZE", 5000);
            int durationSec = ParseNumberEnv("DURATION_SEC", 10);
            long pointsOverride = ParseNumberEnv<long>("POINTS") ?? -1;
            int? tsSpanOverride = ParseNumberEnv<int>("TIMESTAMP_SPAN_SEC");
            int? pcOverride = ParseNumberEnv<int>("PARALLEL_CLIENTS");
            int? seriesMultOverride = ParseNumberEnv<int>("SERIES_MULTIPLIER");

            // Load scenario
            var matrix = ScenarioConfig.Load(scenarioMatrixPath);
            if (!matrix.TryGetValue(scenarioName, out var scen))
            {
                throw new ConfigException($"Scenario not defined: {scenarioName}");
            }

            // Calculate final parameters
            long totalPoints = pointsOverride >= 0 ? pointsOverride : scen.points;
            int tsSpan = tsSpanOverride ?? scen.timestamp_span_sec;
            int parallelClients = pcOverride ?? scen.parallel_clients;
            int seriesMult = seriesMultOverride ?? scen.series_multiplier;

            // Parse tags and measurements
            var tags = ParseTags(tagStr);
            var measurements = measStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            // Initialize result logging
            string logPath = Path.Combine(logDir, $"{reportName}.csv");
            using var logger = new RunLogger(logPath);
            
            logger.Event("SCENARIO_START", scenarioName);
            logger.Event("PARAMS", $"points={totalPoints};ts_span={tsSpan};pc={parallelClients};series_mult={seriesMult}");

            // Execute workload
            var success = await ExecuteWorkload(
                logger,
                host,
                port,
                org,
                bucket,
                token,
                batchSize,
                measurements,
                tags,
                totalPoints,
                parallelClients,
                tsSpan,
                seriesMult);

            logger.Event("SCENARIO_END", success ? "SUCCESS" : "FAILED");

            // Handle post-execution delay
            if (durationSec > 0)
            {
                Console.WriteLine($"Waiting {durationSec} seconds before exiting...");
                await Task.Delay(TimeSpan.FromSeconds(durationSec));
            }

            return success ? 0 : 1;
        }
        catch (ConfigException ex)
        {
            Console.WriteLine($"[CONFIG ERROR] {ex.Message}");
            return 2;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[UNEXPECTED ERROR] {ex}");
            return 3;
        }
        finally
        {
            Serilog.Log.Information("END {EndTime}", DateTime.UtcNow.ToString("O"));
            Serilog.Log.CloseAndFlush();
        }
    }

    private static int ParseNumberEnv(string varName, int defaultValue)
    {
        string value = Env(varName, null);
        if (string.IsNullOrEmpty(value)) return defaultValue;

        // Special handling for boolean strings
        if (bool.TryParse(value, out bool boolResult))
        {
            Console.WriteLine($"[CONFIG] Converting boolean {varName}={value} to numeric ({defaultValue})");
            return boolResult ? defaultValue : 0;
        }

        if (int.TryParse(value, out int intResult))
        {
            return intResult;
        }

        Console.WriteLine($"[CONFIG WARNING] Invalid {varName} value '{value}'. Using default: {defaultValue}");
        return defaultValue;
    }

    private static T? ParseNumberEnv<T>(string varName) where T : struct
    {
        string value = Env(varName, null);
        if (string.IsNullOrEmpty(value)) return null;

        // Handle boolean strings for all numeric types
        if (bool.TryParse(value, out bool boolResult))
        {
            Console.WriteLine($"[CONFIG] Converting boolean {varName}={value} to numeric");
            return (T)(object)(boolResult ? 1 : 0);
        }

        try
        {
            return (T)Convert.ChangeType(value, typeof(T));
        }
        catch
        {
            Console.WriteLine($"[CONFIG WARNING] Invalid {typeof(T).Name} value for {varName}: '{value}'");
            return null;
        }
    }

    private static async Task<bool> ExecuteWorkload(
        RunLogger logger,
        string host,
        int port,
        string org,
        string bucket,
        string token,
        int batchSize,
        IEnumerable<string> measurements,
        IDictionary<string, string> tags,
        long totalPoints,
        int parallelClients,
        int tsSpan,
        int seriesMult)
    {
        try
        {
            // Divide workload across clients
            long perClient = totalPoints / parallelClients;
            long remainder = totalPoints % parallelClients;
            var tasks = new List<Task>();
            DateTimeOffset tsStart = DateTimeOffset.UtcNow;

            Console.WriteLine($"[CONFIG] Total Points: {totalPoints}");
            Console.WriteLine($"[CONFIG] Parallel Clients: {parallelClients}");
            Console.WriteLine($"[CONFIG] Batch Size: {batchSize}");

            for (int i = 0; i < parallelClients; i++)
            {
                long n = perClient + (i == parallelClients - 1 ? remainder : 0);
                int idx = i;
                
                logger.Event($"THREAD_{idx}_START", n.ToString());
                Console.WriteLine($"[THREAD {idx}] Processing {n} points");

    // Read optional chunk size from env so tests can tune HTTP request sizes
    int chunkSize = ParseNumberEnv("CHUNK_SIZE_BYTES", 4000000);
    // Optional flag to send writes using legacy db-only query param (useful for management ports like 8181)
    bool useDbOnly = string.Equals(Env("INFLUX_USE_DB_ONLY", "false"), "true", StringComparison.OrdinalIgnoreCase);

    // Read gzip/send concurrency from environment so we can tune without recompiling
    bool useGzipEnv = string.Equals(Env("USE_GZIP", "false"), "true", StringComparison.OrdinalIgnoreCase);
    int sendConcurrency = ParseNumberEnv("SEND_CONCURRENCY", 8);
    // Points per HTTP request. Default to 50000 to allow large single-request payloads;
    // can be overridden with env POINTS_PER_REQUEST for smaller batch sizes during tests.
    int pointsPerRequest = ParseNumberEnv("POINTS_PER_REQUEST", 50000);
            // Optional: use v3 write_lp endpoint instead of v2 write
            bool useV3WriteLp = string.Equals(Env("USE_V3_WRITE_LP", "false"), "true", StringComparison.OrdinalIgnoreCase);
            string v3Db = Env("V3_DB", "");
            bool v3AcceptPartial = string.Equals(Env("V3_ACCEPT_PARTIAL", "true"), "true", StringComparison.OrdinalIgnoreCase);
            bool v3NoSync = string.Equals(Env("V3_NO_SYNC", "false"), "true", StringComparison.OrdinalIgnoreCase);
            string v3Precision = Env("V3_PRECISION", "ns");

        tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var lines = WorkloadAsyncLines(measurements, tags, n, tsStart, tsSpan, seriesMult);
                        int flushInterval = ParseNumberEnv("FLUSH_INTERVAL_MS", 1000);
            // The InfluxWriter constructor was refactored to accept (host,port,org,bucket,token,batchSize,flushInterval,useGzip,chunkSizeBytes,capacityMultiplier)
            // Map SEND_CONCURRENCY to capacityMultiplier to control internal channel capacity/backpressure.
            int capacityMultiplier = Math.Max(1, sendConcurrency);
                        var opts = new InfluxWriterOptions
                        {
                            Host = host,
                            Port = port,
                            Org = org,
                            Bucket = bucket,
                            Token = token,
                            BatchSize = batchSize,
                            FlushIntervalMs = flushInterval,
                            UseGzip = useGzipEnv,
                            ChunkSizeBytes = chunkSize,
                                CapacityMultiplier = capacityMultiplier,
                                // Map SEND_CONCURRENCY env to send concurrency so writer can parallelize HTTP requests
                                SendConcurrency = sendConcurrency,
                                // Allow POINTS_PER_REQUEST to be configured via env for batching tests
                                PointsPerRequest = pointsPerRequest,
                            UseV3WriteLp = useV3WriteLp,
                            V3Db = v3Db,
                            AcceptPartial = v3AcceptPartial,
                            NoSync = v3NoSync,
                            Precision = v3Precision
                        };

                        using var writer = new InfluxWriter(opts);
                        await writer.WritePointsAsync(lines);
                        logger.Event($"THREAD_{idx}_END", "SUCCESS");
                        Console.WriteLine($"[THREAD {idx}] Completed successfully");
                    }
                    catch (Exception ex)
                    {
                        // Log full exception (message + stack) so run.csv contains actionable info
                        logger.Event($"THREAD_{idx}_ERROR", ex.ToString());
                        Console.WriteLine($"[THREAD {idx}] Failed: {ex}");
                        throw;
                    }
                }));
            }

            await Task.WhenAll(tasks);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static Dictionary<string, string> ParseTags(string tagStr)
    {
        try
        {
            return tagStr.Split(',', StringSplitOptions.RemoveEmptyEntries)
                .Select(kv => kv.Split('=', 2))
                .Where(a => a.Length == 2)
                .ToDictionary(a => a[0].Trim(), a => a[1].Trim());
        }
        catch (Exception ex)
        {
            throw new ConfigException($"Invalid TAGS format: {ex.Message}");
        }
    }

    private static void LogEnvironmentVariables(string logDir)
    {
        try
        {
            string logPath = Path.Combine(logDir, "environment_variables.log");
            var envVars = Environment.GetEnvironmentVariables()
                .Cast<DictionaryEntry>()
                .OrderBy(e => e.Key.ToString());

            foreach (var entry in envVars)
            {
                string line = $"{entry.Key}={entry.Value}";
                File.AppendAllText(logPath, line + Environment.NewLine);
                Console.WriteLine($"[ENV] {line}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARNING] Failed to log environment variables: {ex.Message}");
        }
    }

    private static IAsyncEnumerable<string> WorkloadAsyncLines(
        IEnumerable<string> measurements,
        IDictionary<string, string> tags,
        long count,
        DateTimeOffset tsStart,
        int tsSpanSec,
        int seriesMultiplier)
    {
        // Wrap the synchronous generator into an async enumerable to avoid an
        // unnecessary async state machine for each yielded value.
        return AsyncEnumerableFromEnumerable(PointGenerator.Generate(measurements.ToArray(), tags, count, tsStart, tsSpanSec, seriesMultiplier));
    }

    private static async IAsyncEnumerable<string> AsyncEnumerableFromEnumerable(IEnumerable<string> src)
    {
        foreach (var s in src) yield return s;
        await Task.CompletedTask;
    }

    #region Helper Classes and Methods

    public class ConfigException : Exception
    {
        public ConfigException(string message) : base(message) {}
    }

    private static string Env(string name, string? defaultValue = null, bool required = false)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            if (required && defaultValue == null)
                throw new ConfigException($"Missing required environment variable: {name}");
            return defaultValue ?? string.Empty;
        }
        return value;
    }

    #endregion
}