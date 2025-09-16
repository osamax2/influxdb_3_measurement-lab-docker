using System;
using Serilog;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            string token = Env("INFLUX_TOKEN", required: true);
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

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var lines = WorkloadAsyncLines(measurements, tags, n, tsStart, tsSpan, seriesMult);
                        using var writer = new InfluxWriter(host, port, org, bucket, token, batchSize);
                        await writer.WritePointsAsync(lines);
                        logger.Event($"THREAD_{idx}_END", "SUCCESS");
                        Console.WriteLine($"[THREAD {idx}] Completed successfully");
                    }
                    catch (Exception ex)
                    {
                        logger.Event($"THREAD_{idx}_ERROR", ex.Message);
                        Console.WriteLine($"[THREAD {idx}] Failed: {ex.Message}");
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

    private static async IAsyncEnumerable<string> WorkloadAsyncLines(
        IEnumerable<string> measurements,
        IDictionary<string, string> tags,
        long count,
        DateTimeOffset tsStart,
        int tsSpanSec,
        int seriesMultiplier)
    {
        foreach (var line in PointGenerator.Generate(measurements.ToArray(), tags, count, tsStart, tsSpanSec, seriesMultiplier))
        {
            yield return line;
            await Task.Yield();
        }
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