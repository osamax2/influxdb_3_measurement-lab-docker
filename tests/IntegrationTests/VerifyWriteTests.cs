using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace IntegrationTests
{
    public class VerifyWriteTests
    {
        HttpClient Client() => new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        string ReadToken()
        {
            var runFile = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "run", "influx_token");
            if (File.Exists(runFile))
            {
                var txt = File.ReadAllText(runFile).Trim();
                try
                {
                    using var doc = JsonDocument.Parse(txt);
                    if (doc.RootElement.TryGetProperty("token", out var t)) return t.GetString() ?? string.Empty;
                }
                catch { return txt; }
            }
            // environment fallback
            var env = Environment.GetEnvironmentVariable("INFLUX_TOKEN");
            if (!string.IsNullOrEmpty(env))
            {
                try
                {
                    using var doc = JsonDocument.Parse(env);
                    if (doc.RootElement.TryGetProperty("token", out var t)) return t.GetString() ?? string.Empty;
                }
                catch { return env; }
            }
            return string.Empty;
        }

        [Fact(Skip = "Requires local docker-compose stack; run manually")]
        public async Task PostRun_VerifyPointsExist()
        {
            // read token
            var token = ReadToken();
            if (string.IsNullOrEmpty(token)) throw new InvalidOperationException("No token available in run/influx_token or INFLUX_TOKEN env.");

            var client = Client();
            client.DefaultRequestHeaders.Add("Authorization", "Bearer " + token);

            var baseUrl = Environment.GetEnvironmentVariable("INFLUX_HOST") ?? "http://localhost:8181";
            if (!baseUrl.StartsWith("http")) baseUrl = "http://" + baseUrl;

            // Query the DB: use INFLUX_BUCKET env or default my-bucket
            var db = Environment.GetEnvironmentVariable("INFLUX_BUCKET") ?? Environment.GetEnvironmentVariable("INFLUX_DB") ?? "my-bucket";
            var q = $"SELECT * FROM \"{db}\" LIMIT 1";

            var payload = new { db = db, q = q, format = "json" };

            var resp = await client.PostAsJsonAsync(new Uri(new Uri(baseUrl), "/api/v3/query_sql"), payload);
            resp.EnsureSuccessStatusCode();
            var body = await resp.Content.ReadAsStringAsync();
            Assert.Contains("results", body, StringComparison.OrdinalIgnoreCase);
        }
    }
}
