using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Serilog;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;

namespace Workload;

public sealed class InfluxWriter : IDisposable
{
    readonly InfluxDBClient _client;
    readonly string _bucket;
    readonly string _org;
    readonly int _batchSize;

    public InfluxWriter(string host, int port, string org, string bucket, string token, int batchSize, bool insecure = false)
    {
        _batchSize = batchSize;
        _bucket = bucket;
        _org = org;

        string baseUrl;
        if (!host.StartsWith("http://", StringComparison.OrdinalIgnoreCase) && !host.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            baseUrl = "http://" + host;
        }
        else
        {
            baseUrl = host;
        }

        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var tmp))
        {
            throw new ArgumentException($"Invalid host/URL: {host}");
        }

        if (tmp.IsDefaultPort)
        {
            var ub = new UriBuilder(tmp) { Port = port };
            baseUrl = ub.Uri.GetLeftPart(UriPartial.Authority);
        }
        else
        {
            baseUrl = tmp.GetLeftPart(UriPartial.Authority);
        }

        try
        {
            _client = InfluxDBClientFactory.Create(baseUrl, token.ToCharArray());
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Failed to create InfluxDB client");
            throw;
        }
    }

    public async Task WritePointsAsync(IAsyncEnumerable<string> lines)
    {
        var batch = new List<string>(_batchSize);
        try
        {
            await foreach (var line in lines)
            {
                batch.Add(line);
                if (batch.Count >= _batchSize)
                {
                    await FlushAsync(batch);
                    batch.Clear();
                }
            }
            if (batch.Count > 0)
                await FlushAsync(batch);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Exception in WritePointsAsync");
            throw;
        }
    }

    async Task FlushAsync(List<string> batch)
    {
        var payload = string.Join('\n', batch) + '\n';

        int attempts = 0;
        const int maxAttempts = 3;
        while (true)
        {
            attempts++;
            try
            {
                var writeApi = _client.GetWriteApiAsync();
                await writeApi.WriteRecordAsync(_bucket, _org, WritePrecision.Ns, payload);
                break;
            }
            catch (MissingMethodException)
            {
                var writeApi = _client.GetWriteApi();
                await Task.Run(() => writeApi.WriteRecord(_bucket, _org, WritePrecision.Ns, payload));
                break;
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Write attempt {Attempt} failed via SDK", attempts);
                if (attempts >= maxAttempts) throw;
                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempts));
            }
        }
    }

    public void Dispose()
    {
        try { _client?.Dispose(); } catch { }
    }
}
