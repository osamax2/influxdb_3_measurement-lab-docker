using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;
using System.IO.Compression;
using System.Threading;
using Serilog;

namespace Workload;

public sealed class InfluxWriter : IDisposable
{
    readonly string _bucket;
    readonly string _org;
    readonly int _batchSize;
    readonly HttpClient _httpClient;
    readonly string _writeUrl;
    readonly bool _useGzip;
    readonly Timer _flushTimer;
    readonly int _flushIntervalMs;
    readonly List<string> _batchBuffer = new List<string>();
    readonly object _bufferLock = new object();
    int _isFlushing = 0;
    readonly int _minFlushPoints;
    readonly int _chunkSizeBytes;
    long _totalPoints = 0;
    DateTime _lastFlushTime = DateTime.UtcNow;

    public InfluxWriter(string host, int port, string org, string bucket, string token, 
                       int batchSize, int flushIntervalMs = 1000, bool useGzip = false, int chunkSizeBytes = 4_000_000)
    {
        _batchSize = batchSize;
        _bucket = bucket;
        _org = org;
        _useGzip = useGzip;
        _flushIntervalMs = flushIntervalMs;

        // Build base URL
        var baseUrl = host.StartsWith("http") ? host : $"http://{host}";
        if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var uri))
            throw new ArgumentException($"Invalid host/URL: {host}");
        
        var authority = uri.GetLeftPart(UriPartial.Authority);
        if (uri.IsDefaultPort)
        {
            var ub = new UriBuilder(uri) { Port = port };
            authority = ub.Uri.GetLeftPart(UriPartial.Authority);
        }

        _writeUrl = $"{authority}/api/v3/write_lp?db={Uri.EscapeDataString(_bucket)}&bucket={Uri.EscapeDataString(_bucket)}&org={Uri.EscapeDataString(_org)}&precision=nanosecond";

        // Configure HTTP client with aggressive timeouts and connection settings
        _httpClient = new HttpClient(new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            // Allow many concurrent connections to match parallel clients in the workload
            MaxConnectionsPerServer = 100,
            UseCookies = false,
            UseProxy = false
        })
        {
            Timeout = TimeSpan.FromSeconds(60),
            DefaultRequestHeaders =
            {
                Authorization = new AuthenticationHeaderValue("Bearer", token),
                Accept = { new MediaTypeWithQualityHeaderValue("application/json") }
            }
        };

    // Minimum points required before timer-triggered flushes will run.
    // Increase threshold so the timer doesn't cause many small flushes under load.
    _minFlushPoints = Math.Max(1, _batchSize / 4);

    // Chunk size for splitting payloads (bytes). Make configurable for tuning.
    _chunkSizeBytes = Math.Max(64_000, chunkSizeBytes);

    // Set up periodic flush timer
    _flushTimer = new Timer(FlushTimerCallback, null, flushIntervalMs, flushIntervalMs);
    }

    public async Task WritePointsAsync(IAsyncEnumerable<string> lines)
    {
        await foreach (var line in lines)
        {
            lock (_bufferLock)
            {
                _batchBuffer.Add(line);
                _totalPoints++;
                
                // Flush if batch size reached
                if (_batchBuffer.Count >= _batchSize)
                {
                    _ = Task.Run(() => FlushAsync()); // Fire and forget for async flush
                }
            }
        }
        
        // Final flush
        await FlushAsync();
    }

    private void FlushTimerCallback(object state)
    {
        // Flush if buffer has data and it's been a while since last flush.
        // Lock briefly to snapshot buffer size and last flush time to avoid races.
        bool shouldFlush = false;
        lock (_bufferLock)
        {
            // Only flush on timer when we have accumulated a reasonable number of points
            if (_batchBuffer.Count >= _minFlushPoints && (DateTime.UtcNow - _lastFlushTime).TotalMilliseconds > _flushIntervalMs)
            {
                shouldFlush = true;
            }
        }

        if (shouldFlush)
        {
            // Avoid concurrent flushes
            _ = Task.Run(() => FlushAsync());
        }
    }

    private async Task FlushAsync()
    {
        // Prevent overlapping FlushAsync executions
        if (System.Threading.Interlocked.Exchange(ref _isFlushing, 1) == 1)
        {
            return; // already flushing
        }

        List<string> currentBatch;
        lock (_bufferLock)
        {
            if (_batchBuffer.Count == 0) return;
            
            currentBatch = new List<string>(_batchBuffer);
            _batchBuffer.Clear();
        }

    var payload = string.Join('\n', currentBatch) + '\n';
        var attempts = 0;
        const int maxAttempts = 3;
    // Legacy local const removed; use _chunkSizeBytes instance field instead.

        while (attempts < maxAttempts)
        {
            attempts++;
            try
            {
                var bytes = Encoding.UTF8.GetBytes(payload);

                if (bytes.Length <= _chunkSizeBytes)
                {
                    Log.Debug("Posting single chunk bytes={Bytes}", bytes.Length);
                    using var content = _useGzip 
                        ? CreateGzipContent(payload) 
                        : new StringContent(payload, Encoding.UTF8, "text/plain");

                    var response = await _httpClient.PostAsync(_writeUrl, content);
                    response.EnsureSuccessStatusCode();
                    Log.Information("Successfully wrote {Count} points (Total: {Total}) in 1 chunk (bytes={Bytes})", currentBatch.Count, _totalPoints, bytes.Length);
                }
                else
                {
                    // Split by lines ensuring each chunk's UTF8 byte length <= _chunkSizeBytes
                    var lines = payload.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                    var chunk = new List<string>();
                    var chunkBytes = 0;
                    var sentPoints = 0;

                    foreach (var line in lines)
                    {
                        var lineBytes = Encoding.UTF8.GetByteCount(line + '\n');
                        if (chunkBytes + lineBytes > _chunkSizeBytes && chunk.Count > 0)
                        {
                            var chunkPayload = string.Join('\n', chunk) + '\n';
                            var chunkByteLen = Encoding.UTF8.GetByteCount(chunkPayload);
                            Log.Debug("Posting chunk bytes={Bytes} (points={Points})", chunkByteLen, chunk.Count);
                            using var content = _useGzip ? CreateGzipContent(chunkPayload) : new StringContent(chunkPayload, Encoding.UTF8, "text/plain");
                            var response = await _httpClient.PostAsync(_writeUrl, content);
                            response.EnsureSuccessStatusCode();
                            sentPoints += chunk.Count;
                            Log.Information("Successfully wrote {Count} points (Total: {Total}) in chunk (bytes={Bytes})", chunk.Count, _totalPoints, chunkBytes);
                            chunk.Clear();
                            chunkBytes = 0;
                        }

                        chunk.Add(line);
                        chunkBytes += lineBytes;
                    }

                    if (chunk.Count > 0)
                    {
                        var chunkPayload = string.Join('\n', chunk) + '\n';
                        var finalChunkByteLen = Encoding.UTF8.GetByteCount(chunkPayload);
                        Log.Debug("Posting final chunk bytes={Bytes} (points={Points})", finalChunkByteLen, chunk.Count);
                        using var content = _useGzip ? CreateGzipContent(chunkPayload) : new StringContent(chunkPayload, Encoding.UTF8, "text/plain");
                        var response = await _httpClient.PostAsync(_writeUrl, content);
                        response.EnsureSuccessStatusCode();
                        sentPoints += chunk.Count;
                        Log.Information("Successfully wrote {Count} points (Total: {Total}) in final chunk (bytes={Bytes})", chunk.Count, _totalPoints, chunkBytes);
                    }

                    if (sentPoints != currentBatch.Count)
                    {
                        Log.Warning("Sent points ({Sent}) != batch size ({Batch})", sentPoints, currentBatch.Count);
                    }
                }

                _lastFlushTime = DateTime.UtcNow;
        break;
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Write attempt {Attempt} failed", attempts);
                if (attempts >= maxAttempts)
                {
                    Log.Error("All write attempts failed for batch of {Count} points", currentBatch.Count);
                    throw;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempts));
            }
        }

    // Allow subsequent flushes
    System.Threading.Interlocked.Exchange(ref _isFlushing, 0);
    }

    private HttpContent CreateGzipContent(string payload)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        using var output = new System.IO.MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest))
        {
            gzip.Write(bytes, 0, bytes.Length);
        }
        
        var content = new ByteArrayContent(output.ToArray());
        content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        content.Headers.ContentEncoding.Add("gzip");
        return content;
    }

    public void Dispose()
    {
        // Stop the timer first so no new timer callbacks run while we're flushing.
        _flushTimer?.Dispose();

        // Wait for any in-flight flush to complete (with a short timeout).
        // This prevents disposing the shared HttpClient while a background flush is using it.
        var waited = 0;
        const int waitSliceMs = 100;
        const int maxWaitMs = 5000;
        while (System.Threading.Interlocked.CompareExchange(ref _isFlushing, 0, 0) == 1 && waited < maxWaitMs)
        {
            System.Threading.Thread.Sleep(waitSliceMs);
            waited += waitSliceMs;
        }

        // Final flush of any remaining points. Do this before disposing the HttpClient
        // because FlushAsync uses the shared HttpClient instance. Lock buffer when checking.
        lock (_bufferLock)
        {
            if (_batchBuffer.Count > 0)
            {
                try
                {
                    Task.Run(() => FlushAsync()).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    // Log and swallow exceptions during dispose so callers can continue shutdown.
                    Log.Warning(ex, "Final flush during Dispose failed");
                }
            }
        }

        // Now it's safe to dispose the HttpClient.
        _httpClient?.Dispose();
    }
}