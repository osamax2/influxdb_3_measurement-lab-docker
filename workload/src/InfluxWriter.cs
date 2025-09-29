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
    // Use a single shared HttpClient for the process to avoid races when disposing
    // individual clients while other threads are sending requests.
    static readonly HttpClient _sharedHttpClient;
    readonly string _writeUrl;
    readonly bool _useGzip;
    readonly Timer _flushTimer;
    readonly int _flushIntervalMs;
    readonly List<string> _batchBuffer = new List<string>();
    readonly object _bufferLock = new object();
    int _isFlushing = 0;
    readonly int _minFlushPoints;
    readonly int _chunkSizeBytes;
    readonly System.Threading.SemaphoreSlim _sendSemaphore;
    readonly string? _token;
    // When disposing, prevent scheduling additional flushes
    volatile bool _isDisposing = false;
    private const string TextPlain = "text/plain";
    long _totalPoints = 0;
    DateTime _lastFlushTime = DateTime.UtcNow;

    // Note: httpTimeoutSec removed; use process-wide shared HttpClient timeout instead.
    public InfluxWriter(string host, int port, string org, string bucket, string? token, 
                       int batchSize, int flushIntervalMs = 1000, bool useGzip = false, int chunkSizeBytes = 4_000_000, bool useDbOnly = false)
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

    // Build write URL. Two supported variants:
    // - v3-compatible: bucket + db (compat) + org + precision
    // - db-only: only legacy ?db= (useful when targeting management/compat ports like 8181)
    if (useDbOnly)
    {
        _writeUrl = $"{authority}/api/v3/write_lp?db={Uri.EscapeDataString(_bucket)}";
    }
    else
    {
        _writeUrl = $"{authority}/api/v3/write_lp?bucket={Uri.EscapeDataString(_bucket)}&db={Uri.EscapeDataString(_bucket)}&org={Uri.EscapeDataString(_org)}&precision=nanosecond";
    }

        // Shared HttpClient is initialized in static constructor.
        // We prefer to set Authorization per-request only when a token is provided to avoid stale header reuse across retries
        if (!string.IsNullOrEmpty(token))
        {
            _token = token;
        }

    // Minimum points required before timer-triggered flushes will run.
    // Increase threshold so the timer doesn't cause many small flushes under load.
    _minFlushPoints = Math.Max(1, _batchSize / 4);

    // Chunk size for splitting payloads (bytes). Make configurable for tuning.
    // Allow small chunk sizes for stress testing; enforce a sensible lower bound of 4KB.
    _chunkSizeBytes = Math.Max(4_096, chunkSizeBytes);

    // Set up periodic flush timer
    _flushTimer = new Timer(FlushTimerCallback, null, flushIntervalMs, flushIntervalMs);
        // Limit concurrent HTTP send operations per writer to avoid bursts that can overwhelm the server
        _sendSemaphore = new System.Threading.SemaphoreSlim(2);
    }

    // Static ctor to initialize shared HttpClient once per process.
    static InfluxWriter()
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = 20,
            UseCookies = false,
            UseProxy = false
        };
        handler.AllowAutoRedirect = false;

        _sharedHttpClient = new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(120)
        };
        _sharedHttpClient.DefaultRequestHeaders.ExpectContinue = false;
        try { _sharedHttpClient.DefaultRequestVersion = new Version(1, 1); } catch { }
        _sharedHttpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
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
                if (_batchBuffer.Count >= _batchSize && !_isDisposing)
                {
                    _ = Task.Run(() => FlushAsync()); // Fire and forget for async flush
                }
            }
        }
        
        // Final flush
        await FlushAsync();
    }

    private void FlushTimerCallback(object? _)
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
            // Avoid concurrent flushes and don't schedule new flushes when disposing
            if (!_isDisposing)
            {
                _ = Task.Run(() => FlushAsync());
            }
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
                    HttpContent content;
                    string? payloadPreview = null;
                    if (_useGzip)
                    {
                        content = CreateGzipContent(payload);
                        payloadPreview = "<gzip>";
                    }
                    else
                    {
                        // Ensure charset is present per docs
                        content = new StringContent(payload, Encoding.UTF8, "text/plain");
                        content.Headers.ContentType.CharSet = "utf-8";
                        payloadPreview = payload.Length <= 512 ? payload : payload.Substring(0, 512) + "...";
                    }

                    var response = await SendHttpRequestAsync(content).ConfigureAwait(false);
                    if (!response.IsSuccessStatusCode)
                    {
                        var body = "";
                        try { body = await response.Content.ReadAsStringAsync().ConfigureAwait(false); } catch { /* ignore read errors of response body for logging */ }
                        Log.Warning("Write failed to URL {Url} with status {Status} {Reason}. Body: {Body}. Request preview: {Preview}", _writeUrl, (int)response.StatusCode, response.ReasonPhrase, body, payloadPreview);
                        response.EnsureSuccessStatusCode();
                    }
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
                                HttpContent content;
                                string? chunkPreview = null;
                                if (_useGzip)
                                {
                                    content = CreateGzipContent(chunkPayload);
                                    chunkPreview = "<gzip>";
                                }
                                else
                                {
                                    content = new StringContent(chunkPayload, Encoding.UTF8, "text/plain");
                                    content.Headers.ContentType.CharSet = "utf-8";
                                    chunkPreview = chunkPayload.Length <= 512 ? chunkPayload : chunkPayload.Substring(0, 512) + "...";
                                }

                                // small randomized backoff to avoid synchronized spikes across many clients
                                await Task.Delay(TimeSpan.FromMilliseconds(25 + Random.Shared.Next(75)));
                                await TrySendWithDiagnostics(content, chunkPreview, chunkPayload).ConfigureAwait(false);
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
                            HttpContent content;
                            string? finalPreview = null;
                            if (_useGzip)
                            {
                                content = CreateGzipContent(chunkPayload);
                                finalPreview = "<gzip>";
                            }
                            else
                            {
                                content = new StringContent(chunkPayload, Encoding.UTF8, "text/plain");
                                content.Headers.ContentType.CharSet = "utf-8";
                                finalPreview = chunkPayload.Length <= 512 ? chunkPayload : chunkPayload.Substring(0, 512) + "...";
                            }

                            await Task.Delay(TimeSpan.FromMilliseconds(25 + Random.Shared.Next(75)));
                            await TrySendWithDiagnostics(content, finalPreview, chunkPayload).ConfigureAwait(false);
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

    private static HttpContent CreateGzipContent(string payload)
    {
        var bytes = Encoding.UTF8.GetBytes(payload);
        using var output = new System.IO.MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest))
        {
            gzip.Write(bytes, 0, bytes.Length);
        }
        
        var content = new ByteArrayContent(output.ToArray());
        content.Headers.ContentType = new MediaTypeHeaderValue(TextPlain);
        content.Headers.ContentType.CharSet = "utf-8";
        content.Headers.ContentEncoding.Add("gzip");
        return content;
    }

    private async Task<HttpResponseMessage> SendHttpRequestAsync(HttpContent content)
    {
        await _sendSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, _writeUrl)
            {
                Content = content,
                Version = new Version(1, 1)
            };

            // Add per-request Authorization header to avoid issues with reused default headers across retries
            if (!string.IsNullOrEmpty(_token))
            {
                req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
            }

            // Rely on connection pooling; do not force Connection: close which can increase connection churn
            return await _sharedHttpClient.SendAsync(req, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
        }
        finally
        {
            _sendSemaphore.Release();
        }
    }

    // Diagnostic helper: try to send content; if server returns 400, and content is plain text with multiple lines,
    // split and send lines individually to find the exact offending line causing Bad Request.
    private async Task TrySendWithDiagnostics(HttpContent content, string preview, string fullPayload)
    {
        var response = await SendHttpRequestAsync(content).ConfigureAwait(false);
        if (response.IsSuccessStatusCode) return;

        var status = (int)response.StatusCode;
        var reason = response.ReasonPhrase;
        var body = "";
        try { body = await response.Content.ReadAsStringAsync().ConfigureAwait(false); } catch { /* ignore */ }

        Log.Warning("Write failed to URL {Url} with status {Status} {Reason}. Body: {Body}. Request preview: {Preview}", _writeUrl, status, reason, body, preview);

        // If payload looks like plain text (not gzip) and contains multiple lines, try to isolate the bad line.
        if (fullPayload != null && !fullPayload.StartsWith("<gzip>") && fullPayload.Contains('\n'))
        {
            var lines = fullPayload.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                try
                {
                    using var singleContent = new StringContent(line + "\n", Encoding.UTF8, "text/plain");
                    singleContent.Headers.ContentType.CharSet = "utf-8";
                    var r2 = await SendHttpRequestAsync(singleContent).ConfigureAwait(false);
                    if (!r2.IsSuccessStatusCode)
                    {
                        var b2 = "";
                        try { b2 = await r2.Content.ReadAsStringAsync().ConfigureAwait(false); } catch { }
                        Log.Error("Diagnostic: single-line write failed at index {Idx} status={Status} reason={Reason} body={Body} preview={Preview}", i, (int)r2.StatusCode, r2.ReasonPhrase, b2, (line.Length <= 256 ? line : line.Substring(0, 256) + "..."));
                        // stop after first failing line
                        break;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Diagnostic send failed for line index {Idx}", i);
                }
            }
        }

        response.EnsureSuccessStatusCode();
    }

    public void Dispose()
    {
        // Signal disposing so no new flushes are scheduled
        _isDisposing = true;

        // Stop the timer first so no new timer callbacks run while we're flushing.
        _flushTimer?.Dispose();

        // Wait for any in-flight flush to complete (with a longer timeout).
        // This prevents disposing the HttpClient while a background flush is using it.
        var waited = 0;
        const int waitSliceMs = 100;
        const int maxWaitMs = 30_000; // up to 30s wait for flushes to finish
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
                    // Run synchronously to ensure flush completes before we continue
                    Task.Run(() => FlushAsync()).GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    // Log and swallow exceptions during dispose so callers can continue shutdown.
                    Log.Warning(ex, "Final flush during Dispose failed");
                }
            }
        }

        // Do NOT dispose the HttpClient here. Disposing HttpClient while other threads
        // (or the runtime) may still use underlying handlers can cause ObjectDisposedException
        // when concurrent SendAsync calls are racing with disposal. Let the runtime dispose
        // process exit clean up sockets, or refactor to share a single HttpClient across writers.
    }
}