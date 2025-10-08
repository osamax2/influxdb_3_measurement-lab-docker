using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Workload
{
    // Minimal, robust InfluxDB line-protocol writer.
    // - Accepts lines via channel
    // - Groups up to pointsPerRequest per HTTP POST
    // - Optional gzip
    public sealed class InfluxWriterOptions
    {
        public string Host { get; init; } = "localhost";
        public int Port { get; init; } = 8086;
        public string Org { get; init; } = string.Empty;
        public string Bucket { get; init; } = string.Empty;
        public string? Token { get; init; }
        public int BatchSize { get; init; } = 2000;
        public int FlushIntervalMs { get; init; } = 1500;
        public bool UseGzip { get; init; } = false;
        public int ChunkSizeBytes { get; init; } = 131072;
        public int CapacityMultiplier { get; init; } = 1;
    // Increase default to 50k to allow large single-request payloads when desired
    public int PointsPerRequest { get; init; } = 50000;
        public bool UseV3WriteLp { get; init; } = false;
        public string? V3Db { get; init; }
        public bool AcceptPartial { get; init; } = true;
        public bool NoSync { get; init; } = false;
        public string Precision { get; init; } = "ns";
        // Number of concurrent HTTP POSTs to allow for single-point-per-request mode
        public int SendConcurrency { get; init; } = 8;
    }

    public sealed class InfluxWriter : IDisposable
    {
        private readonly HttpClient _http;
        private readonly Uri _writeUri;
        private readonly string _token;
        private readonly Channel<string> _channel;
        private readonly int _pointsPerRequest;
        private readonly bool _useGzip;
    private readonly int _chunkSizeBytes;
    private readonly int _sendConcurrency;
    private readonly SemaphoreSlim? _sendSemaphore;

        private readonly Task _consumerTask;
        private Task[]? _workerTasks;
    private CancellationTokenSource? _cts;
    // Cache a resolved IP for the write host to avoid repeated DNS lookups
    private IPAddress? _cachedAddress;
    // Batch CSV logger
    private StreamWriter? _batchLog;
    // Per-point CSV logger for latency profiling
    private StreamWriter? _pointLog;

        // Convenience constructor: accept options object and delegate to the existing constructor
        public InfluxWriter(InfluxWriterOptions opts)
            : this(
                opts.Host,
                opts.Port,
                opts.Org,
                opts.Bucket,
                opts.Token ?? string.Empty,
                opts.BatchSize,
                opts.FlushIntervalMs,
                opts.UseGzip,
                opts.ChunkSizeBytes,
                opts.CapacityMultiplier,
                opts.PointsPerRequest,
                opts.SendConcurrency,
                opts.UseV3WriteLp,
                opts.V3Db,
                opts.AcceptPartial,
                opts.NoSync,
                opts.Precision)
        {
        }

        public InfluxWriter(
            string host,
            int port,
            string org,
            string bucket,
            string token,
            int batchSize,
            int flushIntervalMs,
            bool useGzip,
            int chunkSizeBytes,
            int capacityMultiplier,
            int pointsPerRequest,
            int sendConcurrency,
            bool useV3WriteLp = false,
            string? v3Db = null,
            bool acceptPartial = true,
            bool noSync = false,
            string precision = "ns")
        {
            _token = token ?? string.Empty;
            _pointsPerRequest = Math.Max(1, pointsPerRequest);
            // ensure batchSize is at least as large as pointsPerRequest to avoid frequent flushes
            batchSize = Math.Max(batchSize, _pointsPerRequest);
            _useGzip = useGzip;

            var handler = new SocketsHttpHandler
            {
                PooledConnectionLifetime = TimeSpan.FromMinutes(10),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                // Allow many concurrent connections per server based on configured send concurrency
                // (set below after computing _sendConcurrency)
            };

            // configure send concurrency and map it to the connection pool limit
            _sendConcurrency = Math.Max(1, sendConcurrency);
            handler.MaxConnectionsPerServer = Math.Max(2, _sendConcurrency);

            _http = new HttpClient(handler, disposeHandler: true)
            {
                // Increase timeout for large uploads; chunking will reduce per-request size but
                // keep a generous timeout to tolerate server-side processing delays.
                Timeout = TimeSpan.FromMinutes(5)
            };
            // Not using chunking for simplified mode
            _chunkSizeBytes = 0;
            _sendSemaphore = new SemaphoreSlim(_sendConcurrency, _sendConcurrency);

            var ub = new UriBuilder("http", host, port);
            if (useV3WriteLp)
            {
                // Use the v3 write_lp endpoint. The API supports a `db` query parameter (db name),
                // and optional flags accept_partial, no_sync and precision.
                string dbName = string.IsNullOrEmpty(v3Db) ? bucket : v3Db;
                var q = new List<string>();
                if (!string.IsNullOrEmpty(dbName)) q.Add($"db={Uri.EscapeDataString(dbName)}");
                q.Add($"accept_partial={(acceptPartial ? "true" : "false")}" );
                q.Add($"no_sync={(noSync ? "true" : "false")}" );
                if (!string.IsNullOrEmpty(precision))
                {
                    // Map short precision tokens to full names expected by v3 API
                    string p = precision.Trim().ToLowerInvariant();
                    string mapped = p switch
                    {
                        "ns" => "nanosecond",
                        "nanosecond" => "nanosecond",
                        "us" => "microsecond",
                        "microsecond" => "microsecond",
                        "ms" => "millisecond",
                        "millisecond" => "millisecond",
                        "s" => "second",
                        "second" => "second",
                        "auto" => "auto",
                        _ => "auto"
                    };
                    q.Add($"precision={Uri.EscapeDataString(mapped)}");
                }

                ub.Path = "/api/v3/write_lp";
                ub.Query = string.Join("&", q);
            }
            else
            {
                // Default to v2 write endpoint (org/bucket model)
                ub.Path = "/api/v2/write";
                ub.Query = $"org={Uri.EscapeDataString(org)}&bucket={Uri.EscapeDataString(bucket)}&precision=ns";
            }
            _writeUri = ub.Uri;

            // Resolve host to an IP once to avoid repeated DNS cost under high load.
            try
            {
                var addrs = Dns.GetHostAddresses(host);
                if (addrs != null && addrs.Length > 0)
                {
                    // prefer IPv4 when available
                    _cachedAddress = Array.Find(addrs, a => a.AddressFamily == AddressFamily.InterNetwork) ?? addrs[0];
                }
            }
            catch
            {
                // DNS failures are non-fatal; we'll fall back to hostname in requests
            }

            // Channel capacity: allow at least one full batch per capacity multiplier to avoid blocking producers
            int channelCapacity = Math.Max(batchSize, _pointsPerRequest) * Math.Max(1, capacityMultiplier);
            _channel = Channel.CreateBounded<string>(new BoundedChannelOptions(channelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            _cts = new CancellationTokenSource();
            // Initialize batch CSV logger if WORKLOAD_LOG_DIR is set
            try
            {
                var logDir = Environment.GetEnvironmentVariable("WORKLOAD_LOG_DIR") ?? "logs/workload";
                Directory.CreateDirectory(logDir);
                var fname = Path.Combine(logDir, $"batches_{DateTime.UtcNow:yyyyMMddTHHmmssZ}.csv");
                _batchLog = new StreamWriter(new FileStream(fname, FileMode.Create, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
                _batchLog.WriteLine("timestamp,points,attempts,success,http_status,latency_ms,error");
                var pfile = Path.Combine(logDir, $"points_{DateTime.UtcNow:yyyyMMddTHHmmssZ}.csv");
                _pointLog = new StreamWriter(new FileStream(pfile, FileMode.Create, FileAccess.Write, FileShare.Read)) { AutoFlush = true };
                _pointLog.WriteLine("timestamp,success,http_status,latency_ms,error");
            }
            catch (Exception)
            {
                // ignore logging init failures
            }

            // Start a fixed pool of sender workers equal to send concurrency. Each worker
            // reads from the channel and sends points, avoiding per-item Task.Run overhead.
            _workerTasks = new Task[_sendConcurrency];
            for (int i = 0; i < _sendConcurrency; i++)
            {
                _workerTasks[i] = Task.Run(() => SendWorkerAsync(_cts.Token));
            }
        }

        // Producer: enqueue a single line
        public ValueTask WritePointAsync(string line)
        {
            if (line is null) return ValueTask.CompletedTask;
            _channel.Writer.WriteAsync(line);
            return ValueTask.CompletedTask;
        }

        // Bulk writer: accept async enumerable of lines, enqueue them and wait for workers to finish
        public async Task WritePointsAsync(IAsyncEnumerable<string> lines)
        {
            await foreach (var line in lines.ConfigureAwait(false))
            {
                await _channel.Writer.WriteAsync(line).ConfigureAwait(false);
            }

            // signal completion and wait for consumer to finish flushing
            _channel.Writer.Complete();
            try
            {
                if (_workerTasks != null)
                {
                    await Task.WhenAll(_workerTasks).ConfigureAwait(false);
                }
            }
            catch { }
        }

        // Worker loop: each worker reads lines from the channel and sends them directly.
        private async Task SendWorkerAsync(CancellationToken ct)
        {
            try
            {
                while (await _channel.Reader.WaitToReadAsync(ct).ConfigureAwait(false))
                {
                    while (_channel.Reader.TryRead(out var line))
                    {
                        await PostSinglePointAsync(line, ct).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // expected on cancellation
            }
            catch (Exception ex)
            {
                try { Console.Error.WriteLine($"SendWorker failed: {ex}"); } catch { }
            }
        }

        private async Task PostBatchAsync(List<string> lines, CancellationToken ct)
        {
            if (lines == null || lines.Count == 0) return;

            // Build payload
            string payload;
            try
            {
                var sb = new StringBuilder();
                foreach (var l in lines)
                {
                    sb.Append(l);
                    sb.Append('\n');
                }
                payload = sb.ToString();
            }
            catch (Exception ex)
            {
                try { Console.Error.WriteLine($"Failed to build batch payload: {ex}"); } catch { }
                return;
            }

            // Respect send concurrency
            if (_sendSemaphore != null)
            {
                await _sendSemaphore.WaitAsync(ct).ConfigureAwait(false);
            }

            try
            {
                // Prepare content factory so we can recreate content for retries
                HttpContent CreateContent()
                {
                    if (_useGzip)
                    {
                        var ms = new MemoryStream();
                        using (var gz = new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: true))
                        using (var sw = new StreamWriter(gz, Encoding.UTF8))
                        {
                            sw.Write(payload);
                        }
                        ms.Seek(0, SeekOrigin.Begin);
                        var sc = new StreamContent(ms);
                        sc.Headers.ContentEncoding.Add("gzip");
                        sc.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
                        return sc;
                    }
                    else
                    {
                        return new StringContent(payload, Encoding.UTF8, "text/plain");
                    }
                }

                int maxRetries = 3;
                int attempt = 0;
                while (true)
                {
                    attempt++;
                    HttpContent content = CreateContent();

                    var targetUri = _writeUri;
                    if (_cachedAddress != null)
                    {
                        var ub = new UriBuilder(_writeUri) { Host = _cachedAddress.ToString() };
                        targetUri = ub.Uri;
                    }

                    using var req = new HttpRequestMessage(HttpMethod.Post, targetUri) { Content = content };
                    if (!string.IsNullOrEmpty(_token))
                        req.Headers.Authorization = new AuthenticationHeaderValue("Token", _token);
                    if (!string.IsNullOrEmpty(_writeUri.Host)) req.Headers.Host = _writeUri.Host;

                    var swt = System.Diagnostics.Stopwatch.StartNew();
                    try
                    {
                        var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
                        swt.Stop();
                        if (resp.IsSuccessStatusCode)
                        {
                            try { _batchLog?.WriteLine($"{DateTime.UtcNow:o},{lines.Count},{attempt},1,{(int)resp.StatusCode},{swt.ElapsedMilliseconds},"); } catch { }
                            break;
                        }
                        else
                        {
                            var body = string.Empty;
                            try { body = await resp.Content.ReadAsStringAsync().ConfigureAwait(false); } catch { }
                            bool shouldRetry = false;
                            if ((int)resp.StatusCode >= 500 || resp.StatusCode == (HttpStatusCode)429) shouldRetry = true;
                            try { Console.Error.WriteLine($"Batch write failed (attempt {attempt}): status={(int)resp.StatusCode}; body={body}"); } catch { }
                            if (!shouldRetry || attempt >= maxRetries)
                            {
                                try { _batchLog?.WriteLine($"{DateTime.UtcNow:o},{lines.Count},{attempt},0,{(int)resp.StatusCode},{swt.ElapsedMilliseconds},{body?.Replace('\n',' ')}"); } catch { }
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        swt.Stop();
                        try { Console.Error.WriteLine($"Batch write exception (attempt {attempt}): {ex}"); } catch { }
                        if (attempt >= maxRetries)
                        {
                            try { _batchLog?.WriteLine($"{DateTime.UtcNow:o},{lines.Count},{attempt},0,0,{swt.ElapsedMilliseconds},{ex.ToString().Replace('\n',' ')}"); } catch { }
                            break;
                        }
                    }

                    // Backoff before retry
                    int backoffMs = 100 * (1 << (attempt - 1)); // 100,200,400
                    backoffMs += new Random().Next(0, 50);
                    try { await Task.Delay(backoffMs, ct).ConfigureAwait(false); } catch { break; }
                }
            }
            finally
            {
                try { _sendSemaphore?.Release(); } catch { }
            }
        }

        // Send a single line as its own HTTP POST (no retries, no gzip, minimal headers)
        private async Task PostSinglePointAsync(string line, CancellationToken ct)
        {
            if (string.IsNullOrEmpty(line)) return;
            var content = new StringContent(line + "\n", Encoding.UTF8, "text/plain");
            var targetUri = _writeUri;
            if (_cachedAddress != null)
            {
                var ub = new UriBuilder(_writeUri) { Host = _cachedAddress.ToString() };
                targetUri = ub.Uri;
            }

            // Throttle concurrency with semaphore so we issue up to SendConcurrency parallel POSTs
            if (_sendSemaphore != null)
            {
                await _sendSemaphore.WaitAsync(ct).ConfigureAwait(false);
            }

            var sw = System.Diagnostics.Stopwatch.StartNew();
            int status = 0;
            bool success = false;
            string? err = null;
            try
            {
                int maxRetries = 3;
                int attempt = 0;
                while (true)
                {
                    attempt++;
                    var contentTry = new StringContent(line + "\n", Encoding.UTF8, "text/plain");
                    var curTarget = targetUri;
                    using var req = new HttpRequestMessage(HttpMethod.Post, curTarget) { Content = contentTry };
                    req.Version = System.Net.HttpVersion.Version11;
                    if (!string.IsNullOrEmpty(_token))
                        req.Headers.Authorization = new AuthenticationHeaderValue("Token", _token);
                    if (!string.IsNullOrEmpty(_writeUri.Host)) req.Headers.Host = _writeUri.Host;

                    try
                    {
                        var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
                        status = (int)resp.StatusCode;
                        success = resp.IsSuccessStatusCode;
                        if (success)
                        {
                            break;
                        }
                        else
                        {
                            try { err = await resp.Content.ReadAsStringAsync().ConfigureAwait(false); } catch { err = "(no body)"; }
                            bool shouldRetry = ((int)resp.StatusCode >= 500) || resp.StatusCode == (HttpStatusCode)429;
                            if (!shouldRetry || attempt >= maxRetries)
                            {
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        err = ex.ToString();
                        if (attempt >= maxRetries) break;
                    }

                    // Backoff
                    int backoffMs = 50 * (1 << (attempt - 1)); // 50,100,200
                    backoffMs += new Random().Next(0, 25);
                    try { await Task.Delay(backoffMs, ct).ConfigureAwait(false); } catch { break; }
                }
            }
            finally
            {
                sw.Stop();
                try
                {
                    // Log per-request for latency debugging. reuse batch log file for simplicity.
                    _batchLog?.WriteLine($"{DateTime.UtcNow:o},1,1,{(success ? 1 : 0)},{status},{sw.ElapsedMilliseconds},{err?.Replace('\n',' ')}");
                }
                catch { }

                try { _sendSemaphore?.Release(); } catch { }
            }
        }

        public void Dispose()
        {
            try
            {
                _cts?.Cancel();
            }
            catch { }

            _cts?.Dispose();
            try { _batchLog?.Flush(); _batchLog?.Dispose(); } catch { }
            try { _sendSemaphore?.Dispose(); } catch { }
            _http?.Dispose();
        }

        // Try to resolve the configured host. Returns true if resolution succeeded.
        private async Task<bool> EnsureHostResolvableAsync(CancellationToken ct)
        {
            try
            {
                // Use DNS to resolve the host name to IP addresses.
                var host = _writeUri.Host;
                if (string.IsNullOrEmpty(host)) return false;
                IPAddress[] addrs = await Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
                return addrs != null && addrs.Length > 0;
            }
            catch (Exception ex) when (ex is SocketException || ex is System.Net.Sockets.SocketException || ex is System.ArgumentException || ex is TaskCanceledException)
            {
                // treat resolution failures as transient
                return false;
            }
            catch
            {
                return false;
            }
        }
    }
}
