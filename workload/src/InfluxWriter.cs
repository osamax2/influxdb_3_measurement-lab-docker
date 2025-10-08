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
    }

    public sealed class InfluxWriter : IDisposable
    {
        private readonly HttpClient _http;
        private readonly Uri _writeUri;
        private readonly string _token;
        private readonly Channel<string> _channel;
        private readonly int _pointsPerRequest;
        private readonly bool _useGzip;

        private readonly Task _consumerTask;
    private CancellationTokenSource? _cts;
    // Cache a resolved IP for the write host to avoid repeated DNS lookups
    private IPAddress? _cachedAddress;
    // Batch CSV logger
    private StreamWriter? _batchLog;

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
                MaxConnectionsPerServer = Math.Max(2, Environment.ProcessorCount * 2),
            };

            _http = new HttpClient(handler, disposeHandler: true)
            {
                Timeout = TimeSpan.FromSeconds(60)
            };

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
            }
            catch (Exception)
            {
                // ignore logging init failures
            }
            _consumerTask = Task.Run(() => ConsumerLoopAsync(_cts.Token));
        }

        // Producer: enqueue a single line
        public ValueTask WritePointAsync(string line)
        {
            if (line is null) return ValueTask.CompletedTask;
            _channel.Writer.WriteAsync(line);
            return ValueTask.CompletedTask;
        }

        // Bulk writer: accept async enumerable of lines, enqueue them and wait for consumer to finish
        public async Task WritePointsAsync(IAsyncEnumerable<string> lines)
        {
            await foreach (var line in lines.ConfigureAwait(false))
            {
                await _channel.Writer.WriteAsync(line).ConfigureAwait(false);
            }

            // signal completion and wait for consumer to finish flushing
            _channel.Writer.Complete();
            try { await _consumerTask.ConfigureAwait(false); } catch { }
        }

        // Consumer: batch lines into requests
        private async Task ConsumerLoopAsync(CancellationToken ct)
        {
            var buffer = new List<string>(_pointsPerRequest);

            try
            {
                while (await _channel.Reader.WaitToReadAsync(ct))
                {
                    while (_channel.Reader.TryRead(out var line))
                    {
                        buffer.Add(line);

                        if (buffer.Count >= _pointsPerRequest)
                        {
                            await PostBatchAsync(buffer, ct).ConfigureAwait(false);
                            buffer.Clear();
                        }
                    }
                }

                if (buffer.Count > 0)
                {
                    await PostBatchAsync(buffer, ct).ConfigureAwait(false);
                    buffer.Clear();
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                // best-effort: write to stderr so container logs capture it
                try { Console.Error.WriteLine($"Consumer loop failed: {ex}"); } catch { }
            }
        }

        private async Task PostBatchAsync(List<string> lines, CancellationToken ct)
        {
            var payload = string.Join('\n', lines) + '\n';

            // Prepare payload bytes once (either compressed or raw). We'll recreate HttpContent per attempt
            byte[] payloadBytes;
            bool isGzip = false;

            if (_useGzip)
            {
                try
                {
                    var raw = Encoding.UTF8.GetBytes(payload);
                    using (var ms = new MemoryStream())
                    {
                        using (var gz = new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: false))
                        {
                            gz.Write(raw, 0, raw.Length);
                        }
                        payloadBytes = ms.ToArray();
                    }
                    isGzip = true;
                }
                catch (Exception ex)
                {
                    try { Console.Error.WriteLine($"Gzip compression failed, sending uncompressed: {ex}"); } catch { }
                    payloadBytes = Encoding.UTF8.GetBytes(payload);
                    isGzip = false;
                }
            }
            else
            {
                payloadBytes = Encoding.UTF8.GetBytes(payload);
            }

            int maxRetries = 3;
            var rand = new Random();

            int attemptsMade = 0;
            bool success = false;
            int httpStatus = 0;
            string errorMsg = string.Empty;
            long latencyMs = 0;

            for (int attempt = 0; attempt < maxRetries; attempt++)
            {
                if (ct.IsCancellationRequested)
                {
                    attemptsMade = attempt;
                    break;
                }
                // Before attempting network I/O, ensure the host name resolves to an IP.
                // If DNS fails, wait/backoff and retry so transient DNS hiccups (Docker DNS flaps)
                // don't immediately produce noisy stack traces.
                bool resolvable = await EnsureHostResolvableAsync(ct).ConfigureAwait(false);
                if (!resolvable)
                {
                    try { Console.Error.WriteLine($"Host name resolution failed for {_writeUri.Host}, backing off and will retry (attempt={attempt})"); } catch { }
                    // perform backoff and skip attempting the HTTP request this iteration
                    int dnsBackoffMs = (int)(100 * Math.Pow(2, attempt));
                    dnsBackoffMs += new Random().Next(0, 100);
                    try { await Task.Delay(dnsBackoffMs, ct).ConfigureAwait(false); } catch (OperationCanceledException) { return; }
                    continue;
                }

                HttpContent content = new ByteArrayContent(payloadBytes);
                try
                {
                    if (isGzip) content.Headers.ContentEncoding.Add("gzip");
                    content.Headers.ContentType = new MediaTypeHeaderValue("text/plain") { CharSet = "utf-8" };

                    // If we have a cached address, construct a request URI using that IP and
                    // preserve the original Host header so virtual-hosting on the server still works.
                    Uri targetUri = _writeUri;
                    if (_cachedAddress != null)
                    {
                        var ub = new UriBuilder(_writeUri)
                        {
                            Host = _cachedAddress.ToString()
                        };
                        targetUri = ub.Uri;
                    }

                    using var req = new HttpRequestMessage(HttpMethod.Post, targetUri) { Content = content };
                    if (!string.IsNullOrEmpty(_token))
                        req.Headers.Authorization = new AuthenticationHeaderValue("Token", _token);
                    // set Host header to original host so server sees expected Host value
                    if (!string.IsNullOrEmpty(_writeUri.Host)) req.Headers.Host = _writeUri.Host;

                    HttpResponseMessage resp;
                    try
                    {
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
                        sw.Stop();
                        latencyMs = sw.ElapsedMilliseconds;
                        attemptsMade = attempt + 1;
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        attemptsMade = attempt + 1;
                        break; // cancelled
                    }

                    if (resp.IsSuccessStatusCode)
                    {
                        // success
                        success = true;
                        httpStatus = (int)resp.StatusCode;
                        // log after loop
                        break;
                    }

                    // not success - read body for diagnostics
                    string body = string.Empty;
                    try { body = await resp.Content.ReadAsStringAsync().ConfigureAwait(false); } catch (Exception ex) { body = ex.Message; }

                    int status = (int)resp.StatusCode;
                    // Retry for server errors (5xx), otherwise log and break
                    if (status >= 500 && attempt < maxRetries - 1)
                    {
                        try { Console.Error.WriteLine($"Transient server error, will retry: status={status}; points={lines.Count}; attempt={attempt}"); } catch { }
                    }
                    else
                    {
                        try { Console.Error.WriteLine($"Failed to post batch: status={status}; points={lines.Count}; url={_writeUri}; body={body}"); } catch { }
                        httpStatus = status;
                        errorMsg = body ?? string.Empty;
                        attemptsMade = attempt + 1;
                        break;
                    }
                }
                catch (HttpRequestException hre)
                {
                    // Network-level errors - retryable
                    if (attempt < maxRetries - 1)
                    {
                        try { Console.Error.WriteLine($"Failed to post batch exception (network), will retry: {hre.Message}; attempt={attempt}"); } catch { }
                    }
                    else
                    {
                        try { Console.Error.WriteLine($"Failed to post batch exception: {hre}"); } catch { }
                        errorMsg = hre.Message;
                        attemptsMade = attempt + 1;
                        break;
                    }
                }
                catch (Exception ex)
                {
                    // Other errors: log and don't retry
                    try { Console.Error.WriteLine($"Failed to post batch exception: {ex}"); } catch { }
                    errorMsg = ex.Message;
                    attemptsMade = attempt + 1;
                    break;
                }
                finally
                {
                    // content will be disposed by HttpRequestMessage / using var req scope
                }

                // backoff before next attempt (exponential with jitter)
                int backoffMsNext = (int)(100 * Math.Pow(2, attempt)); // 100ms, 200ms, 400ms ...
                // apply small random jitter up to +100ms
                backoffMsNext += rand.Next(0, 100);
                try { await Task.Delay(backoffMsNext, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
            }

            // Write instrumentation line for this batch
            try
            {
                if (_batchLog != null)
                {
                    var ts = DateTime.UtcNow.ToString("o");
                    var line = $"{ts},{lines.Count},{attemptsMade},{(success?1:0)},{httpStatus},{latencyMs},\"{errorMsg.Replace('"','\'')}\"";
                    _batchLog.WriteLine(line);
                }
            }
            catch { }
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
