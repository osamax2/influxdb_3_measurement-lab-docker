using System;
using System.IO;
using System.Threading;

namespace Workload
{
    public sealed class RunLogger : IDisposable
    {
        private readonly StreamWriter _writer;
        private readonly object _lock = new object();
        private bool _headerWritten = false;

        public RunLogger(string path)
        {
            Console.WriteLine($"[RunLogger] Creating log file: {path}");
            _writer = new StreamWriter(path, append: false);
            WriteHeader();
            Console.WriteLine($"[RunLogger] Log file stream opened: {_writer.BaseStream}");
        }

        private void WriteHeader()
        {
            if (!_headerWritten)
            {
                Console.WriteLine("[RunLogger] Writing header");
                _writer.WriteLine("ts,event,extra");
                _headerWritten = true;
                _writer.Flush();
                Console.WriteLine("[RunLogger] Header written and flushed");
            }
        }

        public void Event(string evt, string extra = "")
        {
            var ts = DateTimeOffset.UtcNow.ToString("o");
            lock (_lock)
            {
                Console.WriteLine($"[RunLogger] Event: {ts},{evt},{extra}");
                _writer.WriteLine($"{ts},{evt},{extra}");
                _writer.Flush();
                Console.WriteLine($"[RunLogger] Flushed event: {evt}");
            }
        }

        public void Dispose()
        {
            Console.WriteLine("[RunLogger] Disposing logger and flushing file");
            _writer?.Flush();
            _writer?.Dispose();
        }
    }
}
