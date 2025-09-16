using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Workload;

public static class PointGenerator
{
    static readonly Random _rand = new();

    /// <summary>
    /// Generate line protocol lines for a chunk of rows.
    /// We stream-build to avoid storing all points in memory.
    /// </summary>
    public static IEnumerable<string> Generate(
        string[] measurements,
        IDictionary<string,string> baseTags,
        long count,
        DateTimeOffset tsStartUtc,
        int tsSpanSec,
        int seriesMultiplier)
    {
        // To reduce allocations
        var sb = new StringBuilder(capacity:256);

        Console.WriteLine($"[PointGenerator] Generate called: measurements=[{string.Join(",", measurements)}], count={count}, seriesMultiplier={seriesMultiplier}");
        for (int s=0; s<seriesMultiplier; s++)
        {
            // add per-series tag
            var seriesTag = $"s{s}";
            Console.WriteLine($"[PointGenerator] Generating series {seriesTag} with {count} points");
            long yielded = 0;
            foreach (var line in GenSeries(measurements, baseTags, seriesTag, count, tsStartUtc, tsSpanSec))
            {
                yielded++;
                if (yielded <= 5 || yielded == count) // print first 5 and last
                    Console.WriteLine($"[PointGenerator] Yielded line: {line}");
                yield return line;
            }
            Console.WriteLine($"[PointGenerator] Series {seriesTag} yielded {yielded} lines");
        }
    }

    static IEnumerable<string> GenSeries(string[] measurements,
        IDictionary<string,string> baseTags,
        string seriesTag,
        long count,
        DateTimeOffset tsStartUtc,
        int tsSpanSec)
    {
        // Construct static tag prefix: measurement,<tags>
        // We'll append ",series=sX"
        // escape minimal: replace spaces/commas with backslash (not exhaustive)
        string Esc(string v) => v.Replace(" ", "\\ ").Replace(",", "\\,").Replace("=", "\\=");
        var tagPrefixBase = new StringBuilder();
        foreach (var kv in baseTags)
        {
            tagPrefixBase.Append(',').Append(Esc(kv.Key)).Append('=').Append(Esc(kv.Value));
        }
        tagPrefixBase.Append(',').Append("series").Append('=').Append(Esc(seriesTag));
        var tagSuffix = tagPrefixBase.ToString();

        for (long i=0; i<count; i++)
        {
            DateTimeOffset t = tsSpanSec == 0
                ? tsStartUtc
                : tsStartUtc.AddSeconds(i % tsSpanSec);
            long ns = t.ToUnixTimeMilliseconds() * 1_000_000; // ms->ns (coarse; good enough for comp)
            // Debug output for first few points
            if (i < 3)
                Console.WriteLine($"[GenSeries] Generating point {i}: ts={t:o}, ns={ns}");
            // simulate value
            double val = _rand.NextDouble() * 100.0;
            string valStr = val.ToString(CultureInfo.InvariantCulture);

            foreach (var m in measurements)
            {
                yield return $"{m}{tagSuffix} value={valStr} {ns}";
            }
        }
    }
}
