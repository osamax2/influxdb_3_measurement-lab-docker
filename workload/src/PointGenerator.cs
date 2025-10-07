using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Workload;

public static class PointGenerator
{
    private static readonly ThreadLocal<Random> _rand = new ThreadLocal<Random>(() => new Random(Guid.NewGuid().GetHashCode()));
    // Do not reuse a ThreadLocal<StringBuilder> across an async iterator's
    // suspension/resumption points. Create a fresh local StringBuilder in the
    // generator to avoid occasional ArgumentOutOfRangeException from
    // StringBuilder.ToString() when execution hops threads.

    public static IEnumerable<string> Generate(
        string[] measurements,
        IDictionary<string, string> baseTags,
        long count,
        DateTimeOffset tsStartUtc,
        int tsSpanSec,
        int seriesMultiplier)
    {
        // Precompute tag strings for each series
        var tagStrings = new string[seriesMultiplier];
        for (int s = 0; s < seriesMultiplier; s++)
        {
            tagStrings[s] = BuildTagString(baseTags, $"s{s}");
        }

        // Precompute measurement strings
        var measurementStrings = new string[measurements.Length];
        for (int i = 0; i < measurements.Length; i++)
        {
            measurementStrings[i] = EscapeMeasurement(measurements[i]);
        }

        // Generate points
        for (int s = 0; s < seriesMultiplier; s++)
        {
            foreach (var point in GenSeries(measurementStrings, tagStrings[s], count, tsStartUtc, tsSpanSec))
            {
                yield return point;
            }
        }
    }

    private static string BuildTagString(IDictionary<string, string> baseTags, string seriesTag)
    {
        var sb = new StringBuilder();
        foreach (var kv in baseTags)
        {
            sb.Append(',')
              .Append(EscapeTagKey(kv.Key))
              .Append('=')
              .Append(EscapeTagValue(kv.Value));
        }
        sb.Append(",series=").Append(EscapeTagValue(seriesTag));
        return sb.ToString();
    }

    private static IEnumerable<string> GenSeries(
        string[] measurementStrings,
        string tagString,
        long count,
        DateTimeOffset tsStartUtc,
        int tsSpanSec)
    {
        var nsBase = tsStartUtc.ToUnixTimeMilliseconds() * 1_000_000;
        
        for (long i = 0; i < count; i++)
        {
            long ns = tsSpanSec == 0
                ? nsBase
                : nsBase + (i % tsSpanSec) * 1_000_000_000L;
            
            double val = _rand.Value!.NextDouble() * 100.0;
            string valStr = val.ToString("F6", CultureInfo.InvariantCulture);

                        foreach (var measurement in measurementStrings)
                        {
                                // Use a local StringBuilder per yielded value. This is cheap and
                                // avoids subtle threading issues when the iterator resumes on
                                // a different thread.
                                var sb = new StringBuilder(256);
                                sb.Append(measurement)
                                    .Append(tagString)
                                    .Append(" value=")
                                    .Append(valStr)
                                    .Append(' ')
                                    .Append(ns);

                                yield return sb.ToString();
                        }
        }
    }

    private static string EscapeMeasurement(string value)
    {
        return value.Replace(" ", "\\ ")
                   .Replace(",", "\\,");
    }

    private static string EscapeTagKey(string value)
    {
        return value.Replace(" ", "\\ ")
                   .Replace(",", "\\,")
                   .Replace("=", "\\=");
    }

    private static string EscapeTagValue(string value)
    {
        return value.Replace(" ", "\\ ")
                   .Replace(",", "\\,")
                   .Replace("=", "\\=");
    }
}