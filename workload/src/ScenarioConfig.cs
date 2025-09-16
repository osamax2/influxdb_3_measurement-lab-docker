using System;
using System.Collections.Generic;
using System.IO;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Workload;

public sealed class ScenarioParams
{
    public long points { get; set; } = 0;
    public int timestamp_span_sec { get; set; } = 0;
    public int parallel_clients { get; set; } = 1;
    public int series_multiplier { get; set; } = 1;
    public int duration_sec { get; set; } = 10; // default 10 seconds if missing
}

public static class ScenarioConfig
{
    public static IDictionary<string,ScenarioParams> Load(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"Scenario matrix not found: {path}");
        var yaml = File.ReadAllText(path);
        var deserializer = new DeserializerBuilder()
            .WithNamingConvention(UnderscoredNamingConvention.Instance)
            .Build();
        return deserializer.Deserialize<Dictionary<string,ScenarioParams>>(yaml);
    }
}