using System.Collections.Generic;
using System.Text.Json.Serialization;
using NodaTime;

namespace IqFeedDownloaderLib;

public class SymbolListPerDayTime
{
    [JsonPropertyName("date")] public LocalDate Date { get; set; }

    [JsonPropertyName("time")] public LocalTime Time { get; set; }

    [JsonPropertyName("symbols")] public List<string> Symbols { get; set; }
}