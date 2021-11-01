using System.Collections.Generic;
using System.Text.Json.Serialization;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class SymbolListPerDay
    {
        [JsonPropertyName("date")] public LocalDate Date { get; set; }

        [JsonPropertyName("symbols")] public List<string> Symbols { get; set; }
    }
}