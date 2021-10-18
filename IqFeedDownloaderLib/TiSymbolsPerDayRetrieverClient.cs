using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Web;
using CustomShared;
using NodaTime;
using NodaTime.Serialization.SystemTextJson;

namespace IqFeedDownloaderLib
{
    public class TiSymbolsPerDayRetrieverClient : IDisposable
    {
        private readonly string _hostAddress;
        private readonly JsonSerializerOptions _jsonSerializerOptions;
        private readonly HttpClient _client;

        public TiSymbolsPerDayRetrieverClient(string hostAddress)
        {
            _hostAddress = hostAddress;
            _jsonSerializerOptions = BuildJsonSerializerOptions();
            _client = new HttpClient();
        }

        private UriBuilder UriBuilderBase
        {
            get
            {
                var uriBuilder = new UriBuilder(_hostAddress + "/symbols/");
                uriBuilder.Port = 5000;
                return uriBuilder;
            }
        }

        private string BuildGetSymbolsForBreakoutsUri(LocalDate? startDate, LocalDate endDate)
        {
            var uriBuilder = UriBuilderBase;
            uriBuilder.Path += "breakouts";

            var query = HttpUtility.ParseQueryString(uriBuilder.Query);
            if (startDate.HasValue)
                query["start_date"] = startDate.Value.ToYYYYMMDD();
            query["end_date"] = endDate.ToYYYYMMDD();
            uriBuilder.Query = query.ToString();
            var uri = uriBuilder.ToString();
            return uri;
        }

        private string BuildGetMissingBreakoutOhlcDays(LocalDate? startDate, LocalDate? endDate)
        {
            var uriBuilder = UriBuilderBase;
            uriBuilder.Path += "breakouts/missing-days";

            var query = HttpUtility.ParseQueryString(uriBuilder.Query);
            if (startDate.HasValue)
                query["start_date"] = startDate.Value.ToYYYYMMDD();
            if (endDate.HasValue)
                query["end_date"] = endDate.Value.ToYYYYMMDD();
            uriBuilder.Query = query.ToString();
            var uri = uriBuilder.ToString();
            return uri;
        }

        public async Task<List<SymbolListPerDay>> GetBreakoutSymbolListPerDayAsync(LocalDate? startDate,
            LocalDate endDate)
        {
            var uri = BuildGetSymbolsForBreakoutsUri(startDate, endDate);
            return await GetAsync<List<SymbolListPerDay>>(uri, _client);
        }

        public async Task<List<LocalDate>> GetMissingBreakoutOhlcDaysAsync(LocalDate? startDate = null,
            LocalDate? endDate = null)
        {
            var uri = BuildGetMissingBreakoutOhlcDays(startDate, endDate);
            var jsonDoc = await GetAsync<JsonDocument>(uri, _client);

            return jsonDoc.RootElement.GetProperty("missing_days").EnumerateArray()
                .Select(dateStr => dateStr.ToString().ParseToLocalDate())
                .ToList();
        }

        private async Task<T> GetAsync<T>(string uri, HttpClient httpClient)
        {
            return await httpClient.GetFromJsonAsync<T>(uri, _jsonSerializerOptions);

            try
            {
            }
            catch (HttpRequestException) // Non success
            {
                Console.WriteLine("An error occurred.");
            }
            catch (NotSupportedException) // When content type is not valid
            {
                Console.WriteLine("The content type is not supported.");
            }
            catch (JsonException) // Invalid JSON
            {
                Console.WriteLine("Invalid JSON.");
            }
        }

        private JsonSerializerOptions BuildJsonSerializerOptions()
        {
            var jsonSerializerOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = new SnakeCaseNamingPolicy(),
                WriteIndented = true
            };
            jsonSerializerOptions.ConfigureForNodaTime(DateTimeZoneProviders.Tzdb);
            return jsonSerializerOptions;
        }

        public void Dispose()
        {
            _client.Dispose();
        }
    }

    public class SnakeCaseNamingPolicy : JsonNamingPolicy
    {
        public override string ConvertName(string name)
        {
            return ToSnakeCase(name);
        }

        public static string ToSnakeCase(string text)
        {
            if (text == null)
            {
                throw new ArgumentNullException(nameof(text));
            }

            if (text.Length < 2)
            {
                return text;
            }

            var sb = new StringBuilder();
            sb.Append(char.ToLowerInvariant(text[0]));
            for (int i = 1; i < text.Length; ++i)
            {
                char c = text[i];
                if (char.IsUpper(c))
                {
                    sb.Append('_');
                    sb.Append(char.ToLowerInvariant(c));
                }
                else
                {
                    sb.Append(c);
                }
            }

            return sb.ToString();
        }
    }
}