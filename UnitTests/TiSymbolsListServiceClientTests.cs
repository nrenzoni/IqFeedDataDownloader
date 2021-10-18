using System.Threading.Tasks;
using IqFeedDownloaderLib;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    public class TiSymbolsListServiceClientTests
    {
        [Test]
        public async Task GetSymbolListPerDayTest()
        {
            var startDate = new LocalDate(2021, 3, 1);
            var endDate = new LocalDate(2021, 4, 1);

            var tiSymbolsPerDayRetrieverClient =
                new TiSymbolsPerDayRetrieverClient(
                    IqFeedDownloaderConfigVariables.Instance.TiSymbolsPerDayServiceHostAddress);
            var symbolListPerDays =
                await tiSymbolsPerDayRetrieverClient.GetBreakoutSymbolListPerDayAsync(startDate, endDate);
        }

        [Test]
        public async Task GetMissingBreakoutOhlcDaysTest()
        {
            var tiSymbolsPerDayRetrieverClient =
                new TiSymbolsPerDayRetrieverClient(
                    IqFeedDownloaderConfigVariables.Instance.TiSymbolsPerDayServiceHostAddress);
            var symbolListPerDays = await tiSymbolsPerDayRetrieverClient.GetMissingBreakoutOhlcDaysAsync();
        }
    }
}