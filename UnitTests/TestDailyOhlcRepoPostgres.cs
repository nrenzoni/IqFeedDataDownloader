using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using IqFeedDownloaderLib;
using Models;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    public class TestDayDataPgDb
    {
        [OneTimeSetUp]
        public static void Setup()
        {
            TestCommon.SetupTest();
        }

        [Test]
        public void TestDayDataPgDb1()
        {
            using var dayDataPgDb = new DailyOhlcRepoPg(
                IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr);

            for (int i = 2; i < 4; i++)
            {
                var ohlc = new DailyOhlc
                {
                    Symbol = "ABC_" + i,
                    Ts = new LocalDate(2021, 1, 1),
                    Open = 5,
                    High = 6,
                    Low = 4,
                    Close = 5,
                    Volume = 154235434
                };

                dayDataPgDb.Save(ohlc);
            }
        }

        // integration
        [Test]
        public async Task TestGetAlreadySavedAsync()
        {
            using var repo = new DailyOhlcRepoPg(
                IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr);

            var toCheck = new List<SymbolDatePair>
            {
                new()
                {
                    Symbol = "UPC",
                    Date = new LocalDate(2021, 3, 24)
                }
            };

            var alreadySavedRes = await repo.GetAlreadySavedDaysAsync(toCheck);
        }
    }
}