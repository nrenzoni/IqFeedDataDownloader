using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CustomShared;
using IqFeedDownloaderLib;
using ModelGenerators;
using Models;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    [TestFixture]
    public class MinuteOhlcPgRepoTests
    {
        [OneTimeSetUp]
        public static void Setup()
        {
            TestCommon.SetupTest();
        }

        [Test]
        public async Task WriteRowTest()
        {
            using var repo = new MinuteOhlcRepoPg(
                IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr, 
                "minute_ohlc_test",
                flushSize: 1,
                maxSimultaneousSavers: 1);

            var minuteOhlc = new MinuteOhlc
            {
                Symbol = "TEST",
                Ts = new LocalDateTime(2020, 1, 1, 8, 0).InZoneStrictly(DateUtils.NyDateTz),
                Open = 5,
                High = 10,
                Low = 5,
                Close = 9,
                Volume = 100
            };

            await repo.WriteToDbAsync(new List<MinuteOhlc> { minuteOhlc }, 0);
        }

        [Test]
        public async Task WriteMultiOhlcToDbAsyncTest()
        {
            using var repo = new MinuteOhlcRepoPg(
                IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr, 
                "minute_ohlc_test",
                flushSize: 1,
                maxSimultaneousSavers: 1);

            var minuteOhlcs = Enumerable.Range(1, 10001)
                .Select(_ => OhlcGenerators.GenerateMinuteOhlc())
                .ToList();


            await repo.WriteToDbAsync(minuteOhlcs, 0);
        }
    }
}