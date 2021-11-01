using System.Threading.Tasks;
using Autofac;
using CustomShared;
using IqFeedDownloaderLib;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    [TestFixture]
    public class MinuteOhlcControllerTests
    {
        private static IContainer _scope;

        [OneTimeSetUp]
        public static void Setup()
        {
            TestCommon.SetupTest();
            _scope = TestCommon.SetupAutofac().Build();
        }

        [TearDown]
        public static void TearDown()
        {
            _scope.Dispose();
        }

        [Test]
        public async Task TestDownloadOhlc()
        {
            using var iqMinuteOhlcDownloader = new IqMinuteOhlcDownloader(nConcurrent: 1);
            var minuteOhlcController = new MinuteOhlcController(
                iqMinuteOhlcDownloader,
                _scope.Resolve<MinuteOhlcRepoPg>(),
                _scope.Resolve<DownloadPlanUtils>());

            var downloadPlanBuilder = new DayPeriodDownloadPlanBuilder(
                _scope.Resolve<MarketDayChecker>(),
                0, 0);

            var symbolsForDateContainer = new SymbolsForDateContainer();
            symbolsForDateContainer.AddSymbolToDateToDownload(
                new LocalDate(2021, 3, 23), "UPC");

            await minuteOhlcController.DownloadOhlcAsync(symbolsForDateContainer, downloadPlanBuilder);
        }

        [Test]
        public async Task GetUnsavedDownloadPlansAsyncTest()
        {
            var iqMinuteOhlcDownloader = new IqMinuteOhlcDownloader(nConcurrent: 1);
            var minuteOhlcController = new MinuteOhlcController(
                iqMinuteOhlcDownloader,
                _scope.Resolve<MinuteOhlcRepoPg>(),
                _scope.Resolve<DownloadPlanUtils>());

            var downloadPlanBuilder = new DayPeriodDownloadPlanBuilder(
                _scope.Resolve<MarketDayChecker>(),
                0, 0);

            var symbolsForDateContainer = new SymbolsForDateContainer();
            symbolsForDateContainer.AddSymbolToDateToDownload(
                new LocalDate(2021, 3, 23), "UPC");

            var plans = downloadPlanBuilder.GetPlans(symbolsForDateContainer);

            var unsavedPlans = await minuteOhlcController.GetUnsavedDownloadPlans(plans);
        }
    }
}