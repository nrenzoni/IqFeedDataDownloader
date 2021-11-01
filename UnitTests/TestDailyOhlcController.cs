using Autofac;
using IqFeedDownloaderLib;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    [TestFixture]
    public class TestDailyOhlcController
    {
        private static IContainer _scope;

        [OneTimeSetUp]
        public static void Setup()
        {
            TestCommon.SetupTest();
            _scope = TestCommon.SetupAutofac().Build();
        }

        [Test]
        public void TestDailyOhlcController1()
        {
            var dailyOhlcController = _scope.Resolve<DailyOhlcController>();

            var dailyOhlcDownloadPlanBuilder = _scope.Resolve<DailyOhlcDownloadPlanBuilder>();

            var symbolsForDateContainer = new SymbolsForDateContainer();
            symbolsForDateContainer.AddSymbolToDateToDownload(
                new LocalDate(2021, 3, 23), "UPC");

            var plans = dailyOhlcDownloadPlanBuilder.GetPlans(symbolsForDateContainer);

            dailyOhlcController.DownloadOhlcAsync(plans);
        }
    }
}