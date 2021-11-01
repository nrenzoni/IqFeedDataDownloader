using System;
using System.IO;
using Autofac;
using CustomShared;
using IqFeedDownloaderLib;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    public class TestIqTickDownloader
    {
        private static IContainer _scope;

        [OneTimeSetUp]
        public static void Setup()
        {
            TestCommon.SetupTest();
            _scope = TestCommon.SetupAutofac().Build();
        }

        [Test]
        public void IqTickDownloaderTest1()
        {
            var marketDayChecker = _scope.Resolve<MarketDayChecker>();

            var prevMarketDay =
                marketDayChecker.GetNextOpenDay(LocalDate.FromDateTime(DateTime.Today), -1);

            var symbolsForDateContainer = new SymbolsForDateContainer();
            symbolsForDateContainer.AddSymbolToDateToDownload(prevMarketDay, "SPY");

            var downloadPlanBuilder = new TickDownloadPlanBuilder(
                _scope.Resolve<MarketDayChecker>(),
                0, 0);

            var downloadPlans = downloadPlanBuilder.GetPlans(symbolsForDateContainer);


            var iqTickDownloader = new IqTickDownloader(
                _scope.Resolve<DownloadPlanUtils>(),
                _scope.Resolve<DailyTicksDownloaderPathBuilderImpl>(),
                nConcurrent: 1);

            iqTickDownloader.Download(downloadPlans);
        }

        [Test]
        public void IqTickDownloaderTest2()
        {
            var marketDayChecker = _scope.Resolve<MarketDayChecker>();

            var prevMarketDay =
                marketDayChecker.GetNextOpenDay(LocalDate.FromDateTime(DateTime.Today), -1);

            var symbolsForDateContainer = new SymbolsForDateContainer();
            symbolsForDateContainer.AddSymbolToDateToDownload(prevMarketDay, "SPY");

            var downloadPlanBuilder = new TickDownloadPlanBuilder(
                _scope.Resolve<MarketDayChecker>(),
                0, 0);

            var downloadPlans = downloadPlanBuilder.GetPlans(symbolsForDateContainer);


            using var iqTickDownloader = new IqTickDownloader(
                _scope.Resolve<DownloadPlanUtils>(),
                _scope.Resolve<DailyTicksDownloaderPathBuilderImpl>(),
                nConcurrent: 1);

            iqTickDownloader.Download(downloadPlans);
        }
    }
}