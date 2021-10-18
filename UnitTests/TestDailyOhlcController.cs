using Autofac;
using IqFeedDownloaderLib;
using Models;
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
            var builder = new ContainerBuilder();
            builder.RegisterType<DailyOhlcRepoPg>();
            builder.RegisterType<DailyOhlcController>();
            builder.Register(c => new DailyOhlcRepoPg(
                    IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr))
                .AsSelf()
                .As<IOhlcRepoCombined<DailyOhlc, LocalDate>>();

            using var scope = builder.Build();

            var dailyOhlcController = scope.Resolve<DailyOhlcController>();

            var dailyOhlcDownloadPlanBuilder = _scope.Resolve<DailyOhlcDownloadPlanBuilder>();

            var symbolsForDateContainer = new SymbolsForDateContainer();
            symbolsForDateContainer.AddSymbolToDateToDownload(
                new LocalDate(2021, 3, 23), "UPC");
            
            var plans = dailyOhlcDownloadPlanBuilder.GetPlans(symbolsForDateContainer);

            dailyOhlcController.DownloadAndSaveMissingAsync(plans);
        }
    }
}