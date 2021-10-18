using System.IO;
using Autofac;
using CustomShared;
using IqFeedDownloaderLib;
using Models;
using NodaTime;

namespace UnitTests
{
    public static class TestCommon
    {
        public static void SetupTest()
        {
            PgMisc.SetupTypeMapping();
            LogConfig.LogToConsoleForDebug();
        }

        public static ContainerBuilder SetupAutofac()
        {
            var builder = new ContainerBuilder();

            builder.Register(c =>
                new YearNonWeekendClosedDayChecker(
                    IqFeedDownloaderConfigVariables.TestInstance.MarketDayClosedListDir))
                .As<IYearNonWeekendClosedDayChecker>();
            builder.RegisterType<MarketDayChecker>();
            builder.Register(c => new DailyTicksDownloaderPathBuilder(
                Path.GetTempPath()));
            builder.RegisterType<DownloadPlanUtils>();
            builder.Register(c => new DailyOhlcDownloadPlanBuilder(
                c.Resolve<MarketDayChecker>(),
                1));
            builder.Register(c =>
                    new MinuteOhlcRepoPg(
                        IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr,
                        flushSize: 1000))
                .AsSelf()
                .As<IOhlcRepoCombined<MinuteOhlc, ZonedDateTime>>();

            return builder;
        }
    }
}