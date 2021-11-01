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
            DotEnvLoader.Load();
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
            builder.Register(c => new DailyTicksDownloaderPathBuilderImpl(
                Path.GetTempPath()));
            builder.RegisterType<DownloadPlanUtils>();
            builder.Register(c => new DailyOhlcDownloadPlanBuilder(
                c.Resolve<MarketDayChecker>(),
                1));
            builder.RegisterType<DailyOhlcController>();
            builder.Register(c =>
                new IqDailyOhlcDownloader(3, 3));
            builder.Register(c =>
                    new DailyOhlcRepoPg(
                        IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr,
                        "daily_ohlc_test"
                    ))
                .AsSelf()
                .As<IOhlcRepoCombined<DailyOhlc, LocalDate>>();
            builder.Register(c =>
                    new MinuteOhlcRepoPg(
                        IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr,
                        "minute_ohlc_test",
                        flushSize: 1000, maxSimultaneousSavers: 50))
                .AsSelf()
                .As<IOhlcRepoCombined<MinuteOhlc, ZonedDateTime>>();

            return builder;
        }
    }
}