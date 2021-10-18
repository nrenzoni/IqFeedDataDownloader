using Autofac;
using CustomShared;
using IqFeedDownloaderLib;
using Models;
using NodaTime;
using StockDataCore.Db;

namespace DownloaderMain
{
    public static class AutofacSetup
    {
        public static ContainerBuilder GetBuilder()
        {
            var builder = new ContainerBuilder();
            builder.Register(c =>
                    new YearNonWeekendClosedDayChecker(
                        IqFeedDownloaderConfigVariables.Instance.MarketDayClosedListDir))
                .As<IYearNonWeekendClosedDayChecker>();
            builder.RegisterType<MarketDayChecker>();
            builder.Register(c => new DailyTicksDownloaderPathBuilder(
                IqFeedDownloaderConfigVariables.Instance.IqfeedTickDataBaseDirectory));

            builder.Register(c =>
                    new MinuteOhlcRepoPg(IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr))
                .As<IOhlcRepoCombined<MinuteOhlc, ZonedDateTime>>()
                .SingleInstance();

            builder.RegisterType<ClickHouseRepoFactory>()
                .SingleInstance();

            builder.RegisterType<DownloadPlanUtils>()
                .SingleInstance();
            builder.RegisterType<DailyOhlcDownloadPlanBuilder>()
                .SingleInstance();
            builder.RegisterType<TickDownloadPlanBuilder>()
                .SingleInstance();

            builder.RegisterType<DownloadUtilHelper>()
                .SingleInstance();

            builder.RegisterType<DailyOhlcController>()
                .SingleInstance();
            builder.Register(c =>
                    new DailyOhlcRepoPg(IqFeedDownloaderConfigVariables.Instance.PostgresConnectionStr))
                .As<IOhlcRepoCombined<DailyOhlc, LocalDate>>()
                .SingleInstance();
            builder.RegisterType<IqDailyOhlcDownloader>();

            builder.RegisterType<IqMinuteOhlcDownloader>();
            builder.RegisterType<MinuteOhlcController>();

            builder.Register(c =>
                new TiSymbolsPerDayRetrieverClient(
                    IqFeedDownloaderConfigVariables.Instance.TiSymbolsPerDayServiceHostAddress));
            return builder;
        }
    }
}