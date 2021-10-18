using System;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using CommandLine;
using CustomShared;
using IqFeedDownloaderLib;
using log4net;
using Models;
using NodaTime;
using StockDataCore.Db;
using StockDataCore.Models;

namespace DownloaderMain
{
    class Program : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Program));

        private readonly IContainer _scope;

        public Program()
        {
            _scope = AutofacSetup.GetBuilder().Build();
        }


        static async Task Main(string[] args)
        {
            LogConfig.LogToConsoleForDebug();
            // DotEnvLoader.Load();
            PgMisc.SetupTypeMapping();

            await Main2(args);
            return;

            // const string outputIqTickDataDirectory = IqFeedDownloaderConfigVariables.Instance.IqfeedTickDataBaseDirectory;

            LocalDate startDate = new LocalDate(2021, 4, 1);
            LocalDate endDate = new LocalDate(2021, 10, 1);

            {
                using var p = new Program();
                // var symbolsForDateContainer = p.GetRawSymbolsForDateContainer();
                var symbolsForDateContainer = p.GetRawSymbolsForDateContainer2(
                    startDate, endDate);
                p.DownloadTickData(symbolsForDateContainer);
                // await p.DownloadDailyOhlc(symbolsForDateContainer);
            }

            Log.Info("Finished.");
        }

        public SymbolsForDateContainer GetRawSymbolsForDateContainerFromDir()
        {
            var directoryOfTopOvernightGainerSymbols = IqFeedDownloaderConfigVariables.Instance.topListSymbolOutputDir;
            var topListSymbolsList = TopListSymbols.BuildFromDirectory(directoryOfTopOvernightGainerSymbols);

            return TopListSymbolsToContainerConverter.Convert(topListSymbolsList);
        }

        public SymbolsForDateContainer GetRawSymbolsForDateContainer2(
            LocalDate startDate, LocalDate endDate)
        {
            SymbolsForDateContainer symbolsForDateContainer = new();

            var clickhouseTopListRepo = _scope.Resolve<ClickHouseRepoFactory>().BuildTopListRepo();

            TopListParams topListParams = new TopListParams(
                new LocalTime(9, 25, 0, 0),
                "FCP",
                true);

            foreach (var date in _scope.Resolve<MarketDayChecker>().GetMarketOpenDaysInRange(startDate, endDate))
            {
                var topListSymbols = clickhouseTopListRepo.GetTopSymbols(topListParams, date);
                symbolsForDateContainer.AddSymbolsToDateToDownload(date, topListSymbols);
            }

            return symbolsForDateContainer;
        }

        public void DownloadTickData(SymbolsForDateContainer symbolsForDateContainer)
        {
            var downloadPlanBuilder = _scope.Resolve<TickDownloadPlanBuilder>();

            var downloadPlans = downloadPlanBuilder.GetPlans(symbolsForDateContainer);

            using var iqTickDownloader = new IqTickDownloader(
                _scope.Resolve<DownloadPlanUtils>(),
                _scope.Resolve<DailyTicksDownloaderPathBuilder>());

            iqTickDownloader.Download(downloadPlans);
        }

        public async Task DownloadDailyOhlc(SymbolsForDateContainer symbolsForDateContainer)
        {
            var ohlcDownloadPlanBuilder = new DailyOhlcDownloadPlanBuilder(
                _scope.Resolve<MarketDayChecker>());

            using var dailyOhlcController = _scope.Resolve<DailyOhlcController>();

            await dailyOhlcController.DownloadOhlcAsync(symbolsForDateContainer, ohlcDownloadPlanBuilder);
        }

        public void Dispose()
        {
            _scope.Dispose();
        }

        public static async Task Main2(string[] args)
        {
            if (!DotEnvLoader.Load())
                return;

            var parseResults = Parser.Default.ParseArguments<
                CliOpts.DownloadIqDataForSymbolsOpts>(args);

            {
                var builder = AutofacSetup.GetBuilder();
                await using var scope = builder.Build();

                var handlers = new CliHandlers(
                    scope.Resolve<TiSymbolsPerDayRetrieverClient>(),
                    scope.Resolve<MarketDayChecker>(),
                    scope.Resolve<MinuteOhlcController>());

                await parseResults
                    .WithParsedAsync<CliOpts.DownloadIqDataForSymbolsOpts>(handlers.HandleDownloadIqDataForSymbols);
            }
        }
    }
}