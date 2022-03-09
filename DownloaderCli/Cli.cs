using System;
using System.Threading.Tasks;
using Autofac;
using CommandLine;
using CustomShared;
using IqFeedDownloaderLib;
using log4net;

namespace DownloaderMain
{
    class Cli : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Cli));

        private readonly IContainer _scope;

        public Cli()
        {
            _scope = AutofacSetup.GetBuilder().Build();
        }


        static async Task Main(string[] args)
        {
            LogConfig.LogToConsoleForDebug();
            // DotEnvLoader.Load();
            PgMisc.SetupTypeMapping();

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
                    scope.Resolve<MinuteOhlcController>(),
                    scope.Resolve<IqTickDownloader>(),
                    scope.Resolve<DownloadPlanUtils>(),
                    scope.Resolve<SavedIqTickChecker>());

                await parseResults
                    .WithParsedAsync(handlers.HandleDownloadIqDataForSymbols);
            }

            Log.Info("Finished CLI run.");
        }

        public SymbolsForDateContainer GetRawSymbolsForDateContainerFromDir()
        {
            var directoryOfTopOvernightGainerSymbols = IqFeedDownloaderConfigVariables.Instance.topListSymbolOutputDir;
            var topListSymbolsList = TopListSymbols.BuildFromDirectory(directoryOfTopOvernightGainerSymbols);

            return TopListSymbolsToContainerConverter.Convert(topListSymbolsList);
        }

        public async Task DownloadDailyOhlc(SymbolsForDateContainer symbolsForDateContainer)
        {
            var ohlcDownloadPlanBuilder = new DailyOhlcDownloadPlanBuilder(
                _scope.Resolve<MarketDayChecker>());

            var dailyOhlcController = _scope.Resolve<DailyOhlcController>();

            await dailyOhlcController.DownloadOhlcAsync(symbolsForDateContainer, ohlcDownloadPlanBuilder);
        }

        public void Dispose()
        {
            _scope.Dispose();
        }
    }
}