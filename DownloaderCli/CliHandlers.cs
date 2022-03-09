using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using CustomShared;
using IqFeedDownloaderLib;
using log4net;
using NodaTime;

namespace DownloaderMain
{
    public class CliHandlers
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(CliHandlers));

        private readonly TiSymbolsPerDayRetrieverClient _tiSymbolsPerDayRetrieverClient;
        private readonly MarketDayChecker _marketDayChecker;
        private readonly MinuteOhlcController _minuteOhlcController;
        private readonly IqTickDownloader _iqTickDownloader;
        private readonly DownloadPlanUtils _downloadPlanUtils;
        private readonly SavedIqTickChecker _savedIqTickChecker;

        public CliHandlers(
            TiSymbolsPerDayRetrieverClient tiSymbolsPerDayRetrieverClient,
            MarketDayChecker marketDayChecker,
            MinuteOhlcController minuteOhlcController,
            IqTickDownloader iqTickDownloader,
            DownloadPlanUtils downloadPlanUtils,
            SavedIqTickChecker savedIqTickChecker)
        {
            _marketDayChecker = marketDayChecker;
            _minuteOhlcController = minuteOhlcController;
            _iqTickDownloader = iqTickDownloader;
            _downloadPlanUtils = downloadPlanUtils;
            _savedIqTickChecker = savedIqTickChecker;
            _tiSymbolsPerDayRetrieverClient = tiSymbolsPerDayRetrieverClient;
        }

        public async Task HandleDownloadIqDataForSymbols(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            switch (opts.StrategyType)
            {
                case CliOpts.StrategyType.Breakouts:
                    await DownloadBreakoutData(opts);
                    break;
                case CliOpts.StrategyType.SectorEtf:
                    await DownloadSectorEtfData(opts);
                    break;
                case CliOpts.StrategyType.PremarketGainers:
                    await DownloadPremarketGainersData(opts);
                    break;
                case CliOpts.StrategyType.RunningUp:
                    await DownloadRunningUpData(opts);
                    break;
                case CliOpts.StrategyType.IntradayGainers:
                    await DownloadIntradayGainersData(opts);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public async Task DownloadBreakoutData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            switch (opts.DataType)
            {
                case CliOpts.DataType.MinuteOhlc:
                case CliOpts.DataType.Default:
                    await DownloadBreakoutMinuteData(opts);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task DownloadSectorEtfData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            switch (opts.DataType)
            {
                case CliOpts.DataType.MinuteOhlc:
                case CliOpts.DataType.Default:
                    await DownloadSectorEtfMinuteData(opts);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task DownloadPremarketGainersData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            switch (opts.DataType)
            {
                case CliOpts.DataType.Tick:
                case CliOpts.DataType.Default:
                    await DownloadPremarketGainersTickData(opts);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task DownloadRunningUpData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            switch (opts.DataType)
            {
                case CliOpts.DataType.Tick:
                case CliOpts.DataType.Default:
                    await DownloadRunningUpTickData(opts);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task DownloadIntradayGainersData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            switch (opts.DataType)
            {
                case CliOpts.DataType.Tick:
                case CliOpts.DataType.Default:
                    await DownloadIntradayGainerTickData(opts);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public async Task DownloadBreakoutMinuteData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            using var tiSymbolsPerDayRetrieverClient = _tiSymbolsPerDayRetrieverClient;

            uint minBreakoutDays = 50;

            var (startingDate, endDate) = GetStartEndDateFromCli(
                opts.StartingFromDate.ParseToLocalDate(), opts.MaxDownloadDayCount,
                CliOpts.DataType.MinuteOhlc);

            Log.Info($"Downloading symbols for breakout ({minBreakoutDays} days) " +
                     $"for [{startingDate.ToYYYYMMDD()}, {endDate.ToYYYYMMDD()}]...");

            List<SymbolListPerDay> breakoutSymbolListPerDay =
                await tiSymbolsPerDayRetrieverClient.GetBreakoutSymbolListPerDayAsync(
                    startingDate, endDate, minimumBreakoutDays: minBreakoutDays);

            var symbolsForDateContainer =
                SymbolsForDateContainer.FromSymbolListPerDay(breakoutSymbolListPerDay);

            var minuteOhlcDownloadPlanBuilder = new MinuteOhlcDownloadPlanBuilder(
                _marketDayChecker,
                20, 20);

            await _minuteOhlcController.DownloadOhlcAsync(symbolsForDateContainer, minuteOhlcDownloadPlanBuilder);
        }

        private async Task DownloadSectorEtfMinuteData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            using var symbolsRetrieverClient = _tiSymbolsPerDayRetrieverClient;

            var startingDate = opts.StartingFromDate.ParseToLocalDate();
            var endDate = _marketDayChecker.GetNextOpenDay(LocalDate.FromDateTime(DateTime.Today), -1);

            Log.Info($"Downloading minute data for sector ETFs " +
                     $"for [{startingDate.ToYYYYMMDD()}, {endDate.ToYYYYMMDD()}]...");

            List<string> sectorEtfSymbols =
                await symbolsRetrieverClient.GetSectorEtfSymbolListAsync();

            var allDates =
                _marketDayChecker.GetMarketOpenDaysInRange(startingDate, endDate)
                    .ToImmutableSortedSet();

            var downloadDaysSchemata =
                _downloadPlanUtils.BuildDownloadDateSchemataContiguousDates(allDates);

            var downloadPlans = new List<DownloadPlan>();

            foreach (var symbol in sectorEtfSymbols)
            {
                downloadPlans.Add(
                    new DownloadPlan
                    {
                        Symbol = symbol,
                        DownloadDateSchemata = downloadDaysSchemata
                    });
            }

            await _minuteOhlcController.DownloadOhlcAsync(downloadPlans);
        }

        private async Task DownloadPremarketGainersTickData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            using var symbolsRetrieverClient = _tiSymbolsPerDayRetrieverClient;

            var (startingDate, endDate) = GetStartEndDateFromCli(
                opts.StartingFromDate.ParseToLocalDate(), opts.MaxDownloadDayCount, CliOpts.DataType.Tick);

            Log.Info($"Downloading tick data for premarket gainer stocks " +
                     $"for [{startingDate.ToYYYYMMDD()}, {endDate.ToYYYYMMDD()}]...");

            var premarketGainerSymbolListPerDay
                = await symbolsRetrieverClient.GetPremarketGainerSymbolsAsync(startingDate, endDate);

            var symbolsForDateContainer =
                SymbolsForDateContainer.FromSymbolListPerDay(premarketGainerSymbolListPerDay);

            var downloadPlanBuilder = new TickDownloadPlanBuilder(
                _marketDayChecker, 5, 5);

            var downloadPlans = downloadPlanBuilder.GetPlans(symbolsForDateContainer);

            _iqTickDownloader.Download(downloadPlans);
        }

        private async Task DownloadRunningUpTickData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            using var symbolsRetrieverClient = _tiSymbolsPerDayRetrieverClient;

            var (startingDate, endDate) = GetStartEndDateFromCli(
                opts.StartingFromDate.ParseToLocalDate(), opts.MaxDownloadDayCount,
                CliOpts.DataType.Tick);

            Log.Info($"Downloading tick data for running up stocks " +
                     $"between [{startingDate.ToYYYYMMDD()}, {endDate.ToYYYYMMDD()}]...");

            var runningUpSymbolListPerDay
                = await symbolsRetrieverClient.GetRunningUpStockSymbolsAsync(startingDate, endDate);

            var symbolsForDateContainer =
                SymbolsForDateContainer.FromSymbolListPerDay(runningUpSymbolListPerDay);

            var downloadPlanBuilder = new TickDownloadPlanBuilder(
                _marketDayChecker, 2, 3);

            var downloadPlans
                = downloadPlanBuilder.GetPlans(symbolsForDateContainer);

            var toRetrieveDownloadPlans
                = RemoveAlreadySavedTicks(downloadPlans);

            _iqTickDownloader.Download(toRetrieveDownloadPlans);
        }
        
        private async Task DownloadIntradayGainerTickData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            using var symbolsRetrieverClient = _tiSymbolsPerDayRetrieverClient;

            var (startingDate, endDate) = GetStartEndDateFromCli(
                opts.StartingFromDate.ParseToLocalDate(), opts.MaxDownloadDayCount,
                CliOpts.DataType.Tick);

            Log.Info($"Downloading tick data for highest ranked intraday gainer stocks " +
                     $"between [{startingDate.ToYYYYMMDD()}, {endDate.ToYYYYMMDD()}]...");

            var intradayGainerStockSymbolData
                = await symbolsRetrieverClient.GetIntradayGainerStockSymbolsAsync(startingDate, endDate);

            var symbolsForDateContainer =
                SymbolsForDateContainer.FromSymbolListPerDay(intradayGainerStockSymbolData);

            var downloadPlanBuilder = new TickDownloadPlanBuilder(
                _marketDayChecker, 2, 3);

            var downloadPlans
                = downloadPlanBuilder.GetPlans(symbolsForDateContainer);

            var toRetrieveDownloadPlans
                = RemoveAlreadySavedTicks(downloadPlans);

            _iqTickDownloader.Download(toRetrieveDownloadPlans);
        }

        private List<DownloadPlan> RemoveAlreadySavedTicks(List<DownloadPlan> downloadPlans)
        {
            var tasks = new List<Task<IList<DownloadPlan>>>();
            foreach (var downloadPlansBatch in downloadPlans.Batch(50))
            {
                tasks.Add(
                    Task.Run(() => BatchRemoveAlreadySavedTicks(downloadPlansBatch)));
            }

            var downloadPlansToKeep = new List<DownloadPlan>();

            tasks.ForEach(t =>
                downloadPlansToKeep.AddRange(t.Result));

            return downloadPlansToKeep;
        }

        private IList<DownloadPlan> BatchRemoveAlreadySavedTicks(IEnumerable<DownloadPlan> downloadPlans)
        {
            List<DownloadPlan> downloadPlansToKeep = new();

            foreach (var downloadPlan in downloadPlans)
            {
                _savedIqTickChecker.RemoveAlreadySaved(downloadPlan);
                if (downloadPlan.DownloadDateSchemata.Count > 0)
                    downloadPlansToKeep.Add(downloadPlan);
            }

            return downloadPlansToKeep;
        }

        public (LocalDate startDate, LocalDate endDate) GetStartEndDateFromCli(LocalDate startDate, uint? maxDays,
            CliOpts.DataType dataType)
        {
            if (!_marketDayChecker.IsOpen(startDate))
                startDate = _marketDayChecker.GetNextOpenDay(startDate);

            if (dataType == CliOpts.DataType.Tick)
            {
                var today = LocalDate.FromDateTime(DateTime.Today);
                var todayMinute180Days = today - Period.FromDays(180);

                if (startDate < todayMinute180Days)
                    startDate = todayMinute180Days;
            }

            LocalDate endDate;
            if (maxDays.HasValue)
            {
                endDate = maxDays.Value == 1
                    ? startDate
                    : _marketDayChecker.GetNextOpenDay(startDate, (int)maxDays.Value - 1);
            }
            else
                endDate = _marketDayChecker.GetNextOpenDay(LocalDate.FromDateTime(DateTime.Today), -1);

            return (startDate, endDate);
        }
    }
}