using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CustomShared;
using IqFeedDownloaderLib;
using NodaTime;

namespace DownloaderMain
{
    public class CliHandlers
    {
        private readonly TiSymbolsPerDayRetrieverClient _tiSymbolsPerDayRetrieverClient;
        private readonly MarketDayChecker _marketDayChecker;
        private readonly MinuteOhlcController _minuteOhlcController;

        public CliHandlers(TiSymbolsPerDayRetrieverClient tiSymbolsPerDayRetrieverClient,
            MarketDayChecker marketDayChecker, MinuteOhlcController minuteOhlcController)
        {
            _marketDayChecker = marketDayChecker;
            _minuteOhlcController = minuteOhlcController;
            _tiSymbolsPerDayRetrieverClient = tiSymbolsPerDayRetrieverClient;
        }

        public async Task HandleDownloadIqDataForSymbols(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            switch (opts.StrategyType)
            {
                case CliOpts.StrategyType.Breakouts:
                    await DownloadBreakoutData(opts);
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
                    await DownloadBreakoutMinuteData(opts);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public async Task DownloadBreakoutMinuteData(CliOpts.DownloadIqDataForSymbolsOpts opts)
        {
            using var tiSymbolsPerDayRetrieverClient = _tiSymbolsPerDayRetrieverClient;

            var startingDate = opts.StartingFromDate.ParseToLocalDate();

            var endDate = opts.MaxDownloadDayCount.HasValue
                ? _marketDayChecker.GetNextOpenDay(startingDate, (int)opts.MaxDownloadDayCount.Value)
                : _marketDayChecker.GetNextOpenDay(LocalDate.FromDateTime(DateTime.Today), -1);

            List<SymbolListPerDay> breakoutSymbolListPerDay =
                await tiSymbolsPerDayRetrieverClient.GetBreakoutSymbolListPerDayAsync(startingDate, endDate);

            var symbolsForDateContainer =
                SymbolsForDateContainer.FromSymbolListPerDay(breakoutSymbolListPerDay);

            var minuteOhlcDownloadPlanBuilder = new MinuteOhlcDownloadPlanBuilder(
                _marketDayChecker,
                20, 20);

            await _minuteOhlcController.DownloadOhlcAsync(symbolsForDateContainer, minuteOhlcDownloadPlanBuilder);
        }
    }
}