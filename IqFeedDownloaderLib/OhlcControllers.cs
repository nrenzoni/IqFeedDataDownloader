using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using IQFeed.CSharpApiClient.Lookup.Historical.Messages;
using log4net;
using Models;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public interface IOhlcController
    {
        public void DownloadAndSaveMissingAsync(IEnumerable<DownloadPlan> downloadPlans);

        public Task<List<DownloadPlan>> GetUnsavedDownloadPlansAsync(List<DownloadPlan> downloadPlansToCheck);

        public Task FilterOutAlreadySavedDatesAsync(List<SymbolDateSet> symbolDateSetList);

        public Task DownloadOhlcAsync(
            SymbolsForDateContainer symbolsForDateContainer,
            IDownloadPlanBuilder ohlcDownloadPlanBuilder);
    }

    public abstract class OhlcControllerBase<TDownloader, TOhlc, TTime, TMsg>
        : IOhlcController, IDisposable
        where TDownloader : IqOhlcBaseDownloader<TOhlc, TTime, TMsg>
        where TOhlc : Ohlc<TTime>
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(OhlcControllerBase<,,,>));

        private readonly TDownloader _iqDailyOhlcDownloader;

        private readonly IOhlcRepoCombined<TOhlc, TTime> _ohlcRepoSaverBaseSaver;

        private readonly ConcurrentQueue<TOhlc> _toVerify = new();
        private readonly uint _verifySavedSize = 10000;

        private readonly CancellationTokenSource _cts = new();

        private readonly Task _backgroundSaver;

        private readonly DownloadPlanUtils _downloadPlanUtils;


        protected OhlcControllerBase(
            TDownloader iqDailyOhlcDownloader,
            IOhlcRepoCombined<TOhlc, TTime> dailyOhlcRepoSaverBaseSaver, DownloadPlanUtils downloadPlanUtils)
        {
            _iqDailyOhlcDownloader = iqDailyOhlcDownloader;
            _ohlcRepoSaverBaseSaver = dailyOhlcRepoSaverBaseSaver;
            _downloadPlanUtils = downloadPlanUtils;

            _backgroundSaver = Task.Run(async () => await GetUnsavedAndSaveAsync());
        }

        public void DownloadAndSaveMissingAsync(IEnumerable<DownloadPlan> downloadPlans)
        {
            _iqDailyOhlcDownloader.Download(downloadPlans);

            while (!_iqDailyOhlcDownloader.FinishedToken.IsCancellationRequested ||
                   _iqDailyOhlcDownloader.DownloadedOhlc.Count > 0)
            {
                var res = _iqDailyOhlcDownloader.DownloadedOhlc.TryDequeue(out var popped);
                if (res == false)
                {
                    Thread.Sleep(100);
                    continue;
                }

                _toVerify.Enqueue(popped);
            }
        }

        public async Task<List<DownloadPlan>> GetUnsavedDownloadPlansAsync(List<DownloadPlan> downloadPlansToCheck)
        {
            Log.Info($"Getting unsaved plans to download for {downloadPlansToCheck.Count} plans...");

            var marketDaySymbolDateSetList =
                _downloadPlanUtils.ToMarketDaySymbolDateSetList(downloadPlansToCheck);

            // first filter already downloaded
            await FilterOutAlreadySavedDatesAsync(marketDaySymbolDateSetList);

            // then build contiguous schemata for missing dates per symbol
            var missingDownloadPlans = new List<DownloadPlan>();

            foreach (var symbolDateSet in marketDaySymbolDateSetList)
            {
                if (symbolDateSet.Dates.Count == 0)
                    continue;

                var downloadDateSchemata =
                    _downloadPlanUtils.BuildDownloadDateSchemataContiguousDates(symbolDateSet.Dates);

                missingDownloadPlans.Add(
                    new DownloadPlan
                    {
                        Symbol = symbolDateSet.Symbol,
                        DownloadDateSchemata = downloadDateSchemata
                    });
            }

            return missingDownloadPlans;
        }

        private async Task GetUnsavedAndSaveAsync()
        {
            while (!_cts.IsCancellationRequested || _toVerify.Count > 0)
            {
                if (_toVerify.Count < _verifySavedSize && !_cts.IsCancellationRequested)
                {
                    await Task.Delay(100);
                    continue;
                }

                List<Tuple<SymbolDatePair, TOhlc>> toVerify = GetSymbolDatesToVerifyIfSavedOuter();

                Dictionary<string, SymbolDateSet> alreadySaved =
                    await GetAlreadySaved(toVerify.Select(i => i.Item1)
                        .ToHashSet().ToList());

                var missingToSave = GetMissingToSave(toVerify, alreadySaved);

                foreach (var dailyOhlc in missingToSave)
                {
                    _ohlcRepoSaverBaseSaver.Save(dailyOhlc);
                }
            }
        }

        protected List<TOhlc> GetMissingToSave(
            List<Tuple<SymbolDatePair, TOhlc>> toVerify,
            Dictionary<string, SymbolDateSet> alreadySaved)
        {
            List<TOhlc> missingToSave = new();

            foreach (var (symbolDate, ohlcVal) in toVerify)
            {
                if (alreadySaved.ContainsKey(symbolDate.Symbol)
                    && alreadySaved[symbolDate.Symbol].Dates.Contains(symbolDate.Date))
                    continue;

                missingToSave.Add(ohlcVal);
            }

            return missingToSave;
        }

        private List<Tuple<SymbolDatePair, TOhlc>> GetSymbolDatesToVerifyIfSavedOuter()
        {
            var toVerify = new List<Tuple<SymbolDatePair, TOhlc>>();
            for (int i = 0; i < _verifySavedSize; i++)
            {
                var dequeueRes = _toVerify.TryDequeue(out var poppedDailyOhlc);
                if (dequeueRes == false)
                    break;

                var toVerifyInnerRes = GetSymbolDatesToVerifyIfSavedTemplate(poppedDailyOhlc);
                toVerify.Add(toVerifyInnerRes);
            }

            return toVerify;
        }

        protected abstract Tuple<SymbolDatePair, TOhlc> GetSymbolDatesToVerifyIfSavedTemplate(TOhlc poppedDailyOhlc);

        private async Task<Dictionary<string, SymbolDateSet>> GetAlreadySaved(
            List<SymbolDatePair> symbolDatePairs)
        {
            return
                await _ohlcRepoSaverBaseSaver.GetAlreadySavedDaysAsync(
                    symbolDatePairs);
        }

        public async Task FilterOutAlreadySavedDatesAsync(List<SymbolDateSet> symbolDateSetList)
        {
            List<SymbolDatePair> symbolWithDateTuples =
                symbolDateSetList.SelectMany(i =>
                        i.Dates.Select(x =>
                            new SymbolDatePair { Symbol = i.Symbol, Date = x }
                        ))
                    .ToList();

            var alreadySaved = await GetAlreadySaved(symbolWithDateTuples);

            foreach (var symbolDate in symbolDateSetList)
            {
                if (alreadySaved.ContainsKey(symbolDate.Symbol))
                    symbolDate.Dates.ExceptWith(alreadySaved[symbolDate.Symbol].Dates);
            }
        }

        public async Task DownloadOhlcAsync(
            SymbolsForDateContainer symbolsForDateContainer,
            IDownloadPlanBuilder ohlcDownloadPlanBuilder)
        {
            var downloadPlans =
                ohlcDownloadPlanBuilder.GetPlans(symbolsForDateContainer)
                    .ToList();

            var unsavedDownloadPlans =
                await GetUnsavedDownloadPlansAsync(downloadPlans);

            if (unsavedDownloadPlans.Count == 0)
            {
                Log.Info("No plans to download.");
                return;
            }
            
            DownloadAndSaveMissingAsync(unsavedDownloadPlans);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _backgroundSaver.Wait();
        }
    }

    public class DailyOhlcController
        : OhlcControllerBase<IqDailyOhlcDownloader, DailyOhlc, LocalDate, DailyWeeklyMonthlyMessage>
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DailyOhlcController));


        public DailyOhlcController(
            IOhlcRepoCombined<DailyOhlc, LocalDate> dailyOhlcRepoSaverBaseSaver,
            IqDailyOhlcDownloader iqDailyOhlcDownloader,
            DownloadPlanUtils downloadPlanUtils)
            : base(iqDailyOhlcDownloader, dailyOhlcRepoSaverBaseSaver, downloadPlanUtils)
        {
        }

        protected override Tuple<SymbolDatePair, DailyOhlc> GetSymbolDatesToVerifyIfSavedTemplate(
            DailyOhlc poppedDailyOhlc)
        {
            var symbolDate = new SymbolDatePair
            {
                Symbol = poppedDailyOhlc.Symbol,
                Date = poppedDailyOhlc.Ts
            };

            return
                new Tuple<SymbolDatePair, DailyOhlc>(
                    symbolDate, poppedDailyOhlc);
        }
    }

    public class MinuteOhlcController
        : OhlcControllerBase<IqMinuteOhlcDownloader, MinuteOhlc, ZonedDateTime, IntervalMessage>
    {
        public MinuteOhlcController(
            IqMinuteOhlcDownloader iqDailyOhlcDownloader,
            IOhlcRepoCombined<MinuteOhlc, ZonedDateTime> dailyOhlcRepoSaverBaseSaver,
            DownloadPlanUtils downloadPlanUtils)
            : base(iqDailyOhlcDownloader, dailyOhlcRepoSaverBaseSaver, downloadPlanUtils)
        {
        }

        protected override Tuple<SymbolDatePair, MinuteOhlc> GetSymbolDatesToVerifyIfSavedTemplate(
            MinuteOhlc poppedDailyOhlc)
        {
            var symbolDate = new SymbolDatePair
            {
                Symbol = poppedDailyOhlc.Symbol,
                Date = poppedDailyOhlc.Ts.Date
            };

            return
                new Tuple<SymbolDatePair, MinuteOhlc>(
                    symbolDate, poppedDailyOhlc);
        }
    }
}