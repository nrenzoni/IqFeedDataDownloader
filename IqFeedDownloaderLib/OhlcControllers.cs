using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CustomShared;
using IQFeed.CSharpApiClient.Lookup.Historical.Messages;
using log4net;
using Models;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public interface IOhlcController
    {
        public Task<List<DownloadPlan>> GetUnsavedDownloadPlans(List<DownloadPlan> downloadPlansToCheck);

        public Task DownloadOhlcAsync(
            SymbolsForDateContainer symbolsForDateContainer,
            IDownloadPlanBuilder ohlcDownloadPlanBuilder);
    }

    public abstract class OhlcControllerBase<TDownloader, TOhlc, TTime, TMsg>
        : IOhlcController
        where TDownloader : IqOhlcBaseDownloader<TOhlc, TTime, TMsg>
        where TOhlc : Ohlc<TTime>
    {
        private static readonly ILog Log = LogManager.GetLogger(
            typeof(OhlcControllerBase<TDownloader, TOhlc, TTime, TMsg>).GetRealTypeName());

        private readonly TDownloader _iqDailyOhlcDownloader;

        private readonly IOhlcRepoCombined<TOhlc, TTime> _ohlcRepo;

        private readonly DownloadPlanUtils _downloadPlanUtils;


        protected OhlcControllerBase(
            TDownloader iqDailyOhlcDownloader,
            IOhlcRepoCombined<TOhlc, TTime> ohlcRepo, DownloadPlanUtils downloadPlanUtils)
        {
            _iqDailyOhlcDownloader = iqDailyOhlcDownloader;
            _ohlcRepo = ohlcRepo;
            _downloadPlanUtils = downloadPlanUtils;
        }

        public async Task<List<DownloadPlan>> GetUnsavedDownloadPlans(List<DownloadPlan> downloadPlans)
        {
            Log.Info($"Getting unsaved plans to download for {downloadPlans.Count} plans...");

            var unsavedPlans = new List<DownloadPlan>();

            foreach (var downloadPlan in downloadPlans)
            {
                var newDownloadPlan =
                    await FilterOutAlreadySavedDatesAsyncIndividualPlan(downloadPlan);
                if (newDownloadPlan == null)
                    continue;

                unsavedPlans.Add(newDownloadPlan);
            }

            return unsavedPlans;
        }

        public async Task<DownloadPlan> FilterOutAlreadySavedDatesAsyncIndividualPlan(DownloadPlan downloadPlan)
        {
            if (downloadPlan.DownloadDateSchemata.Count == 0)
                return null;

            var savedDates = await _ohlcRepo.GetSavedDatesAsync(downloadPlan);
            var symbolDateSet = _downloadPlanUtils.ToMarketDaySymbolDateSet(downloadPlan);
            symbolDateSet.Dates.ExceptWith(savedDates);

            if (symbolDateSet.Dates.Count == 0)
                return null;

            var newDateSchemata =
                _downloadPlanUtils.BuildDownloadDateSchemataContiguousDates(
                    symbolDateSet.Dates.ToImmutableSortedSet());

            return new DownloadPlan
            {
                Symbol = downloadPlan.Symbol,
                DownloadDateSchemata = newDateSchemata
            };
        }

        public async Task DownloadOhlcAsync(
            List<DownloadPlan> downloadPlans)
        {
            var obs = new IqOhlcBaseDownloaderObserverSaver<TOhlc, TTime>(_ohlcRepo);
            using var _ = _iqDailyOhlcDownloader.Subscribe(obs);

            var unsavedDownloadPlans =
                await GetUnsavedDownloadPlans(downloadPlans);

            if (unsavedDownloadPlans.Count == 0)
            {
                Log.Info("No plans to download.");
                return;
            }

            _iqDailyOhlcDownloader.Download(unsavedDownloadPlans);
        }

        public async Task DownloadOhlcAsync(
            SymbolsForDateContainer symbolsForDateContainer,
            IDownloadPlanBuilder ohlcDownloadPlanBuilder)
        {
            var downloadPlans =
                ohlcDownloadPlanBuilder.GetPlans(symbolsForDateContainer)
                    .ToList();

            await DownloadOhlcAsync(downloadPlans);
        }
    }

    public class IqOhlcBaseDownloaderObserverSaver<TOhlc, TTime> : IObserver<TOhlc>
        where TOhlc : Ohlc<TTime>
    {
        private static readonly ILog Log = LogManager.GetLogger(
            typeof(IqOhlcBaseDownloaderObserverSaver<TOhlc, TTime>).GetRealTypeName());

        private readonly IOhlcRepoCombined<TOhlc, TTime> _ohlcRepoSaverBaseSaver;

        public ManualResetEvent IsFinished { get; } = new(false);

        public IqOhlcBaseDownloaderObserverSaver(IOhlcRepoCombined<TOhlc, TTime> ohlcRepoSaverBaseSaver)
        {
            _ohlcRepoSaverBaseSaver = ohlcRepoSaverBaseSaver;
        }

        public void OnCompleted()
        {
            Log.Info("Finished IqOhlcBaseDownloaderObserverSaver.");
            IsFinished.Set();
        }

        public void OnError(Exception error)
        {
            IsFinished.Set();
            throw new NotImplementedException();
        }

        public void OnNext(TOhlc value)
        {
            _ohlcRepoSaverBaseSaver.Save(value);
        }
    }

    public class DailyOhlcController
        : OhlcControllerBase<IqDailyOhlcDownloader, DailyOhlc, LocalDate, DailyWeeklyMonthlyMessage>
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DailyOhlcController));


        public DailyOhlcController(
            IOhlcRepoCombined<DailyOhlc, LocalDate> dailyOhlcRepo,
            IqDailyOhlcDownloader iqDailyOhlcDownloader,
            DownloadPlanUtils downloadPlanUtils)
            : base(iqDailyOhlcDownloader, dailyOhlcRepo, downloadPlanUtils)
        {
        }
    }

    public class MinuteOhlcController
        : OhlcControllerBase<IqMinuteOhlcDownloader, MinuteOhlc, ZonedDateTime, IntervalMessage>
    {
        public MinuteOhlcController(
            IqMinuteOhlcDownloader iqDailyOhlcDownloader,
            IOhlcRepoCombined<MinuteOhlc, ZonedDateTime> dailyOhlcRepo,
            DownloadPlanUtils downloadPlanUtils)
            : base(iqDailyOhlcDownloader, dailyOhlcRepo, downloadPlanUtils)
        {
        }
    }
}