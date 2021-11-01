using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using IQFeed.CSharpApiClient.Lookup.Historical.Enums;
using log4net;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class IqTickDownloader : IqBaseDownloader
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(IqTickDownloader));

        private ConcurrentQueue<DownloadPlan> _toDownload;

        private readonly DownloadPlanUtils _downloadPlanUtils;

        private readonly DailyTicksDownloaderPathBuilderImpl _dailyTicksDownloaderPathBuilder;


        public IqTickDownloader(
            DownloadPlanUtils downloadPlanUtils,
            DailyTicksDownloaderPathBuilderImpl dailyTicksDownloaderPathBuilder,
            uint maxRetry = 5, uint nConcurrent = 20)
            : base(maxRetry, nConcurrent)
        {
            _downloadPlanUtils = downloadPlanUtils;
            _dailyTicksDownloaderPathBuilder = dailyTicksDownloaderPathBuilder;

            Directory.CreateDirectory(_dailyTicksDownloaderPathBuilder.GetCombinedDateCsvsDirectory());
        }

        protected override void DownloadInit(IEnumerable<DownloadPlan> downloadPlans)
        {
            _toDownload = new();
            foreach (var downloadPlan in downloadPlans)
            {
                _toDownload.Enqueue(downloadPlan);
            }
        }

        protected override async Task DownloadTask()
        {
            while (true)
            {
                var dequeueSuccess = _toDownload.TryDequeue(out var downloadPlan);
                if (!dequeueSuccess)
                    break;

                await DownloadDownloadPlan(downloadPlan);
            }
        }

        protected override bool CanDownload => true;

        async Task DownloadDownloadPlan(DownloadPlan downloadPlan)
        {
            var symbol = downloadPlan.Symbol;

            foreach (var dateSchema in downloadPlan.DownloadDateSchemata)
            {
                Log.Info($"Downloading ticks for [{symbol}], {dateSchema}...");

                // string tmpFilename = await DownloadAndReturnFilename(symbol, date);
                string tmpFilename = await DownloadIqHelper(
                    symbol, dateSchema, DownloadAndReturnFilenameInner2);
                if (tmpFilename == null)
                    return;

                var writePath = _dailyTicksDownloaderPathBuilder.GetWritePath(symbol, dateSchema);
                File.Move(tmpFilename, writePath);
            }

            Log.Info($"Finished downloading ticks for symbol [{symbol}], {downloadPlan.AsDatesString}.");
        }

        private async Task<string> DownloadAndReturnFilenameInner2(
            string symbol, DownloadDateSchema schema)
        {
            return await LookupClient.Historical.File.GetHistoryTickTimeframeAsync(
                symbol,
                schema.StartDate.ToDateTimeUnspecified(),
                endDate: schema.EndDate.ToDateTimeUnspecified().AddHours(23),
                dataDirection: DataDirection.Oldest);
        }
    }

    public class DownloadUtilHelper
    {
        public ConcurrentQueue<Tuple<string, LocalDate>> BuildDatesToSymbolToDownloadQueue(
            IEnumerable<Tuple<string, LocalDate>> datesToSymbolToDownload)
        {
            ConcurrentQueue<Tuple<string, LocalDate>> concurrentQueue = new();

            foreach (var symbolDate in datesToSymbolToDownload)
            {
                concurrentQueue.Enqueue(new Tuple<string, LocalDate>(symbolDate.Item1, symbolDate.Item2));
            }

            return concurrentQueue;
        }
    }
}