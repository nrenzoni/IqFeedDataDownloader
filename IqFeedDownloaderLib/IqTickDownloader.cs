using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CustomShared;
using IQFeed.CSharpApiClient.Common.Exceptions;
using IQFeed.CSharpApiClient.Lookup.Historical.Enums;
using log4net;
using Models;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class IqTickDownloader : IqBaseDownloader
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(IqTickDownloader));

        private ConcurrentQueue<SymbolDatePair> _toDownload;

        private readonly DownloadPlanUtils _downloadPlanUtils;

        private readonly DailyTicksDownloaderPathBuilder _dailyTicksDownloaderPathBuilder;


        public IqTickDownloader(
            DownloadPlanUtils downloadPlanUtils,
            DailyTicksDownloaderPathBuilder dailyTicksDownloaderPathBuilder,
            uint maxRetry = 3, uint nConcurrent = 20)
            : base(maxRetry, nConcurrent)
        {
            _downloadPlanUtils = downloadPlanUtils;
            _dailyTicksDownloaderPathBuilder = dailyTicksDownloaderPathBuilder;
        }

        protected override void DownloadInit(IEnumerable<DownloadPlan> downloadPlans)
        {
            _toDownload = new();
            foreach (var tuple in downloadPlans.SelectMany(_downloadPlanUtils.GetSymbolDateTuples))
            {
                _toDownload.Enqueue(tuple);
            }
        }

        protected override async Task DownloadTask()
        {
            while (true)
            {
                var dequeueSuccess = _toDownload.TryDequeue(out var downloadPlan);
                if (!dequeueSuccess)
                    break;

                await DownloadSymbolDatePair(downloadPlan);
            }
        }

        async Task DownloadSymbolDatePair(SymbolDatePair symbolDatePair)
        {
            var symbol = symbolDatePair.Symbol;
            var date = symbolDatePair.Date;

            var existingFiles = _dailyTicksDownloaderPathBuilder.GetAllPaths(
                    symbol, date)
                .Where(File.Exists)
                .ToList();
            if (existingFiles.Any())
            {
                Log.Info($"File {existingFiles.First()} already exists, skipping.");
                return;
            }

            Log.Info($"Downloading symbol [{symbol}] for date [{date.ToYYYYMMDD()}]...");


            // string tmpFilename = await DownloadAndReturnFilename(symbol, date);
            string tmpFilename = await DownloadIqHelper(
                symbol, new DownloadDateSchema(date), DownloadAndReturnFilenameInner2);
            if (tmpFilename == null)
                return;

            var writePath = _dailyTicksDownloaderPathBuilder.GetWritePath(symbol, date);
            Directory.CreateDirectory(Path.GetDirectoryName(writePath));
            File.Move(tmpFilename, writePath);

            Log.Info($"Finished downloading symbol [{symbol}] for date [{date.ToYYYYMMDD()}].");
        }

        private async Task<string> DownloadAndReturnFilename(
            string symbol, LocalDate date)
        {
            var maxRetry = MaxRetry;
            while (true)
            {
                try
                {
                    var tmpFilename = await DownloadAndReturnFilenameInner(symbol, date);
                    return tmpFilename;
                }
                catch (NoDataIQFeedException noDataIqFeedException)
                {
                    Log.Warn($"No data available from IqFeed for {symbol}, {date.ToString()}.");
                    return null;
                }
                catch (IQFeedException otherException)
                {
                    maxRetry--;
                    if (maxRetry > 0)
                        continue;

                    Log.Warn($"Max retries {MaxRetry} hit for {symbol} {date.ToYYYYMMDD()}.");
                    return null;
                }
            }
        }

        private async Task<string> DownloadAndReturnFilenameInner(
            string symbol, LocalDate date)
        {
            return await LookupClient.Historical.File.GetHistoryTickTimeframeAsync(
                symbol,
                date.ToDateTimeUnspecified(),
                date.ToDateTimeUnspecified().AddHours(23),
                dataDirection: DataDirection.Oldest);
        }

        private async Task<string> DownloadAndReturnFilenameInner2(
            string symbol, DownloadDateSchema schema)
        {
            if (!schema.IsSingleDay)
                throw new Exception($"{nameof(DownloadAndReturnFilenameInner2)} " +
                                    $"only supports schema with single day.");

            return await LookupClient.Historical.File.GetHistoryTickTimeframeAsync(
                symbol,
                schema.SingleDate.ToDateTimeUnspecified(),
                schema.SingleDate.ToDateTimeUnspecified().AddHours(23),
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