using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CustomShared;
using IQFeed.CSharpApiClient.Common.Exceptions;
using IQFeed.CSharpApiClient.Lookup;
using IQFeed.CSharpApiClient.Lookup.Historical.Enums;
using IQFeed.CSharpApiClient.Lookup.Historical.Messages;
using log4net;
using Models;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public interface IIqDownloader
    {
        public void Download(IEnumerable<DownloadPlan> downloadPlans);
    }

    public abstract class IqBaseDownloader : IDisposable, IIqDownloader
    {
        protected readonly uint MaxRetry;
        private readonly uint _nConcurrent;
        private static readonly ILog Log = LogManager.GetLogger(typeof(IqBaseDownloader));

        protected readonly LookupClient LookupClient;

        private readonly List<Task> _concurrentTasks;
        private Task _finishedWatcherTask;

        private readonly CancellationTokenSource _finishedCts = new();
        public CancellationToken FinishedToken => _finishedCts.Token;


        protected IqBaseDownloader(uint maxRetry = 3, uint nConcurrent = 20)
        {
            MaxRetry = maxRetry;
            _nConcurrent = nConcurrent;
            _concurrentTasks = new List<Task>((int)_nConcurrent);
            LookupClient = LookupClientFactory.CreateNew((int)nConcurrent);
        }

        public void Download(IEnumerable<DownloadPlan> downloadPlans)
        {
            DownloadInit(downloadPlans);

            LookupClient.Connect();

            DownloadAndInitTasks();
        }

        protected abstract void DownloadInit(IEnumerable<DownloadPlan> downloadPlans);

        private void DownloadAndInitTasks()
        {
            Log.Info($"Starting {_nConcurrent} download worker threads...");

            for (var i = 0; i < _nConcurrent; i++)
            {
                _concurrentTasks.Add(
                    Task.Run(async () => await DownloadTask())
                );
            }

            _finishedWatcherTask = Task.Run(AllFinishedWatcher);
        }

        protected abstract Task DownloadTask();

        private void AllFinishedWatcher()
        {
            TasksRunner.WaitForAllTasksToComplete(_concurrentTasks);
            _finishedCts.Cancel();
        }

        public void Dispose()
        {
            if (_finishedWatcherTask != null)
                _finishedWatcherTask.Wait();
            if (LookupClient != null)
                LookupClient.Disconnect();
        }

        protected async Task<TRet> DownloadIqHelper<TRet>(
            string symbol, DownloadDateSchema schema,
            Func<string, DownloadDateSchema, Task<TRet>> func)
            where TRet : class
        {
            var maxRetry = MaxRetry;
            while (true)
            {
                try
                {
                    var retObj = await func(symbol, schema);
                    return retObj;
                }
                catch (NoDataIQFeedException noDataIqFeedException)
                {
                    Log.Warn($"No data available from IqFeed for {symbol} " +
                             $"with dates: {schema}.");
                    return null;
                }

                catch (IQFeedException otherException)
                {
                    maxRetry--;
                    if (maxRetry <= 0)
                    {
                        Log.Warn($"Max retries {MaxRetry} hit for {symbol} with {schema}.");
                        return null;
                    }

                    continue;
                }
                catch (Exception e)
                {
                    Log.Error($"Caught unexpected exception", e);
                    return null;
                }
            }
        }
    }

    public abstract class IqOhlcBaseDownloader<TOhlc, TTime, TMsg> : IqBaseDownloader
        where TOhlc : Ohlc<TTime>
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(IqOhlcBaseDownloader<,,>));

        public ConcurrentQueue<TOhlc> DownloadedOhlc { get; } = new();

        private ConcurrentQueue<DownloadPlan> _downloadPlans;


        protected IqOhlcBaseDownloader(uint maxRetry = 3, uint nConcurrent = 20)
            : base(maxRetry, nConcurrent)
        {
        }

        protected override void DownloadInit(IEnumerable<DownloadPlan> downloadPlans)
        {
            _downloadPlans = new();
            foreach (var downloadPlan in downloadPlans)
            {
                _downloadPlans.Enqueue(downloadPlan);
            }
        }

        protected override async Task DownloadTask()
        {
            while (true)
            {
                var dequeueSuccess = _downloadPlans.TryDequeue(out var downloadPlan);
                if (!dequeueSuccess)
                    break;

                await DownloadPlan(downloadPlan);
            }
        }

        private async Task DownloadPlan(DownloadPlan downloadPlan)
        {
            foreach (DownloadDateSchema schema in downloadPlan.DownloadDateSchemata)
            {
                Log.Info($"Downloading data for symbol [{downloadPlan.Symbol}] " +
                         $"for schema {schema}.");

                var messages =
                    await DownloadIqHelper(downloadPlan.Symbol, schema, DownloadIqMessages);

                if (messages == null)
                    continue;

                var ohlcObjs = messages.Select(m => ToOhlc(m, downloadPlan.Symbol));

                foreach (var ohlc in ohlcObjs)
                {
                    DownloadedOhlc.Enqueue(ohlc);
                }
            }
        }

        private Task<IEnumerable<TMsg>> DownloadIqMessages(string symbol, DownloadDateSchema schema)
        {
            return DownloadIqMessages(
                symbol,
                schema.StartDate.ToDateTimeUnspecified(),
                schema.EndDate.ToDateTimeUnspecified().AddHours(23));
        }

        protected abstract Task<IEnumerable<TMsg>> DownloadIqMessages(
            string symbol, DateTime startTime,
            DateTime endTime);

        protected abstract TOhlc ToOhlc(TMsg message, string symbol);
    }

    public class IqDailyOhlcDownloader : IqOhlcBaseDownloader<DailyOhlc, LocalDate, DailyWeeklyMonthlyMessage>
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(IqDailyOhlcDownloader));

        public IqDailyOhlcDownloader(uint maxRetry = 3, uint nConcurrent = 20)
            : base(maxRetry, nConcurrent)
        {
        }

        protected override Task<IEnumerable<DailyWeeklyMonthlyMessage>> DownloadIqMessages(
            string symbol, DateTime startTime, DateTime endTime)
        {
            return LookupClient.Historical.GetHistoryDailyTimeframeAsync(
                symbol,
                startTime,
                endTime,
                dataDirection: DataDirection.Oldest);
        }

        protected override DailyOhlc ToOhlc(DailyWeeklyMonthlyMessage message, string symbol)
            => new()
            {
                Symbol = symbol,
                Ts = message.Timestamp.CreateNyDateTime().Date,
                Open = message.Open,
                High = message.High,
                Low = message.Low,
                Close = message.Close,
                Volume = message.PeriodVolume
            };
    }

    public class IqMinuteOhlcDownloader : IqOhlcBaseDownloader<MinuteOhlc, ZonedDateTime, IntervalMessage>
    {
        public IqMinuteOhlcDownloader(uint maxRetry = 3, uint nConcurrent = 20)
            : base(maxRetry, nConcurrent)
        {
        }

        protected override Task<IEnumerable<IntervalMessage>> DownloadIqMessages(
            string symbol, DateTime startTime, DateTime endTime)
        {
            return LookupClient.Historical.GetHistoryIntervalTimeframeAsync(
                symbol,
                60,
                startTime,
                endTime,
                dataDirection: DataDirection.Oldest);
        }

        protected override MinuteOhlc ToOhlc(IntervalMessage message, string symbol)
        {
            var localDt = LocalDateTime.FromDateTime(message.Timestamp);
            var ts = DateUtils.NyDateTz.AtStrictly(localDt);

            return new()
            {
                Symbol = symbol,
                Ts = ts,
                Open = message.Open,
                High = message.High,
                Low = message.Low,
                Close = message.Close,
                Volume = message.PeriodVolume
            };
        }
    }
}