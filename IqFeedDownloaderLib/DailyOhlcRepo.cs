using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using log4net;
using Models;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public interface IOhlcQuerier
    {
        public Task<Dictionary<string, SymbolDateSet>> GetAlreadySavedDaysAsync(List<SymbolDatePair> toCheck);

        public Task<SortedSet<LocalDate>> GetSavedDatesAsync(DownloadPlan downloadPlan);
    }

    public interface IOhlcRepoCombined<TOhlc, TTime>
        : IOhlcQuerier, IOhlcRepoSaver<TOhlc, TTime>
        where TOhlc : Ohlc<TTime>
    {
    }

    public class DailyOhlcRepoPg
        : OhlcRepoPgBase<DailyOhlc, LocalDate>
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DailyOhlcRepoPg));

        private static string TsFieldName = "date";

        public DailyOhlcRepoPg(string connectionStr, string tableName, uint flushSize = 50000, uint maxSimultaneousSavers = 50)
            : base(connectionStr, tableName, TsFieldName, TsFieldName,
                flushSize, maxSimultaneousSavers)
        {
        }

        protected override object ConvertTimeToSerializable(LocalDate time) => time;
    }
}