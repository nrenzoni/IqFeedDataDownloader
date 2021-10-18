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

        public DailyOhlcRepoPg(string connectionStr, uint flushSize = 10000)
            : base(connectionStr, "daily_ohlc", TsFieldName, TsFieldName, flushSize)
        {
        }

        protected override object ConvertTimeToSerializable(LocalDate time) => time;
    }
}