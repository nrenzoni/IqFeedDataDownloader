using System;
using System.Collections.Generic;
using System.Linq;
using CustomShared;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public interface ISymbolsPerDateCollection
    {
        public Dictionary<LocalDate, HashSet<string>> SymbolsPerDate { get; }

        public IEnumerable<Tuple<string, LocalDate>> IterateSymbolWithDate { get; }
    }

    public class PreAndPostDaysSymbolsPerDateCollection : ISymbolsPerDateCollection
    {
        private readonly uint _previousDayDownloadCount;
        private readonly uint _postDayDownloadCount;
        private readonly LocalDate _minDate;
        private readonly uint? _limitCountDays;

        private readonly SymbolsForDateContainer _symbolsForDateContainer;
        private readonly MarketDayChecker _marketDayChecker;

        public PreAndPostDaysSymbolsPerDateCollection(
            SymbolsForDateContainer symbolsForDateContainer,
            MarketDayChecker marketDayChecker,
            uint previousDayDownloadCount,
            uint postDayDownloadCount,
            LocalDate minDate,
            uint? limitCountDays)
        {
            _symbolsForDateContainer = symbolsForDateContainer;
            _marketDayChecker = marketDayChecker;
            _previousDayDownloadCount = previousDayDownloadCount;
            _postDayDownloadCount = postDayDownloadCount;
            _minDate = minDate;
            _limitCountDays = limitCountDays;

            SymbolsPerDate = GetSymbolsPerDates();
        }

        private Dictionary<LocalDate, HashSet<string>> GetSymbolsPerDates()
        {
            Dictionary<LocalDate, HashSet<string>> symbolsPerDates = new();

            bool maxLimitDaysHit = false;

            foreach (var (date, symbols) in _symbolsForDateContainer.DateToSymbolsToDownloadMap)
            {
                foreach (var symbol in symbols)
                {
                    /*if (symbolsPerDates.Count >= _limitCountDays)
                    {
                        maxLimitDaysHit = true;
                        break;
                    }*/

                    var openDaysInRange = _marketDayChecker.GetMarketOpenDaysInRange(
                        _marketDayChecker.GetNextOpenDay(date,
                            (int)(-1 * _previousDayDownloadCount)),
                        _marketDayChecker.GetNextOpenDay(date, (int)_postDayDownloadCount));

                    // filter above _minDate
                    openDaysInRange = openDaysInRange.Where(day => day >= _minDate).ToList();

                    foreach (var openDay in openDaysInRange)
                    {
                        var perDayListExists =
                            symbolsPerDates.TryGetValue(openDay, out var symbolsForDateList);
                        if (!perDayListExists)
                        {
                            symbolsForDateList = new HashSet<string>();
                            symbolsPerDates[openDay] = symbolsForDateList;
                        }

                        symbolsForDateList.Add(symbol);
                    }
                }

                if (maxLimitDaysHit)
                    break;
            }

            var dates = symbolsPerDates.Keys.ToList();
            dates.Sort();
            var datesToTake =
                _limitCountDays == null
                    ? dates
                    : dates.Take((int)_limitCountDays);

            return symbolsPerDates.Where(kv => datesToTake.Contains(kv.Key))
                .ToDictionary(kv => kv.Key, kv => kv.Value);
            // return symbolsPerDates;
        }

        public Dictionary<LocalDate, HashSet<string>> SymbolsPerDate { get; }

        public IEnumerable<Tuple<string, LocalDate>> IterateSymbolWithDate
        {
            get
            {
                foreach (var (date, symbols) in SymbolsPerDate)
                {
                    foreach (var symbol in symbols)
                    {
                        yield return new Tuple<string, LocalDate>(symbol, date);
                    }
                }
            }
        }
    }
}