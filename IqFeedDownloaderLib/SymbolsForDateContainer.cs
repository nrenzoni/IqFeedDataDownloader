using System.Collections.Generic;
using System.Linq;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class SymbolsForDateContainer
    {
        public Dictionary<LocalDate, HashSet<string>> DateToSymbolsToDownloadMap { get; } = new();

        public void AddSymbolToDateToDownload(LocalDate date, string symbol)
        {
            if (!DateToSymbolsToDownloadMap.ContainsKey(date))
                DateToSymbolsToDownloadMap[date] = new();

            DateToSymbolsToDownloadMap[date].Add(symbol);
        }

        public void AddSymbolsToDateToDownload(LocalDate date, IEnumerable<string> symbols)
        {
            foreach (var symbol in symbols)
                AddSymbolToDateToDownload(date, symbol);
        }

        public HashSet<string> GetSymbolsForDate(LocalDate date)
            => !DateToSymbolsToDownloadMap.ContainsKey(date) ? null : DateToSymbolsToDownloadMap[date];

        public List<LocalDate> GetDates()
            => DateToSymbolsToDownloadMap.Keys.ToList();

        public IEnumerable<KeyValuePair<string, LocalDate>> GetSymbolsDates()
        {
            foreach (var date in GetDates())
            {
                foreach (var symbol in GetSymbolsForDate(date))
                {
                    yield return new KeyValuePair<string, LocalDate>(symbol, date);
                }
            }
        }

        public static SymbolsForDateContainer FromSymbolListPerDay(List<SymbolListPerDay> symbolListsPerDay)
        {
            var symbolsForDateContainer = new SymbolsForDateContainer();
            foreach (var symbolListPerDay in symbolListsPerDay)
            {
                symbolsForDateContainer.AddSymbolsToDateToDownload(symbolListPerDay.Date, symbolListPerDay.Symbols);
            }

            return symbolsForDateContainer;
        }
    }
}