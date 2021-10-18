using System;
using System.Collections.Generic;
using System.Linq;
using CustomShared;
using Models;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class DownloadPlan
    {
        public string Symbol { get; set; }
        public List<DownloadDateSchema> DownloadDateSchemata { get; set; }

        public string AsDatesString =>
            string.Join(", ", DownloadDateSchemata.Select(dds => dds.ToString()));
    }

    public class DownloadPlanUtils
    {
        private readonly MarketDayChecker _marketDayChecker;

        public DownloadPlanUtils(MarketDayChecker marketDayChecker)
        {
            _marketDayChecker = marketDayChecker;
        }

        public IEnumerable<LocalDate> AllDates(DownloadPlan downloadPlan)
        {
            foreach (var downloadDateSchema in downloadPlan.DownloadDateSchemata)
            {
                foreach (var date in _marketDayChecker.GetMarketOpenDaysInRange(
                    downloadDateSchema.StartDate,
                    downloadDateSchema.EndDate))
                {
                    yield return date;
                }
            }
        }

        public IEnumerable<SymbolDatePair> GetSymbolDateTuples(DownloadPlan downloadPlan)
        {
            foreach (var date in AllDates(downloadPlan))
            {
                yield return new SymbolDatePair
                {
                    Symbol = downloadPlan.Symbol,
                    Date = date
                };
            }
        }

        public List<SymbolDateSet> ToMarketDaySymbolDateSetList(List<DownloadPlan> downloadPlansToCheck)
        {
            Dictionary<string, SymbolDateSet> symbolDateSetList = new();

            foreach (var downloadPlanToCheck in downloadPlansToCheck)
            {
                foreach (var downloadDateSchema in downloadPlanToCheck.DownloadDateSchemata)
                {
                    var currMarketDays =
                        _marketDayChecker.GetMarketOpenDaysInRange(
                            downloadDateSchema.StartDate,
                            downloadDateSchema.EndDate);

                    if (currMarketDays.Count == 0)
                        continue;

                    SymbolDateSet symbolDateSetToUpdate;
                    if (!symbolDateSetList.ContainsKey(downloadPlanToCheck.Symbol))
                    {
                        symbolDateSetToUpdate = new SymbolDateSet(downloadPlanToCheck.Symbol);
                        symbolDateSetList[downloadPlanToCheck.Symbol] = symbolDateSetToUpdate;
                    }
                    else
                        symbolDateSetToUpdate = symbolDateSetList[downloadPlanToCheck.Symbol];

                    symbolDateSetToUpdate.Dates.UnionWith(currMarketDays);
                }
            }

            return symbolDateSetList.Values.ToList();
        }

        public List<DownloadDateSchema> BuildDownloadDateSchemataContiguousDates(SortedSet<LocalDate> dates)
        {
            List<DownloadDateSchema> downloadDateSchemata = new();

            LocalDate currentMinDate = dates.First();
            LocalDate currentEndDate = currentMinDate;
            foreach (var date in dates.Skip(1))
            {
                if (_marketDayChecker.GetNextOpenDay(currentEndDate) != date)
                {
                    downloadDateSchemata.Add(
                        new(currentMinDate, currentEndDate));

                    currentMinDate = date;
                }

                currentEndDate = date;
            }

            if (downloadDateSchemata.Count == 0 || !downloadDateSchemata.Last().ContainsDate(currentEndDate))
            {
                downloadDateSchemata.Add(
                    new(currentMinDate, currentEndDate));
            }

            return downloadDateSchemata;
        }
    }
}