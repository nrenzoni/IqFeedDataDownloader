using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
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

        public SymbolDateSet ToMarketDaySymbolDateSet(DownloadPlan downloadPlan)
        {
            SymbolDateSet symbolDateSet = new(downloadPlan.Symbol);

            foreach (var downloadDateSchema in downloadPlan.DownloadDateSchemata)
            {
                var currMarketDays =
                    _marketDayChecker.GetMarketOpenDaysInRange(
                        downloadDateSchema.StartDate,
                        downloadDateSchema.EndDate);

                if (currMarketDays.Count == 0)
                    continue;

                symbolDateSet.Dates.UnionWith(currMarketDays);
            }

            return symbolDateSet;
        }

        public List<SymbolDateSet> ToMarketDaySymbolDateSetList(List<DownloadPlan> downloadPlans)
        {
            Dictionary<string, SymbolDateSet> symbolDateSetList = new();

            foreach (var downloadPlanToCheck in downloadPlans)
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

        public List<DownloadDateSchema> BuildDownloadDateSchemataContiguousDates(ImmutableSortedSet<LocalDate> dates)
        {
            if (dates.Count == 0)
                return null;

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

        public List<LocalDate> IterateDays(DownloadPlan downloadPlan, bool marketDayOffset = true)
        {
            Func<LocalDate, LocalDate, IEnumerable<LocalDate>> getDaysInRangeFunc;
            if (marketDayOffset)
                getDaysInRangeFunc
                    = (startDate, endDate) => _marketDayChecker.GetMarketOpenDaysInRange(
                        startDate, endDate);
            else
                getDaysInRangeFunc
                    = DateUtils.RangeOfDates;

            return downloadPlan.DownloadDateSchemata
                .SelectMany(dateSchema => getDaysInRangeFunc(
                    dateSchema.StartDate, dateSchema.EndDate))
                .ToList();
        }

        public void RemoveDate(DownloadPlan downloadPlan, LocalDate date, bool marketDayOffset = true)
        {
            var schemaContainingDate
                = downloadPlan.DownloadDateSchemata.Find(schema => schema.ContainsDate(date));

            if (schemaContainingDate == null)
                throw new Exception($"downloadPlan does not contain {date.ToYYYYMMDD()}");

            var offsetFunc = GetDayOffsetFunc(marketDayOffset);

            if (schemaContainingDate.IsSingleDay && schemaContainingDate.StartDate == date)
                downloadPlan.DownloadDateSchemata.Remove(schemaContainingDate);
            else if (schemaContainingDate.StartDate == date)
                schemaContainingDate.StartDate = offsetFunc(date, 1);
            else if (schemaContainingDate.EndDate == date)
                schemaContainingDate.EndDate = offsetFunc(date, -1);
            else
            {
                var firstHalf = new DownloadDateSchema(schemaContainingDate.StartDate, offsetFunc(date, -1));
                var secondHalf = new DownloadDateSchema(offsetFunc(date, 1), schemaContainingDate.EndDate);
                downloadPlan.DownloadDateSchemata.Remove(schemaContainingDate);
                downloadPlan.DownloadDateSchemata.AddRange(
                    new List<DownloadDateSchema> { firstHalf, secondHalf });
            }
        }

        private Func<LocalDate, int, LocalDate> GetDayOffsetFunc(bool marketDayOffset = true)
        {
            if (marketDayOffset)
                return (d, i) => _marketDayChecker.GetNextOpenDay(d, Math.Sign(i));
            else
                return (d, i) => d + Period.FromDays(Math.Sign(i));
        }
    }
}