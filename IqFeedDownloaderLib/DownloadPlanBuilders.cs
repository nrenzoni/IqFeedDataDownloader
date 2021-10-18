using System;
using System.Collections.Generic;
using System.Linq;
using CustomShared;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public interface IDownloadPlanBuilder
    {
        public List<DownloadPlan> GetPlans(SymbolsForDateContainer symbolsForDateContainer);
    }

    public class DayPeriodDownloadPlanBuilder : IDownloadPlanBuilder
    {
        private readonly MarketDayChecker _marketDayChecker;
        private readonly uint _previousDayPeriod;
        private readonly uint _postDayPeriod;

        public DayPeriodDownloadPlanBuilder(MarketDayChecker marketDayChecker, uint previousDayPeriod, uint postDayPeriod)
        {
            _marketDayChecker = marketDayChecker;
            _previousDayPeriod = previousDayPeriod;
            _postDayPeriod = postDayPeriod;
        }

        protected void BuildSavePlan(
            string symbol,
            LocalDate date,
            Dictionary<string, List<DownloadDateSchema>> symbolToDownloadDateSchema)
        {
            if (_previousDayPeriod == 0 && _postDayPeriod == 0 && !_marketDayChecker.IsOpen(date))
                throw new Exception($"{nameof(_previousDayPeriod)} and {nameof(_postDayPeriod)} are both 0, " +
                                    $"and market is closed on date {date.ToYYYYMMDD()}.");

            var currentDateBegin =
                _previousDayPeriod == 0
                    ? date
                    : _marketDayChecker.GetNextOpenDay(date, (int)(-1 * _previousDayPeriod));

            var currentDateEnd =
                _postDayPeriod == 0
                    ? date
                    : _marketDayChecker.GetNextOpenDay(date, (int)_postDayPeriod);

            if (symbolToDownloadDateSchema.ContainsKey(symbol))
            {
                var downloadDateSchemata = symbolToDownloadDateSchema[symbol];

                var isContained = false;
                foreach (var downloadDateSchema in downloadDateSchemata)
                {
                    if (downloadDateSchema.ContainsDate(currentDateEnd))
                    {
                        if (currentDateBegin < downloadDateSchema.StartDate)
                            downloadDateSchema.StartDate = currentDateBegin;
                        isContained = true;
                    }

                    if (downloadDateSchema.ContainsDate(currentDateBegin))
                    {
                        if (currentDateEnd > downloadDateSchema.EndDate)
                            downloadDateSchema.EndDate = date;
                        isContained = true;
                    }
                }

                if (!isContained)
                {
                    BuildAndAddDownloadSchema(currentDateBegin, currentDateEnd, symbolToDownloadDateSchema[symbol]);
                }
            }
            else
            {
                var downloadDateSchemata = new List<DownloadDateSchema>();
                symbolToDownloadDateSchema[symbol] = downloadDateSchemata;
                BuildAndAddDownloadSchema(currentDateBegin, currentDateEnd, downloadDateSchemata);
            }
        }

        private void BuildAndAddDownloadSchema(
            LocalDate currentDateBegin,
            LocalDate date,
            List<DownloadDateSchema> listToAddTo)
        {
            var downloadDateSchema = new DownloadDateSchema(currentDateBegin, date);
            listToAddTo.Add(downloadDateSchema);
        }

        public List<DownloadPlan> GetPlans(SymbolsForDateContainer symbolsForDateContainer)
        {
            var symbolToDownloadDateSchema = new Dictionary<string, List<DownloadDateSchema>>();

            foreach (var (date, symbols) in symbolsForDateContainer.DateToSymbolsToDownloadMap)
            {
                foreach (var symbol in symbols)
                {
                    BuildSavePlan(symbol, date, symbolToDownloadDateSchema);
                }
            }

            return symbolToDownloadDateSchema.Select(sds =>
            {
                var (symbol, downloadDateSchemata) = sds;
                return new DownloadPlan
                {
                    Symbol = symbol,
                    DownloadDateSchemata = downloadDateSchemata
                };
            }).ToList();
        }
    }

    public class TickDownloadPlanBuilder : DayPeriodDownloadPlanBuilder
    {
        public TickDownloadPlanBuilder(MarketDayChecker marketDayChecker,
            uint previousDayPeriod = 5,
            uint postDayPeriod = 5)
            : base(marketDayChecker, previousDayPeriod, postDayPeriod)
        {
        }
    }

    public class DailyOhlcDownloadPlanBuilder : DayPeriodDownloadPlanBuilder
    {
        public DailyOhlcDownloadPlanBuilder(
            MarketDayChecker marketDayChecker,
            uint previousDayPeriod = 252)
            : base(marketDayChecker, previousDayPeriod, 0)
        {
        }
    }

    public class MinuteOhlcDownloadPlanBuilder : DayPeriodDownloadPlanBuilder
    {
        public MinuteOhlcDownloadPlanBuilder(
            MarketDayChecker marketDayChecker, uint previousDayPeriod, uint postDayPeriod) 
            : base(marketDayChecker, previousDayPeriod, postDayPeriod)
        {
        }
    }
}