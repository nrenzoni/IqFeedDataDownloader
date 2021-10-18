using System.Collections.Generic;
using System.Linq;
using NodaTime;

namespace Models
{
    public class SymbolDateSet
    {
        public SymbolDateSet(string symbol)
        {
            Symbol = symbol;
        }

        public string Symbol { get; }

        public SortedSet<LocalDate> Dates { get; } = new();

        public void AddDate(LocalDate date)
        {
            Dates.Add(date);
        }

        public List<SymbolDatePair> AsSymbolDatePairList
        {
            get
            {
                return Dates.Select(d => new SymbolDatePair
                {
                    Symbol = Symbol,
                    Date = d
                }).ToList();
            }
        }
    }
}