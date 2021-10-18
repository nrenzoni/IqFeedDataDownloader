using System;
using NodaTime;

namespace Models
{
    public class SymbolDatePair : IEquatable<SymbolDatePair>
    {
        public string Symbol { get; init; }

        public LocalDate Date { get; init; }

        public bool Equals(SymbolDatePair other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Symbol == other.Symbol && Date.Equals(other.Date);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SymbolDatePair)obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Symbol, Date);
        }
    }
}