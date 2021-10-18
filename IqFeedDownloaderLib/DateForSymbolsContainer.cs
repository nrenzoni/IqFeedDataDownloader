using System.Collections.Generic;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class DateForSymbolsContainer
    {
        public Dictionary<string, SortedSet<LocalDate>> SymbolToDatesDict = new();
        
        
    }
}