using System.Collections.Generic;

namespace IqFeedDownloaderLib
{
    public static class TopListSymbolsToContainerConverter
    {
        public static SymbolsForDateContainer Convert(List<TopListSymbols> topListSymbolsList)
        {
            SymbolsForDateContainer symbolsForDateContainer = new();
            foreach (var topListSymbols in topListSymbolsList)
                symbolsForDateContainer.AddSymbolsToDateToDownload(topListSymbols.Date, topListSymbols.Symbols);

            return symbolsForDateContainer;
        }
    }
}