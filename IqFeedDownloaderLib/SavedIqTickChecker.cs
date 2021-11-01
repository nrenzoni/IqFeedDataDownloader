using System.IO;
using System.Linq;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class SavedIqTickChecker
    {
        private readonly DownloadPlanUtils _downloadPlanUtils;
        private readonly DailyTicksDownloaderPathBuilderImpl _dailyTicksDownloaderPathBuilder;

        public SavedIqTickChecker(DownloadPlanUtils downloadPlanUtils,
            DailyTicksDownloaderPathBuilderImpl dailyTicksDownloaderPathBuilder)
        {
            _downloadPlanUtils = downloadPlanUtils;
            _dailyTicksDownloaderPathBuilder = dailyTicksDownloaderPathBuilder;
        }

        public void RemoveAlreadySaved(DownloadPlan downloadPlan)
        {
            foreach (var date in _downloadPlanUtils.IterateDays(downloadPlan))
                if (TickDateIsSaved(downloadPlan.Symbol, date))
                    _downloadPlanUtils.RemoveDate(downloadPlan, date);
        }

        private bool TickDateIsSaved(string symbol, LocalDate date)
        {
            var potentialSavedFiles = _dailyTicksDownloaderPathBuilder.GetAllPaths(symbol, date);
            return potentialSavedFiles.Any(File.Exists);
        }
    }
}