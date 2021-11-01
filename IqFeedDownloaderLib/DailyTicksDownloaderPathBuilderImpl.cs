using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CustomShared;
using log4net;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public interface IDailyTicksDownloaderPathBuilder
    {
        public string GetWritePath(string symbol, LocalDate date);

        public string GetWritePath(string symbol, DownloadDateSchema dateSchema);
    }

    public class DailyTicksDownloaderPathBuilderImpl : IDailyTicksDownloaderPathBuilder
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DailyTicksDownloaderPathBuilderImpl));

        private readonly string _outputPerDateBaseDirectory;
        private readonly string _writeExt;
        private readonly List<string> _otherExts;

        public DailyTicksDownloaderPathBuilderImpl(string outputPerDateBaseDirectory, string writeExt = "csv")
        {
            _outputPerDateBaseDirectory = outputPerDateBaseDirectory;
            if (writeExt.Contains('.'))
                writeExt = writeExt.Remove('.');
            _writeExt = writeExt;
            _otherExts = new List<string> { "feather" };

            CreateDirectoryIfNonExistent(outputPerDateBaseDirectory);
        }

        private string GetWritePathHelper(string symbol, LocalDate startDate, string ext, LocalDate? endDate = null)
        {
            if (!endDate.HasValue)
            {
                var fileName = $"{symbol}.{ext}";
                return Path.GetFullPath(
                    Path.Join(_outputPerDateBaseDirectory, startDate.ToYYYYMMDD(), ext, fileName));
            }

            var filename = $"{symbol}_{startDate.ToYYYYMMDD()}-{endDate.Value.ToYYYYMMDD()}.{ext}";
            return Path.GetFullPath(
                Path.Join(GetCombinedDateCsvsDirectory(), filename));
        }

        public string GetCombinedDateCsvsDirectory()
        {
            return Path.Join(_outputPerDateBaseDirectory, "combined_dates_csvs");
        }

        public string GetWritePath(string symbol, LocalDate date)
            => GetWritePathHelper(symbol, date, _writeExt);

        public string GetWritePath(string symbol, DownloadDateSchema dateSchema)
        {
            return GetWritePathHelper(symbol, dateSchema.StartDate, _writeExt, dateSchema.EndDate);
        }

        public IEnumerable<string> GetAllPaths(string symbol, LocalDate date)
            => new List<string> { _writeExt }.Concat(_otherExts)
                .Select(ext => GetWritePathHelper(symbol, date, ext));

        static void CreateDirectoryIfNonExistent(string directory)
        {
            if (!Directory.Exists(directory))
            {
                Log.Info($"Creating new directory: {directory}");
                Directory.CreateDirectory(directory);
            }
        }
    }
}