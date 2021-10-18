using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CustomShared;
using log4net;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class DailyTicksDownloaderPathBuilder
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DailyTicksDownloaderPathBuilder));

        private readonly string _outputBaseDirectory;
        private readonly string _writeExt;
        private readonly List<string> _otherExts;

        public DailyTicksDownloaderPathBuilder(string outputBaseDirectory, string writeExt = "csv")
        {
            _outputBaseDirectory = outputBaseDirectory;
            if (writeExt.Contains('.'))
                writeExt = writeExt.Remove('.');
            _writeExt = writeExt;
            _otherExts = new List<string> { "feather" };

            CreateDirectoryIfNonExistent(outputBaseDirectory);
        }

        private string GetWritePath(string symbol, LocalDate date, string ext)
        {
            var fileName = $"{symbol}.{ext}";
            return Path.GetFullPath(
                Path.Join(_outputBaseDirectory, date.ToYYYYMMDD(), ext, fileName));
        }

        public string GetWritePath(string symbol, LocalDate date)
            => GetWritePath(symbol, date, _writeExt);

        public IEnumerable<string> GetAllPaths(string symbol, LocalDate date)
            => new List<string> { _writeExt }.Concat(_otherExts)
                .Select(ext => GetWritePath(symbol, date, ext));

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