using CommandLine;

namespace DownloaderMain
{
    public class CliOpts
    {
        public enum StrategyType
        {
            Breakouts,
            PremarketGainers,
            PremarketLosers,
            SectorEtf,
            RunningUp,
            IntradayGainers
        }

        public enum DataType
        {
            Tick,
            MinuteOhlc,
            DailyOhlc,
            Default
        }

        [Verb("download_symbols")]
        public class DownloadIqDataForSymbolsOpts
        {
            [Option('t', "strategy-type", Required = true)]
            public StrategyType StrategyType { get; set; }

            [Option('d', "data-type", Required = true)]
            public DataType DataType { get; set; }

            [Option('s', "starting-from", Required = true)]
            public string StartingFromDate { get; set; }

            [Option('a', "max-days-count", Required = false)]
            public uint? MaxDownloadDayCount { get; set; } = null;
        }
    }
}