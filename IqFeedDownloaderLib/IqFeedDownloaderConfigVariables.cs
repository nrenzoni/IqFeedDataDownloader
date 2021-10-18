using CustomShared;

namespace IqFeedDownloaderLib
{
    public class IqFeedDownloaderConfigVariables : ConfigVariables
    {
        private static readonly IqFeedDownloaderConfigVariables _prodInstance =
            LoadFromEnv<IqFeedDownloaderConfigVariables>();

        private static readonly IqFeedDownloaderConfigVariables _testInstance =
            MakeTestConfigVariables<IqFeedDownloaderConfigVariables>();

        public static IqFeedDownloaderConfigVariables Instance =>
            IsTestGlobalChecker.IsTest ? _testInstance : _prodInstance;

        public static ConfigVariables TestInstance => _testInstance;


        public string topListSymbolOutputDir { get; set; }

        public string IqfeedTickDataBaseDirectory { get; set; }

        public string TiSymbolsPerDayServiceHostAddress { get; set; }

        public string PostgresConnectionStr { get; set; }
    }
}