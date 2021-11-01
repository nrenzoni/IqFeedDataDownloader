using Models;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class MinuteOhlcRepoPg
        : OhlcRepoPgBase<MinuteOhlc, ZonedDateTime>
    {
        public MinuteOhlcRepoPg(string connectionStr, string tableName, uint flushSize = 50000,
            uint maxSimultaneousSavers = 50)
            : base(connectionStr, tableName, "ts", "ts::date", flushSize, maxSimultaneousSavers)
        {
        }

        protected override object ConvertTimeToSerializable(ZonedDateTime time)
        {
            return time.ToInstant();
        }
    }
}