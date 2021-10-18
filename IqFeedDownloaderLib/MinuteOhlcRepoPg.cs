using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CustomShared;
using Models;
using NodaTime;
using Npgsql;

namespace IqFeedDownloaderLib
{
    public class MinuteOhlcRepoPg
        : OhlcRepoPgBase<MinuteOhlc, ZonedDateTime>
    {
        public MinuteOhlcRepoPg(string connectionStr, uint flushSize = 50000)
            : base(connectionStr, "minute_ohlc", "ts", "ts::date", flushSize)
        {
        }

        protected override object ConvertTimeToSerializable(ZonedDateTime time)
        {
            return time.ToInstant();
        }
    }
}