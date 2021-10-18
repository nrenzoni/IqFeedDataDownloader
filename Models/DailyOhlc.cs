using System;
using NodaTime;

namespace Models
{
    public abstract class Ohlc<TTime>
    {
        public string Symbol { get; set; }

        public TTime Ts { get; set; }

        public double Open { get; set; }

        public double High { get; set; }
        public double Low { get; set; }
        public double Close { get; set; }
        public long Volume { get; set; }
    }

    public class DailyOhlc : Ohlc<LocalDate>
    {
    }

    public abstract class IntradayOhlc : Ohlc<ZonedDateTime>
    {
    }

    public class MinuteOhlc : IntradayOhlc
    {
    }
}