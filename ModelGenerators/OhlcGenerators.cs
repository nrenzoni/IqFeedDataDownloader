using System;
using CustomShared;
using Models;
using NodaTime;
using NpgsqlSaveSpeedTest;

namespace ModelGenerators
{
    public static class OhlcGenerators
    {
        static readonly Random random = new();

        public static MinuteOhlc GenerateMinuteOhlc()
        {
            var (gen1, gen2) = (random.NextDouble(), random.NextDouble());
            gen1 = Math.Round(10 + gen1 * 10, 2);
            gen2 = Math.Round(10 + gen2 * 10);

            double high, low;
            if (gen1 > gen2)
            {
                high = gen1;
                low = gen2;
            }
            else
            {
                high = gen2;
                low = gen1;
            }

            double open, close;
            if (random.NextDouble() > 0.5)
            {
                open = high;
                close = low;
            }
            else
            {
                open = low;
                close = high;
            }

            var unixMillLowerBound = 946731600000;
            var unixMillUpperBound = 4102491600000;

            var randTs =
                Instant.FromUnixTimeMilliseconds(
                        random.NextLong(
                            unixMillLowerBound,
                            unixMillUpperBound))
                    .InZone(DateUtils.NyDateTz);

            return new MinuteOhlc
            {
                Symbol = "TEST",
                Ts = randTs,
                Open = open,
                High = high,
                Low = low,
                Close = close,
                Volume = random.Next(1000, 10000)
            };
        }
   
    }
}