using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CustomShared;
using log4net;
using Models;
using NodaTime;
using Npgsql;
using NpgsqlTypes;

namespace IqFeedDownloaderLib
{
    public abstract class OhlcRepoPgBase<T, TTime>
        : IOhlcRepoCombined<T, TTime>, IDisposable
        where T : Ohlc<TTime>
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(OhlcRepoPgBase<,>));

        private readonly uint _flushSize;

        private readonly string _tableName;
        private readonly string _tsFieldName;

        private readonly string _connectionStr;

        private readonly string _dateSqlSelector;

        private readonly ConcurrentQueue<T> _ohlcQueue = new();
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _backgroundWriter;
        private bool _disposed;

        protected OhlcRepoPgBase(string connectionStr, string tableName, string tsFieldName, string dateSqlSelector,
            uint flushSize = 10000)
        {
            _connectionStr = connectionStr;
            _tableName = tableName;
            _flushSize = flushSize;
            _tsFieldName = tsFieldName;
            _dateSqlSelector = dateSqlSelector;

            _backgroundWriter = Task.Run(BackgroundSaver);
        }

        public void Save(T ohlc)
        {
            if (_disposed)
                throw new Exception("Cannot save after disposing");
            _ohlcQueue.Enqueue(ohlc);
        }


        private async Task BackgroundSaver()
        {
            while (!_cts.IsCancellationRequested || !_ohlcQueue.IsEmpty)
            {
                if (_ohlcQueue.Count < _flushSize && !_cts.IsCancellationRequested)
                {
                    await Task.Delay(100);
                    continue;
                }

                List<T> ohlcData = new();
                for (int i = 0; i < _flushSize; i++)
                {
                    var result = _ohlcQueue.TryDequeue(out var singleOhlcObj);
                    if (result == false)
                        break;

                    ohlcData.Add(singleOhlcObj);
                }

                await WriteToDbAsync(ohlcData);
            }
        }


        public async Task WriteToDbAsync(List<T> ohlcList)
        {
            Log.Info($"Writing {ohlcList.Count} OHLC data points to pg table {_tableName}...");

            await using var conn = new NpgsqlConnection(_connectionStr);
            await conn.OpenAsync();

            var insertTemplate =
                $"INSERT INTO {_tableName} (symbol, {_tsFieldName}, open, high, low, close, volume) " +
                "VALUES (:1, :2, :3, :4, :5, :6, :7)";

            await using var cmd = new NpgsqlCommand(insertTemplate, conn);
            var p1 = cmd.Parameters.Add("1", NpgsqlDbType.Varchar);
            var p2 = cmd.Parameters.Add("2", NpgsqlDbType.TimestampTz);
            var p3 = cmd.Parameters.Add("3", NpgsqlDbType.Numeric);
            var p4 = cmd.Parameters.Add("4", NpgsqlDbType.Numeric);
            var p5 = cmd.Parameters.Add("5", NpgsqlDbType.Numeric);
            var p6 = cmd.Parameters.Add("6", NpgsqlDbType.Numeric);
            var p7 = cmd.Parameters.Add("7", NpgsqlDbType.Numeric);
            await cmd.PrepareAsync();

            try
            {
                foreach (var ohlc in ohlcList)
                {
                    p1.Value = ohlc.Symbol;
                    p2.Value = ConvertTimeToSerializable(ohlc.Ts);
                    p3.Value = new Decimal(ohlc.Open);
                    p4.Value = new Decimal(ohlc.High);
                    p5.Value = new Decimal(ohlc.Low);
                    p6.Value = new Decimal(ohlc.Close);
                    p7.Value = ohlc.Volume;
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception e)
            {
                Log.Error("Caught exception", e);
            }

            Log.Info($"Finished writing.");
        }

        public async Task WriteToDbAsync1(List<T> ohlcList)
        {
            Log.Info($"Writing {ohlcList.Count} OHLC data points to pg table {_tableName}...");

            await using var conn = new NpgsqlConnection(_connectionStr);
            await conn.OpenAsync();

            await using var writer = await conn.BeginBinaryImportAsync(
                $"copy {_tableName} (symbol, {_tsFieldName}, open, high, low, close, volume) " +
                "from STDIN (FORMAT BINARY)");
            foreach (var ohlc in ohlcList)
            {
                await writer.WriteRowAsync(values: new object[]
                {
                    ohlc.Symbol,
                    ConvertTimeToSerializable(ohlc.Ts),
                    new Decimal(ohlc.Open),
                    new Decimal(ohlc.High),
                    new Decimal(ohlc.Low),
                    new Decimal(ohlc.Close),
                    ohlc.Volume
                });
            }

            try
            {
                await writer.CompleteAsync();
            }
            catch (Exception e)
            {
                Log.Error("Caught exception", e);
            }

            Log.Info($"Finished writing.");
        }

        public void Dispose()
        {
            _disposed = true;
            _cts.Cancel();
            _backgroundWriter.Wait();
        }

        protected abstract object ConvertTimeToSerializable(TTime time);

        public async Task<Dictionary<string, SymbolDateSet>> GetAlreadySavedDaysAsync(List<SymbolDatePair> toCheck)
        {
            await using var conn = new NpgsqlConnection(_connectionStr);
            await conn.OpenAsync();

            var found = new Dictionary<string, SymbolDateSet>();

            foreach (var toCheckTuplesBatch in toCheck.Batch(5000))
            {
                var inSqlRhs =
                    string.Join(", ",
                        toCheckTuplesBatch.Select(t => $"('{t.Symbol}', '{t.Date.ToYYYYMMDD()}')"));

                await using var cmd = new NpgsqlCommand(
                    $"SELECT DISTINCT symbol, {_dateSqlSelector} " +
                    $"FROM {_tableName} " +
                    $"WHERE (symbol, {_dateSqlSelector}) IN ({inSqlRhs})",
                    conn);

                await using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    var foundSymbol = reader.GetString(0);
                    var foundDate = reader.GetFieldValue<LocalDate>(1);

                    if (found.ContainsKey(foundSymbol))
                    {
                        found[foundSymbol].AddDate(foundDate);
                    }
                    else
                    {
                        var newSymbolDateSet = new SymbolDateSet(foundSymbol);
                        newSymbolDateSet.AddDate(foundDate);
                        found[foundSymbol] = newSymbolDateSet;
                    }
                }
            }

            return found;
        }
    }

    public static class PgMisc
    {
        public static void SetupTypeMapping()
        {
            NpgsqlConnection.GlobalTypeMapper.UseNodaTime();
        }
    }
}