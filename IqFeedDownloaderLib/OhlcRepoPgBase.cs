using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    public abstract class OhlcRepoPgBase<TOhlc, TTime>
        : IOhlcRepoCombined<TOhlc, TTime>, IDisposable
        where TOhlc : Ohlc<TTime>
    {
        private static readonly ILog Log = LogManager.GetLogger(
            typeof(OhlcRepoPgBase<TOhlc, TTime>).GetRealTypeName());

        private readonly uint _flushSize;
        private readonly uint _maxSimultaneousSavers;
        private readonly string _tableName;
        private readonly string _tsFieldName;
        private readonly string _dateSqlSelector;
        private readonly string _connectionStr;

        private readonly ConcurrentQueue<TOhlc> _ohlcQueue = new();
        private readonly CancellationTokenSource _cts = new();

        private readonly Task _backgroundWriter;
        private readonly Task _backgroundFinishedSaverTaskRemover;

        // task by id
        private readonly Dictionary<int, Task> _saverTasks = new();
        private uint _currentLastSaverId = 0;
        private bool _disposed;

        protected OhlcRepoPgBase(
            string connectionStr,
            string tableName,
            string tsFieldName,
            string dateSqlSelector,
            uint flushSize = 50000,
            uint maxSimultaneousSavers = 50)
        {
            _connectionStr = connectionStr;
            _tableName = tableName;
            _tsFieldName = tsFieldName;
            _dateSqlSelector = dateSqlSelector;
            _flushSize = flushSize;
            _maxSimultaneousSavers = maxSimultaneousSavers;

            _backgroundWriter = Task.Run(BackgroundSaver);
            _backgroundFinishedSaverTaskRemover = Task.Run(FinishedSaverTaskRemover);
        }

        public void Save(TOhlc ohlc)
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

                if (_saverTasks.Count >= _maxSimultaneousSavers)
                {
                    // Log.Warn($"Max savers in progress, sleeping...");
                    Thread.Sleep(100);
                    continue;
                }

                List<TOhlc> ohlcData = new();
                for (int i = 0; i < _flushSize; i++)
                {
                    var result = _ohlcQueue.TryDequeue(out var singleOhlcObj);
                    if (result == false)
                        break;

                    ohlcData.Add(singleOhlcObj);
                }

                var task = Task.Run(async () => await WriteToDbAsync(ohlcData, _currentLastSaverId++));
                if (_saverTasks.ContainsKey(task.Id))
                    throw new Exception($"Tasks dictionary already contains task id {task.Id}");

                _saverTasks[task.Id] = task;
            }
        }

        private void FinishedSaverTaskRemover()
        {
            while (!_backgroundWriter.IsCompleted || _saverTasks.Count > 0)
            {
                if (_saverTasks.Count == 0)
                {
                    Thread.Sleep(100);
                    continue;
                }

                var finishedTaskId = TasksRunner.PopFinishedTaskAndRecordErrors(_saverTasks.Values.ToList());
                if (finishedTaskId.HasValue)
                {
                    _saverTasks.Remove(finishedTaskId.Value);
                }
            }
        }

        public async Task WriteToDbAsync2(List<TOhlc> ohlcList, uint saverId)
        {
            Log.Info($"[{saverId}]: Writing {ohlcList.Count} OHLC data points to pg table {_tableName}...");

            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();


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

            watch.Stop();
            var timePerRow = watch.Elapsed / ohlcList.Count;

            Log.Info($"[{saverId}]: Finished writing {ohlcList.Count} rows " +
                     $"in {watch.Elapsed}. {timePerRow} per row.");
        }

        public async Task WriteToDbAsync(List<TOhlc> ohlcList, uint saverId)
        {
            Log.Info($"[{saverId}]: Writing {ohlcList.Count} OHLC data points to pg table {_tableName}...");

            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();


            await using var conn = new NpgsqlConnection(_connectionStr);
            await conn.OpenAsync();

            uint batchSize = 9200; // number of parameters must be below 65535 (7*9200= 64,400)

            var template = BuildMultiInsertTemplate(batchSize);
            var cmdForFullBatch = new NpgsqlCommand(template, conn);
            var pgParams = BuildPgCmd(cmdForFullBatch, batchSize);
            await cmdForFullBatch.PrepareAsync();

            var cmd = cmdForFullBatch;
            foreach (var ohlcListBatch in ohlcList.Batch(batchSize))
            {
                // last batch
                if (ohlcListBatch.Count < batchSize)
                {
                    var template1 = BuildMultiInsertTemplate((uint)ohlcListBatch.Count);
                    cmd = new NpgsqlCommand(template1, conn);
                    pgParams = BuildPgCmd(cmd, (uint)ohlcListBatch.Count);
                }

                AttachPgParams(pgParams, ohlcListBatch);

                await cmd.ExecuteNonQueryAsync();
            }

            watch.Stop();
            var timePerRow = watch.Elapsed / ohlcList.Count;

            Log.Info($"[{saverId}]: Finished writing {ohlcList.Count} rows " +
                     $"in {watch.Elapsed}. {timePerRow} per row.");
        }

        public string BuildMultiInsertTemplate(uint ohlcListCount)
        {
            var insertTemplate =
                $"INSERT INTO {_tableName} (symbol, {_tsFieldName}, open, high, low, close, volume) " +
                "VALUES ";

            var insertValueTemplateArr = new List<string>();
            for (int i = 0; i < ohlcListCount; i++)
            {
                insertValueTemplateArr.Add(
                    $"(:{i * 7 + 1}, " +
                    $":{i * 7 + 2}, " +
                    $":{i * 7 + 3}, " +
                    $":{i * 7 + 4}, " +
                    $":{i * 7 + 5}, " +
                    $":{i * 7 + 6}, " +
                    $":{i * 7 + 7})");
            }

            return insertTemplate + string.Join(", ", insertValueTemplateArr);
        }

        public List<NpgsqlParameter> BuildPgCmd(NpgsqlCommand cmd, uint ohlcListCount)
        {
            var pgParams = new List<NpgsqlParameter>();

            for (int i = 0; i < ohlcListCount; i++)
            {
                pgParams.AddRange(
                    new List<NpgsqlParameter>
                    {
                        cmd.Parameters.Add($"{i * 7 + 1}", NpgsqlDbType.Varchar),
                        cmd.Parameters.Add($"{i * 7 + 2}", NpgsqlDbType.TimestampTz),
                        cmd.Parameters.Add($"{i * 7 + 3}", NpgsqlDbType.Numeric),
                        cmd.Parameters.Add($"{i * 7 + 4}", NpgsqlDbType.Numeric),
                        cmd.Parameters.Add($"{i * 7 + 5}", NpgsqlDbType.Numeric),
                        cmd.Parameters.Add($"{i * 7 + 6}", NpgsqlDbType.Numeric),
                        cmd.Parameters.Add($"{i * 7 + 7}", NpgsqlDbType.Numeric)
                    });
            }

            return pgParams;
        }

        public void AttachPgParams(List<NpgsqlParameter> pgParams, IList<TOhlc> ohlcList)
        {
            if (pgParams.Count != ohlcList.Count * 7)
                throw new Exception($"{nameof(pgParams)} count must match all params for {nameof(ohlcList)}.");

            foreach (var (ohlc, i) in ohlcList.WithIndex())
            {
                pgParams[i * 7 + 0].Value = ohlc.Symbol;
                pgParams[i * 7 + 1].Value = ConvertTimeToSerializable(ohlc.Ts);
                pgParams[i * 7 + 2].Value = new Decimal(ohlc.Open);
                pgParams[i * 7 + 3].Value = new Decimal(ohlc.High);
                pgParams[i * 7 + 4].Value = new Decimal(ohlc.Low);
                pgParams[i * 7 + 5].Value = new Decimal(ohlc.Close);
                pgParams[i * 7 + 6].Value = ohlc.Volume;
            }
        }

        public void Dispose()
        {
            _disposed = true;
            _cts.Cancel();
            _backgroundFinishedSaverTaskRemover.Wait();
        }

        protected abstract object ConvertTimeToSerializable(TTime time);

        public async Task<Dictionary<string, SymbolDateSet>> GetAlreadySavedDaysAsync(List<SymbolDatePair> toCheck)
        {
            await using var conn = new NpgsqlConnection(_connectionStr);
            await conn.OpenAsync();

            var found = new Dictionary<string, SymbolDateSet>();

            foreach (var toCheckTuplesBatch in toCheck.Batch(100))
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

        public async Task<SortedSet<LocalDate>> GetSavedDatesAsync(DownloadPlan downloadPlan)
        {
            await using var conn = new NpgsqlConnection(_connectionStr);
            await conn.OpenAsync();

            SortedSet<LocalDate> found = new();
            var schemaWhereList = new List<string>();

            foreach (var schema in downloadPlan.DownloadDateSchemata)
            {
                if (schema.IsSingleDay)
                    schemaWhereList.Add($"{_dateSqlSelector} = '{schema.StartDate.ToYYYYMMDD()}'");
                else
                    schemaWhereList.Add($"{_dateSqlSelector} >= '{schema.StartDate.ToYYYYMMDD()}' " +
                                        $"AND {_dateSqlSelector} <= '{schema.EndDate.ToYYYYMMDD()}'");
            }

            var whereSqlLeftHandSide = $"symbol = '{downloadPlan.Symbol}' AND ("
                                       + string.Join(" OR ", schemaWhereList) + ")";

            await using var cmd = new NpgsqlCommand(
                $"SELECT DISTINCT {_dateSqlSelector} " +
                $"FROM {_tableName} " +
                $"WHERE {whereSqlLeftHandSide}",
                conn);

            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var foundDate = reader.GetFieldValue<LocalDate>(0);
                found.Add(foundDate);
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