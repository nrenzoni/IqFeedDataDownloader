using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using CustomShared;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class TopListSymbols
    {
        public string[] Symbols { get; }
        public LocalDate Date { get; }

        public TopListSymbols(string[] symbols, LocalDate date)
        {
            Symbols = symbols;
            Date = date;
        }

        public static string ExtractDateString(string rawString)
        {
            const string datePattern = @"[0-9]{4}-[0-9]{2}-[0-9]{2}";
            var match = Regex.Match(rawString, datePattern);
            if (!match.Success)
                throw new Exception($"No date detected in [{rawString}]");

            return match.Value;
        }

        public static TopListSymbols BuildFromFile(string filePath)
        {
            if (!File.Exists(filePath))
                throw new Exception($"File {filePath} does not exist.");

            var symbols = File.ReadAllLines(filePath);
            var filenameWithoutExt = Path.GetFileNameWithoutExtension(filePath);

            var dateString = ExtractDateString(filenameWithoutExt);

            return new TopListSymbols(symbols, dateString.ParseToLocalDate());
        }

        // directory contains txt files named by date with symbols list in each
        public static List<TopListSymbols> BuildFromDirectory(string directoryPath)
        {
            if (!Directory.Exists(directoryPath))
                throw new Exception($"Directory {directoryPath} does not exist.");

            var filesInDir = Directory.GetFiles(directoryPath);

            var topListSymbolsList =
                filesInDir.Select(BuildFromFile).ToList();

            VerifyNoDuplicateDates(topListSymbolsList);

            return topListSymbolsList;
        }

        public static void VerifyNoDuplicateDates(IEnumerable<TopListSymbols> topListSymbolsList)
        {
            var dateTimes = topListSymbolsList.Select(t => t.Date).ToArray();

            if (dateTimes.Length != dateTimes.Distinct().Count())
                throw new Exception("Duplicate dates in topListSymbolsList");
        }
    }
}