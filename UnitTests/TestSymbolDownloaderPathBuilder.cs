using System;
using System.IO;
using System.Linq;
using IqFeedDownloaderLib;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    public class TestSymbolDownloaderPathBuilder
    {
        public string GetTemporaryDirectory()
        {
            string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            return tempDirectory;
        }

        [Test]
        public void SymbolDownloaderPathBuilderTest()
        {
            var tempDir = GetTemporaryDirectory();
            var symbolDownloaderPathBuilder = new DailyTicksDownloaderPathBuilder(tempDir);
            var mockSymbol = "TEST";
            var mockDate = new LocalDate(2021, 1, 2);

            var builtPath = symbolDownloaderPathBuilder.GetWritePath(mockSymbol, mockDate);

            ;
            Assert.AreEqual(
                Path.Combine(tempDir, "2021-01-02/csv/ABC.csv"),
                builtPath);
        }

        [Test]
        public void TestGetAllPaths()
        {
            var tempDir = GetTemporaryDirectory();
            var symbolDownloaderPathBuilder = new DailyTicksDownloaderPathBuilder(tempDir);
            var mockSymbol = "TEST";
            var mockDate = new LocalDate(2021, 1, 2);

            var allPaths = symbolDownloaderPathBuilder.GetAllPaths(mockSymbol, mockDate);

            Assert.IsTrue(allPaths.Contains(
                Path.Combine(tempDir, "/2021-01-02/feather/TEST.feather")));
        }
    }
}