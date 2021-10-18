using System.Collections.Generic;
using Models;
using NodaTime;
using NUnit.Framework;

namespace UnitTests
{
    [TestFixture]
    public class SymbolDatePairTests
    {
        [Test]
        public void LocalDateEqTest()
        {
            Assert.IsTrue(
                new LocalDate(2020, 1, 1) ==
                new LocalDate(2020, 1, 1));
        }
        
        [Test]
        public void SymbolDatePairEquTest()
        {
            var pair1 = new SymbolDatePair
            {
                Symbol = "A",
                Date = new LocalDate(2020, 1, 1)
            };

            var pair2 = new SymbolDatePair
            {
                Symbol = "A",
                Date = new LocalDate(2020, 1, 1)
            };

            var set = new HashSet<SymbolDatePair>
            {
                pair1,
                pair2
            };

            Assert.IsTrue(set.Count == 1);
        }
    }
}