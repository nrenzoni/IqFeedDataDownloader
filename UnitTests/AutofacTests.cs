using System;
using Autofac;
using IqFeedDownloaderLib;
using NUnit.Framework;

namespace UnitTests
{
    [TestFixture]
    public class AutofacTests
    {
        class MockDisposableClass : IDisposable
        {
            public static bool WasDisposed = false;

            public void Dispose()
            {
                WasDisposed = true;
            }
        }


        [Test]
        public void TestScopeDisposal()
        {
            var builder = new ContainerBuilder();
            builder.RegisterType<MockDisposableClass>()
                .SingleInstance();
            
            {
                using var built = builder.Build();
                // using var scope = builder.Build().BeginLifetimeScope();
                using var scope = built.BeginLifetimeScope();

                scope.Resolve<MockDisposableClass>();
            }
            
            Assert.IsTrue(MockDisposableClass.WasDisposed);
        }
    }
}