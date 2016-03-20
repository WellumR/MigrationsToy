using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;

namespace RavenDbMigrationToy
{
    class Program
    {
        static void Main(string[] args)
        {
            var store = new DocumentStore {Url = "http://desktop-86hdiu8:8080/" };
            store.Initialize();

            var databaseName = "MigrationToy";

            store.DatabaseCommands.EnsureDatabaseExists(databaseName);

            var stopwatch = new Stopwatch();

            Console.WriteLine("Seeding...");
            stopwatch.Start();
            Seed(store, databaseName);
            stopwatch.Stop();
            Console.WriteLine($"Seeding took {stopwatch.Elapsed.Seconds} seconds");            

            var migration = new SillyMigration();

            var migrator = new Migrator4(store, databaseName);

            stopwatch.Reset();
            Console.WriteLine("Starting migration...");
            stopwatch.Start();
            Task.WaitAll(migrator.Migrate<SillyMigration, SillyEntity>(migration));
            stopwatch.Stop();
            Console.WriteLine($"Migration took {stopwatch.Elapsed.TotalSeconds} seconds");

            EnsureAllDocumentsMigrated(store, databaseName);

            Console.ReadLine();
        }

        private static void EnsureAllDocumentsMigrated(IDocumentStore documentStore, string databaseName)
        {
            var session = documentStore.OpenSession(databaseName);

            var count = session.Query<SillyEntity>().
                Customize(q => q.WaitForNonStaleResults()).
                Count();
            var sum = session.Query<SillyEntity>().
                Customize(q => q.WaitForNonStaleResults()).
                Count(x => x.SillyProperty == 1);
            if (count != sum)
            {
                throw new InvalidOperationException();
            }
        }

        private static void Seed(IDocumentStore documentStore, string databaseName)
        {
            using (var operation = documentStore.BulkInsert(databaseName))
            {
                for(var i = 0; i < 100000; i++)
                {
                    operation.Store(new SillyEntity { SillyProperty = 0 });
                }                
            }
        }
    }
}
