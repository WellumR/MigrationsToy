using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using Raven.Client;

namespace RavenDbMigrationToy
{
    /// <summary>
    /// Observables for parallel calls, async api, no waiting for indexes
    /// </summary>
    public class Migrator4 : IMigrator
    {
        private readonly IDocumentStore _store;
        private readonly string _databaseName;

        public Migrator4(IDocumentStore store, string databaseName)
        {
            _store = store;
            _databaseName = databaseName;
        }

        public Task Migrate<TMigration, TEntity>(TMigration migration) where TMigration : IMigration<TEntity> where TEntity : IEntity
        {
            int count;
            using (var session = _store.OpenSession(_databaseName))
            {
                count = session.Query<TEntity>().
                    Customize(query => query.WaitForNonStaleResultsAsOfLastWrite()). //This wait is needed, as the bulk insert has only just finished
                    OrderBy(x => x.Id). //This will create an index with the right ordering
                    Count();
            }

            var pageSize = 1000;
            var pages = count/pageSize;

            return Observable.Range(0, pages).Select(i => Observable.Defer(async () =>
            {
                using (var session = _store.OpenAsyncSession(_databaseName))
                {
                    var entities = await session.Query<TEntity>().
                        OrderBy(x => x.Id).
                        Skip(i*pageSize).
                        Take(pageSize).ToListAsync();

                    foreach (var entity in entities)
                    {
                        migration.Migrate(entity);
                    }

                    await session.SaveChangesAsync();
                }
                return Observable.Return(true);
            })).Merge().ToTask();
        }
    }
}