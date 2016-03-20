using System.Linq;
using System.Threading.Tasks;
using Raven.Client;

namespace RavenDbMigrationToy
{
    /// <summary>
    /// Standard writes, batches of 1000, waits for non-stale
    /// </summary>
    public class Migrator1 : IMigrator
    {
        private readonly IDocumentStore _store;
        private readonly string _databaseName;

        public Migrator1(IDocumentStore store, string databaseName)
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
                    Customize(query => query.WaitForNonStaleResultsAsOfLastWrite()).
                    Count();
            }

            var pageSize = 1000;
            var pages = count/pageSize;

            for (var i = 0; i <= pages; i++)
            {
                using (var session = _store.OpenSession(_databaseName))
                {
                    var entities = session.Query<TEntity>().
                        Customize(q => q.WaitForNonStaleResultsAsOfLastWrite().BeforeQueryExecution(x => x.PageSize = pageSize)).
                        AsQueryable().
                        OrderBy(x => x.Id).
                        Skip(i*pageSize).
                        Take(pageSize).ToArray();

                    foreach (var entity in entities)
                    {
                        migration.Migrate(entity);
                    }

                    session.SaveChanges();
                }
            }

            return Task.FromResult(true);
        }
    }
}