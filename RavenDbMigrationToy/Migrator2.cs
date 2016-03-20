using System.Linq;
using System.Threading.Tasks;
using Raven.Client;

namespace RavenDbMigrationToy
{
    /// <summary>
    /// Standard writes, batches of 1000, no waiting
    /// </summary>
    public class Migrator2 : IMigrator
    {
        private readonly IDocumentStore _store;
        private readonly string _databaseName;

        public Migrator2(IDocumentStore store, string databaseName)
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
                    OrderBy(x => x.Id).
                    Count();
            }

            var pageSize = 1000;
            var pages = count / pageSize;

            for (var i = 0; i <= pages; i++)
            {
                using (var session = _store.OpenSession(_databaseName))
                {
                    var entities = session.Query<TEntity>().                        
                        OrderBy(x => x.Id).
                        Skip(i * pageSize).
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