using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;

namespace RavenDbMigrationToy
{
    /// <summary>
    /// Overriding Raven's requests per session, and trying to abuse the bulk insert API (incomplete)
    /// </summary>
    public class Migrator7 : IMigrator
    {
        private readonly IDocumentStore _store;
        private readonly string _databaseName;

        public Migrator7(IDocumentStore store, string databaseName)
        {
            _store = store;
            _databaseName = databaseName;
        }

        public Task Migrate<TMigration, TEntity>(TMigration migration) where TMigration : IMigration<TEntity> where TEntity : IEntity
        {
            var session = _store.OpenSession(_databaseName);
            session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

            var count = session.Query<TEntity>().
                    Customize(query => query.WaitForNonStaleResultsAsOfLastWrite()).
                    Count();

            var pageSize = 1000;
            var pages = count / pageSize;

            var allEntities = new List<TEntity>();

            for (var i = 0; i <= pages; i++)
            {
                var entities = session.Query<TEntity>().
                    Customize(
                        q => q.WaitForNonStaleResultsAsOfLastWrite().BeforeQueryExecution(x => x.PageSize = pageSize)).
                    AsQueryable().
                    OrderBy(x => x.Id).
                    Skip(i*pageSize).
                    Take(pageSize).ToArray();

                allEntities.AddRange(entities);
            }

            using (var operation = _store.BulkInsert(_databaseName))
            {
                foreach (var entity in allEntities)
                {
                    migration.Migrate(entity);
                    operation.Store(entity);
                }
            }

            return Task.FromResult(true);
        }
    }
}