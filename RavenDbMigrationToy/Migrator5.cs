using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace RavenDbMigrationToy
{
    /// <summary>
    /// Playing with streaming api (incomplete)
    /// </summary>
    public class Migrator5 : IMigrator
    {
        private readonly IDocumentStore _store;
        private readonly string _databaseName;

        public Migrator5(IDocumentStore store, string databaseName)
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
                    Customize(query => query.WaitForNonStaleResultsAsOfLastWrite()). //This is here because the streaming API will only use existing indexes, not create its own dynamically
                    Count();
            }

            QueryHeaderInformation info;
            var documents = _store.DatabaseCommands.ForDatabase(_databaseName).StreamQuery("Raven/DocumentsByEntityName", new IndexQuery {Query = "Tag: SillyEntities"}, out info);

            while (documents.MoveNext())
            {
                var documentConvention = new DocumentConvention();
                var entity = (TEntity)(documentConvention.CreateSerializer().Deserialize(new RavenJTokenReader(documents.Current), typeof(TEntity)));
                migration.Migrate(entity);
            }
            
            return Task.FromResult(true);
        }
    }
}