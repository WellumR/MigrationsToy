using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;

namespace RavenDbMigrationToy
{
    /// <summary>
    /// Standard writes, batches of 1000, waits for non-stale
    /// </summary>
    public class Migrator3 : IMigrator
    {
        private readonly IDocumentStore _store;
        private readonly string _databaseName;

        public Migrator3(IDocumentStore store, string databaseName)
        {
            _store = store;
            _databaseName = databaseName;
        }

        public Task Migrate<TMigration, TEntity>(TMigration migration) where TMigration : IMigration<TEntity> where TEntity : IEntity
        {
            var session = _store.OpenSession(_databaseName);
            var count = session.Query<SillyEntity>().Customize(q => q.WaitForNonStaleResultsAsOfLastWrite()).Count();

            var operation = _store.DatabaseCommands.ForDatabase(_databaseName).UpdateByIndex(
                "Raven/DocumentsByEntityName", new IndexQuery {Query = "Tag: SillyEntities"},
                new ScriptedPatchRequest {Script = "this.SillyProperty = 1"}, false);
                //new PatchRequest[] {new PatchRequest {Value = 1, Name = "SillyProperty", Type = PatchCommandType.Set}, }, false);

            operation.WaitForCompletion();

            return Task.FromResult(true);
        }
    }
}