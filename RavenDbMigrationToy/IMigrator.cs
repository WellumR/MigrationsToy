using System.Threading.Tasks;

namespace RavenDbMigrationToy
{
    public interface IMigrator
    {
        Task Migrate<TMigration,TEntity>(TMigration migration) where TMigration : IMigration<TEntity> where TEntity : IEntity;
    }
}