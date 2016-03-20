namespace RavenDbMigrationToy
{
    public interface IMigration<in T>
    {
        void Migrate(T entity);
    }
}