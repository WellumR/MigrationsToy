namespace RavenDbMigrationToy
{
    public class SillyMigration : IMigration<SillyEntity>
    {
        public void Migrate(SillyEntity entity)
        {
            entity.SillyProperty = 1;
        }
    }
}