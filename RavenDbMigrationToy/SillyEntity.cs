using System;

namespace RavenDbMigrationToy
{
    public class SillyEntity : IEntity
    {
        public Guid Id { get; set; }
        public int SillyProperty { get; set; }
    }
}