using System;

namespace RavenDbMigrationToy
{
    public interface IEntity
    {
        Guid Id { get; set; }
    }
}