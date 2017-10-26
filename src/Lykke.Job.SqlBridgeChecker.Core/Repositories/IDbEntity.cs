using System;

namespace Lykke.Job.SqlBridgeChecker.Core.Repositories
{
    public interface IDbEntity
    {
        object GetEntityId();
    }
}
