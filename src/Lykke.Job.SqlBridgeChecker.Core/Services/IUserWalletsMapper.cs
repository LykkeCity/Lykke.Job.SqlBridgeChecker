using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.SqlBridgeChecker.Core.Services
{
    public interface IUserWalletsMapper
    {
        Task AddWalletsAsync(HashSet<string> walletIds);

        void ClearCaches();
    }
}
