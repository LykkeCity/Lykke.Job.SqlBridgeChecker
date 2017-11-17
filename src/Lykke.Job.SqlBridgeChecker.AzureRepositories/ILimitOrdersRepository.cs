using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public interface ILimitOrdersRepository : ITableEntityRepository<LimitOrderEntity>
    {
        Task<LimitOrderEntity> GetLimitOrderByIdAsync(string limitOrderId, string clientId);

        Task<List<LimitOrderEntity>> GetOrdesByMatchingIdsAsync(IEnumerable<string> matchingIds);
    }
}
