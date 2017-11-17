using System.Threading.Tasks;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public interface IMarketOrdersRepository : ITableEntityRepository<MarketOrderEntity>
    {
        Task<MarketOrderEntity> GetMarketOrderByIdAsync(string marketOrderId);
    }
}
