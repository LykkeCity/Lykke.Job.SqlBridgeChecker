using System.Threading.Tasks;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions
{
    public interface IMarketOrdersRepository : ITableEntityRepository<MarketOrderEntity>
    {
        Task<MarketOrderEntity> GetMarketOrderByIdAsync(string marketOrderId);
    }
}
