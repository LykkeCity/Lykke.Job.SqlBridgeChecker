using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions
{
    public interface ITradesRepository : ITableEntityRepository<ClientTradeEntity>
    {
        Task<string> GetOtherClientAsync(string multisig);

        Task<List<ClientTradeEntity>> GetTradesByMarketOrdersAsync(IEnumerable<string> marketOrderIds);

        Task<List<ClientTradeEntity>> GetTradesByLimitOrderKeysAsync(IEnumerable<string> limitOrderIds);

        Task<List<ClientTradeEntity>> GetTradesByLimitOrderIdsAsync(IEnumerable<string> limitOrderIds);
    }
}
