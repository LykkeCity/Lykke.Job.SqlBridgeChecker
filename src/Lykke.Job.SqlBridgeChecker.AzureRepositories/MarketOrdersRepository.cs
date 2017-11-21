using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class MarketOrdersRepository : ItemsRepositoryBase<MarketOrderEntity>, IMarketOrdersRepository
    {
        private readonly string _orderKey = MarketOrderEntity.ByOrderId.GeneratePartitionKey();

        public MarketOrdersRepository(INoSQLTableStorage<MarketOrderEntity> storage)
            : base(storage)
        {
        }

        public async Task<MarketOrderEntity> GetMarketOrderByIdAsync(string marketOrderId)
        {
            var result = await _storage.GetDataAsync(_orderKey, marketOrderId);
            return result;
        }

        protected override string GetAdditionalConditions()
        {
            string partitionFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey", QueryComparisons.Equal, MarketOrderEntity.ByOrderId.GeneratePartitionKey());
            return partitionFilter;
        }

        protected override string GetDateColumn()
        {
            return nameof(MarketOrderEntity.CreatedAt);
        }
    }
}
