using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Common.Log;
using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class LimitOrdersRepository : ItemsRepositoryBase<LimitOrderEntity>, ILimitOrdersRepository
    {
        private readonly ILog _log;
        private readonly string _orderKey = LimitOrderEntity.ByOrderId.GeneratePartitionKey();

        public LimitOrdersRepository(INoSQLTableStorage<LimitOrderEntity> storage, ILog log)
            : base(storage)
        {
            _log = log;
        }

        public async Task<LimitOrderEntity> GetLimitOrderByIdAsync(string limitOrderId, string clientId)
        {
            var result = await _storage.GetDataAsync(
                !string.IsNullOrWhiteSpace(clientId) ? clientId : _orderKey, limitOrderId);
            if (result != null)
                return result;

            string partitionFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey",
                QueryComparisons.Equal,
                !string.IsNullOrWhiteSpace(clientId) ? clientId : _orderKey);
            string idFilter = TableQuery.GenerateFilterCondition(nameof(LimitOrderEntity.MatchingId), QueryComparisons.Equal, limitOrderId);
            string filter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, idFilter);
            var query = new TableQuery<LimitOrderEntity>().Where(filter);
            var items = await _storage.WhereAsync(query);
            return items.FirstOrDefault();
        }

        public async Task<List<LimitOrderEntity>> GetOrdesByMatchingIdsAsync(IEnumerable<string> matchingIds)
        {
            var orders = await BatchHelper.BatchGetDataAsync(
                "OrderId",
                nameof(LimitOrderEntity.MatchingId),
                matchingIds,
                _storage,
                _log);
            return orders;
        }

        protected override string GetAdditionalConditions()
        {
            string partitionFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey", QueryComparisons.Equal, LimitOrderEntity.ByOrderId.GeneratePartitionKey());
            return partitionFilter;
        }

        protected override string GetDateColumn()
        {
            return nameof(LimitOrderEntity.CreatedAt);
        }
    }
}
