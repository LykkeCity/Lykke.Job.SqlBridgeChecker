using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class LimitOrdersRepository : ItemsRepositoryBase<LimitOrderEntity>, ILimitOrdersRepository
    {
        public LimitOrdersRepository(INoSQLTableStorage<LimitOrderEntity> storage)
            : base(storage)
        {
        }

        public async Task<LimitOrderEntity> GetLimitOrderById(string limitOrderId)
        {
            string partitionFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey", QueryComparisons.Equal, LimitOrderEntity.ByOrderId.GeneratePartitionKey());
            string idFilter = TableQuery.GenerateFilterCondition(nameof(LimitOrderEntity.MatchingId), QueryComparisons.Equal, limitOrderId);
            string filter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, idFilter);
            var query = new TableQuery<LimitOrderEntity>().Where(filter);
            var items = await _storage.WhereAsync(query);
            var result = items.FirstOrDefault();
            if (result != null)
                return result;
            query = new TableQuery<LimitOrderEntity>().Where(idFilter);
            items = await _storage.WhereAsync(query);
            return items.FirstOrDefault();
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
