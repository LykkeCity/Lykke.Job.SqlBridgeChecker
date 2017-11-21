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

        public async Task<LimitOrderEntity> GetLimitOrderByIdAsync(string limitOrderId)
        {
            var result = await _storage.GetDataAsync(_orderKey, limitOrderId);
            return result;
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
