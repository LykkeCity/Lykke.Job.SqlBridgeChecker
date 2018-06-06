using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class LimitOrdersRepository : ItemsRepositoryBase<LimitOrderEntity>, ILimitOrdersRepository
    {
        //private readonly string _orderKey = LimitOrderEntity.ByOrderId.GeneratePartitionKey();

        public LimitOrdersRepository(INoSQLTableStorage<LimitOrderEntity> storage)
            : base(storage)
        {
        }

        public async Task<LimitOrderEntity> GetLimitOrderByIdAsync(string clientId, string limitOrderId)
        {
            var result = await _storage.GetDataAsync(LimitOrderEntity.ByClientId.GeneratePartitionKey(clientId), limitOrderId);
            return result;
        }

        protected override string GetAdditionalConditions(DateTime from, DateTime to)
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
