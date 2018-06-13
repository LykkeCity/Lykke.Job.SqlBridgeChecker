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
        private const string _partitionKeyColumn = "PartitionKey";

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
            string fromFilter = TableQuery.GenerateFilterCondition(
                _partitionKeyColumn, QueryComparisons.GreaterThanOrEqual, LimitOrderEntity.ByDate.GeneratePartitionKey(from));
            string toFilter = TableQuery.GenerateFilterCondition(
                _partitionKeyColumn, QueryComparisons.LessThan, LimitOrderEntity.ByDate.GeneratePartitionKey(to));
            return TableQuery.CombineFilters(fromFilter, TableOperators.And, toFilter);
        }

        protected override string GetDateColumn()
        {
            return nameof(LimitOrderEntity.CreatedAt);
        }
    }
}
