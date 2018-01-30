using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Common.Log;
using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Helpers;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class TradesRepository : ItemsRepositoryBase<ClientTradeEntity>, ITradesRepository
    {
        private readonly ILog _log;

        private const string _marketOrderIdField = nameof(ClientTradeEntity.MarketOrderId);
        private const string _partitionKey = "PartitionKey";

        public TradesRepository(INoSQLTableStorage<ClientTradeEntity> storage, ILog log)
            : base(storage)
        {
            _log = log;
        }

        public async Task<string> GetOtherClientAsync(string multisig)
        {
            var item = await _storage.GetTopRecordAsync(multisig);
            return item?.ClientId;
        }

        public async Task<List<ClientTradeEntity>> GetTradesByLimitOrderKeysAsync(IEnumerable<string> limitOrderIds)
        {
            var result = await BatchHelper.BatchGetDataAsync(
                null,
                _partitionKey,
                limitOrderIds,
                _storage,
                _log);
            return result;
        }

        public async Task<List<ClientTradeEntity>> GetTradesByMarketOrdersAsync(IEnumerable<(string, string)> userMarketOrders)
        {
            var result = new List<ClientTradeEntity>();
            foreach (var userOrder in userMarketOrders)
            {
                string partitionFilter = TableQuery.GenerateFilterCondition(_partitionKey, QueryComparisons.Equal, userOrder.Item1);
                var orderFilter = TableQuery.GenerateFilterCondition(_marketOrderIdField, QueryComparisons.Equal, userOrder.Item2);
                var filter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, orderFilter);
                var query = new TableQuery<ClientTradeEntity>().Where(filter);
                var items = await _storage.WhereAsync(query);
                result.AddRange(items);
            }
            return result;
        }

        protected override string GetDateColumn()
        {
            return nameof(ClientTradeEntity.DateTime);
        }

        protected override string GetAdditionalConditions()
        {
            string partitionFilter = TableQuery.GenerateFilterCondition(
                "PartitionKey", QueryComparisons.Equal, ClientTradeEntity.ByDt.GeneratePartitionKey());
            return partitionFilter;
        }
    }
}
