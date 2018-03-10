using System;
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
        private const string _rowKey = "RowKey";

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
            var usersDict = new Dictionary<string, List<string>>();
            foreach (var userOrder in userMarketOrders)
            {
                if (usersDict.ContainsKey(userOrder.Item1))
                    usersDict[userOrder.Item1].Add(userOrder.Item2);
                else
                    usersDict.Add(userOrder.Item1, new List<string> { userOrder.Item2 });
            }
            var result = new List<ClientTradeEntity>();
            foreach (var pair in usersDict)
            {
                var items = await BatchHelper.BatchGetDataAsync(
                    pair.Key,
                    _marketOrderIdField,
                    pair.Value,
                    _storage,
                    _log);
                result.AddRange(items);
            }
            return result;
        }

        protected override string GetDateColumn()
        {
            return nameof(ClientTradeEntity.DateTime);
        }

        protected override string GetAdditionalConditions(DateTime from, DateTime to)
        {
            var partitionFilter = TableQuery.GenerateFilterCondition(_partitionKey, QueryComparisons.Equal, ClientTradeEntity.ByDt.GeneratePartitionKey());
            var fromFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.GreaterThanOrEqual, ClientTradeEntity.ByDt.GetRowKeyPart(from));
            var toFilter = TableQuery.GenerateFilterCondition(_rowKey, QueryComparisons.LessThan, ClientTradeEntity.ByDt.GetRowKeyPart(to));
            var filter = TableQuery.CombineFilters(fromFilter, TableOperators.And, toFilter);
            filter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, filter);
            return filter;
        }
    }
}
