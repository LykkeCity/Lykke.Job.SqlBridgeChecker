using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class TradesRepository : ITradesRepository
    {
        private readonly INoSQLTableStorage<ClientTradeEntity> _storage;
        private readonly ILog _log;

        private readonly string _childPartition = ClientTradeEntity.ByDt.GeneratePartitionKey();
        private const string _marketOrderIdField = nameof(ClientTradeEntity.MarketOrderId);
        private const string _limitOrderIdField = nameof(ClientTradeEntity.LimitOrderId);

        public TradesRepository(INoSQLTableStorage<ClientTradeEntity> storage, ILog log)
        {
            _storage = storage;
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
                "PartitionKey",
                limitOrderIds,
                _storage,
                _log);
            return result;
        }

        public async Task<List<ClientTradeEntity>> GetTradesByLimitOrderIdsAsync(IEnumerable<string> limitOrderIds)
        {
            var result = await BatchHelper.BatchGetDataAsync(
                _childPartition,
                _limitOrderIdField,
                limitOrderIds,
                _storage,
                _log);
            return result;
        }

        public async Task<List<ClientTradeEntity>> GetTradesByMarketOrdersAsync(IEnumerable<string> marketOrderIds)
        {
            var result = await BatchHelper.BatchGetDataAsync(
                _childPartition,
                _marketOrderIdField,
                marketOrderIds,
                _storage,
                _log);
            return result;
        }
    }
}
