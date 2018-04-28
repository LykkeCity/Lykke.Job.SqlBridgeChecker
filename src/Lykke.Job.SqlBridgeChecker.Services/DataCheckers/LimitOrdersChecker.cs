using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class LimitOrdersChecker : DataCheckerBase<LimitOrderEntity, LimitOrder>
    {
        private readonly IUserWalletsMapper _userWalletsMapper;
        private readonly ITradesRepository _tradesRepository;
        private readonly IMarketOrdersRepository _marketOrdersRepository;

        private int _addedTradesCount;

        public LimitOrdersChecker(
            string sqlConnecctionString,
            IUserWalletsMapper userWalletsMapper,
            ILimitOrdersRepository repository,
            ITradesRepository tradesRepository,
            IMarketOrdersRepository marketOrdersRepository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _userWalletsMapper = userWalletsMapper;
            _tradesRepository = tradesRepository;
            _marketOrdersRepository = marketOrdersRepository;
        }

        protected override void ClearCaches()
        {
            _addedTradesCount = 0;
        }

        protected override async Task<List<LimitOrder>> ConvertItemsToSqlTypesAsync(IEnumerable<LimitOrderEntity> items)
        {
            List<LimitOrder> result = new List<LimitOrder>();
            var allChildren = await GetChildrenAsync(items
                .Select(m => m.Id ?? m.RowKey)
                .Where(i => !string.IsNullOrWhiteSpace(i)));
            var byOrders = allChildren
                .Where(i => !i.IsHidden)
                .GroupBy(c => c.PartitionKey)
                .ToDictionary(i => i.Key, i => new List<ClientTradeEntity>(i));
            var clientIds = new HashSet<string>();
            foreach (var item in items)
            {
                List<ClientTradeEntity> children = null;
                string key = item.Id ?? item.RowKey;
                if (byOrders.ContainsKey(key))
                    children = byOrders[key];
                var converted = await LimitOrder.FromModelAsync(
                    item,
                    children,
                    GetLimitOrderAsync,
                    GetMarketOrderAsync,
                    _log);
                result.Add(converted);

                clientIds.Add(item.ClientId);
                if (children != null)
                    foreach (var child in children)
                    {
                        clientIds.Add(child.ClientId);
                    }
            }

            await _userWalletsMapper.AddWalletsAsync(clientIds);

            return result;
        }

        protected override async Task<bool> UpdateItemAsync(LimitOrder inSql, LimitOrder converted, DataContext context)
        {
            bool changed = inSql.Status != converted.Status
                || !AreEqual(inSql.LastMatchTime, converted.LastMatchTime)
                || inSql.RemainingVolume != converted.RemainingVolume;
            if (changed)
            {
                await _log.WriteInfoAsync(nameof(UpdateItemAsync), converted.AssetPairId, $"{inSql.ToJson()}");
                inSql.Status = converted.Status;
                inSql.LastMatchTime = converted.LastMatchTime;
                inSql.RemainingVolume = converted.RemainingVolume;
            }
            bool childrenUpdated = await UpdateChildrenAsync(inSql, converted, context);

            return changed || childrenUpdated;
        }

        protected override async Task LogAddedAsync(int addedCount)
        {
            await _log.WriteWarningAsync(nameof(CheckAndFixDataAsync), "TotalAdded", $"Added {addedCount} item(s).");
            if (_addedTradesCount > 0)
                await _log.WriteWarningAsync(nameof(CheckAndFixDataAsync), "TotalAddedChildren", $"Added {_addedTradesCount} LimitTradeInfos.");
        }

        private async Task<MarketOrderEntity> GetMarketOrderAsync(string marketOrderId)
        {
            var result = await _marketOrdersRepository.GetMarketOrderByIdAsync(marketOrderId);
            if (result != null)
                return result;
            var moFromSql = OrdersFinder.GetMarketOrder(marketOrderId, _sqlConnectionString);
            if (moFromSql != null)
                return new MarketOrderEntity
                {
                    Id = moFromSql.ExternalId,
                    MatchingId = moFromSql.Id,
                    ClientId = moFromSql.ClientId,
                    AssetPairId = moFromSql.AssetPairId,
                };
            return null;
        }

        private async Task<LimitOrderEntity> GetLimitOrderAsync(string limitOrderId)
        {
            var result = await ((ILimitOrdersRepository)_repository).GetLimitOrderByIdAsync(limitOrderId);
            if (result != null)
                return result;
            var loFromSql = OrdersFinder.GetLimitOrder(limitOrderId, _sqlConnectionString);
            if (loFromSql != null)
                return new LimitOrderEntity
                {
                    Id = loFromSql.ExternalId,
                    MatchingId = loFromSql.Id,
                    ClientId = loFromSql.ClientId,
                    AssetPairId = loFromSql.AssetPairId,
                };
            return null;
        }

        private async Task<bool> UpdateChildrenAsync(LimitOrder inSql, LimitOrder converted, DataContext context)
        {
            if (converted.Trades == null || converted.Trades.Count == 0)
                return false;

            var childrenFromDb = context
                .Set<LimitTradeInfo>()
                .Where(t => t.LimitOrderId == inSql.Id)
                .ToList();
            if (childrenFromDb.Count == converted.Trades.Count)
                return false;

            bool added = false;
            foreach (var child in converted.Trades)
            {
                var fromDb = childrenFromDb.FirstOrDefault(i =>
                    child.OppositeOrderId == i.OppositeOrderId && child.Timestamp == i.Timestamp);
                if (fromDb != null)
                    continue;

                if (!child.IsValid())
                    await _log.WriteWarningAsync(nameof(UpdateChildrenAsync), "Invalid", $"Found invalid child object - {child.ToJson()}!");
                await _log.WriteInfoAsync(nameof(UpdateChildrenAsync), $"{child.Asset}_{child.OppositeAsset}", $"Added trade {child.ToJson()} for LimitOrder {inSql.Id} with trades {childrenFromDb}");
                context.LimitTradeInfos.Add(child);
                ++_addedTradesCount;
                added = true;
            }
            return added;
        }

        private async Task<List<ClientTradeEntity>> GetChildrenAsync(IEnumerable<string> parentIds)
        {
            var result = await _tradesRepository.GetTradesByLimitOrderKeysAsync(parentIds);

            await _log.WriteInfoAsync(nameof(GetChildrenAsync), "FetchedChildren", $"Fetched {result.Count} trades.");

            return result;
        }
    }
}
