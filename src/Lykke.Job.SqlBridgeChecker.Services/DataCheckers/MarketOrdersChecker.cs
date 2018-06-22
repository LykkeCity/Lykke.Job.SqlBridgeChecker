using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class MarketOrdersChecker : DataCheckerBase<MarketOrderEntity, MarketOrder>
    {
        private readonly ITradesRepository _tradesRepository;
        private readonly ILimitOrdersRepository _limitOrdersRepository;

        private int _addedTradesCount;

        public MarketOrdersChecker(
            string sqlConnecctionString,
            IMarketOrdersRepository repository,
            ILimitOrdersRepository limitOrdersRepository,
            ITradesRepository tradesRepository,
            ILog log)
            : base(
                sqlConnecctionString,
                true,
                repository,
                log)
        {
            _limitOrdersRepository = limitOrdersRepository;
            _tradesRepository = tradesRepository;
        }

        protected override void ClearCaches(bool isDuringProcessing)
        {
            if (!isDuringProcessing)
                _addedTradesCount = 0;
        }

        protected override async Task<List<MarketOrder>> ConvertItemsToSqlTypesAsync(IEnumerable<MarketOrderEntity> items)
        {
            var result = new List<MarketOrder>();
            var allChildren = (IEnumerable<ClientTradeEntity>)await GetChildrenAsync(items);
            var byOrders = allChildren
                .Where(i => !i.IsHidden)
                .GroupBy(c => c.MarketOrderId)
                .ToDictionary(i => i.Key, i => new List<ClientTradeEntity>(i));
            var clientIds = new HashSet<string>();
            foreach (var item in items)
            {
                List<ClientTradeEntity> children = null;
                string key = item.Id ?? item.RowKey;
                if (byOrders.ContainsKey(key))
                    children = byOrders[key];
                var converted = await MarketOrder.FromModelAsync(item, children, GetLimitOrderAsync, _log);
                result.Add(converted);

                clientIds.Add(item.ClientId);
                if (children != null)
                    foreach (var child in children)
                    {
                        clientIds.Add(child.ClientId);
                    }
            }

            return result;
        }

        protected override bool UpdateItem(MarketOrder inSql, MarketOrder converted, DataContext context)
        {
            bool changed = !AreEqual(inSql.Price, converted.Price)
                || inSql.Status != converted.Status
                || !AreEqual(inSql.MatchedAt, converted.MatchedAt)
                || !AreEqual(inSql.ReservedLimitVolume, converted.ReservedLimitVolume);
            if (changed)
            {
                _log.WriteInfo(nameof(UpdateItem), converted.AssetPairId, $"{inSql.ToJson()}");
                inSql.Price = converted.Price;
                inSql.Status = converted.Status;
                inSql.MatchedAt = converted.MatchedAt;
                inSql.ReservedLimitVolume = converted.ReservedLimitVolume;
            }
            bool childrenUpdated = UpdateChildren(inSql, converted, context);

            return changed || childrenUpdated;
        }

        protected override void LogAdded(int addedCount)
        {
            _log.WriteWarning(nameof(CheckAndFixDataAsync), "TotalAdded", $"Added {addedCount} item(s).");
            if (_addedTradesCount > 0)
                _log.WriteWarning(nameof(CheckAndFixDataAsync), "TotalAddedChildren", $"Added {_addedTradesCount} LimitTradeInfos.");
        }

        private async Task<LimitOrderEntity> GetLimitOrderAsync(string clientId, string limitOrderId)
        {
            var clientIdByLimitOrder = await _tradesRepository.GetClientIdByLimitOrderAsync(limitOrderId, clientId);
            if (string.IsNullOrEmpty(clientIdByLimitOrder))
                return null;
            var result = await _limitOrdersRepository.GetLimitOrderByIdAsync(clientIdByLimitOrder, limitOrderId);
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
            _log.WriteWarning(nameof(GetLimitOrderAsync), "LO not found", $"Could not find LimitOrder by id = {limitOrderId}");
            return null;
        }

        private bool UpdateChildren(MarketOrder inSql, MarketOrder converted, DataContext context)
        {
            if (converted.Trades == null || converted.Trades.Count == 0)
                return false;

            var childrenFromDb = context
                .Set<TradeInfo>()
                .Where(t => t.MarketOrderId == inSql.Id)
                .ToList();
            if (childrenFromDb.Count == converted.Trades.Count)
                return false;

            bool added = false;
            foreach (var child in converted.Trades)
            {
                var fromDb = childrenFromDb.FirstOrDefault(i => child.LimitOrderExternalId == i.LimitOrderExternalId);
                if (fromDb != null)
                    continue;

                if (!child.IsValid())
                    _log.WriteWarning(nameof(UpdateChildren), "Invalid", $"Found invalid child object - {child.ToJson()}!");
                _log.WriteInfo(nameof(UpdateChildren), $"{child.MarketOrderId}", $"Added trade {child.ToJson()} for MarketOrder {inSql.Id} with trades {childrenFromDb.ToJson()}");
                context.TradeInfos.Add(child);
                ++_addedTradesCount;
                added = true;
            }
            return added;
        }

        private async Task<List<ClientTradeEntity>> GetChildrenAsync(IEnumerable<MarketOrderEntity> parents)
        {
            var result = await _tradesRepository.GetTradesByMarketOrdersAsync(parents.Select(i => (i.ClientId, i.Id ?? i.RowKey)));

            _log.WriteInfo(nameof(GetChildrenAsync), "FetchedChildren", $"Initially fetched {result.Count} trades.");

            var missingLimitOrderTradesDict = new Dictionary<string, List<ClientTradeEntity>>();
            var groupsByMarketOrderId = result.GroupBy(t => t.MarketOrderId);
            foreach (var marketIdgroup in groupsByMarketOrderId)
            {
                var groupsByLimitOrder = marketIdgroup.GroupBy(t => t.LimitOrderId);
                foreach (var group in groupsByLimitOrder)
                {
                    var clients = group.Select(t => t.ClientId).Distinct().ToList();
                    if (clients.Count == 1)
                    {
                        var trade = group.First();
                        if (missingLimitOrderTradesDict.ContainsKey(trade.LimitOrderId))
                        {
                            var trades = missingLimitOrderTradesDict[trade.LimitOrderId];
                            if (!trades.Any(t => t.MarketOrderId == trade.MarketOrderId))
                                trades.Add(trade);
                        }
                        else
                        {
                            missingLimitOrderTradesDict.Add(trade.LimitOrderId, new List<ClientTradeEntity> { trade });
                        }
                    }
                }
            }

            var missingTrades = await _tradesRepository.GetTradesByLimitOrderKeysAsync(missingLimitOrderTradesDict.Keys);

            _log.WriteInfo(nameof(GetChildrenAsync), "ThenFetchedChildren", $"Then fetched {missingTrades.Count} trades for missing limit orders.");

            foreach (var trade in missingTrades)
            {
                if (!string.IsNullOrWhiteSpace(trade.MarketOrderId))
                    continue;

                var clientTrades = missingLimitOrderTradesDict[trade.LimitOrderId];
                var matchingTrade = clientTrades.FirstOrDefault(t =>
                    trade.OppositeLimitOrderId == t.MarketOrderId
                    && t.ClientId != trade.ClientId);
                if (matchingTrade != null)
                {
                    trade.MarketOrderId = matchingTrade.MarketOrderId;
                    result.Add(trade);
                }
            }

            return result;
        }
    }
}
