using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class MarketOrdersChecker : DataCheckerBase<MarketOrderEntity, MarketOrder>
    {
        private readonly ITradesRepository _tradesRepository;
        private readonly ILimitOrdersRepository _limitOrdersRepository;

        public MarketOrdersChecker(
            string sqlConnecctionString,
            IMarketOrdersRepository repository,
            ILimitOrdersRepository limitOrdersRepository,
            ITradesRepository tradesRepository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _limitOrdersRepository = limitOrdersRepository;
            _tradesRepository = tradesRepository;
        }

        protected override async Task<List<MarketOrder>> ConvertItemsToSqlTypesAsync(IEnumerable<MarketOrderEntity> items)
        {
            var result = new List<MarketOrder>();
            var allChildren = (IEnumerable<ClientTradeEntity>)await GetChildrenAsync(items.Select(m => m.Id ?? m.RowKey));
            var byOrders = allChildren
                .Where(i => !i.IsHidden)
                .GroupBy(c => c.MarketOrderId)
                .ToDictionary(i => i.Key, i => new List<ClientTradeEntity>(i));
            foreach (var item in items)
            {
                List<ClientTradeEntity> children = null;
                string key = (item.Id ?? item.RowKey).ToString();
                if (byOrders.ContainsKey(key))
                    children = byOrders[key];
                var converted = await MarketOrder.FromModelAsync(item, children, GetOtherClientAsync, _log);
                result.Add(converted);
            }
            return result;
        }

        protected override async Task<bool> UpdateItemAsync(MarketOrder inSql, MarketOrder converted, DataContext context)
        {
            bool changed = !AreEqual(inSql.Price, converted.Price)
                || inSql.Status != converted.Status
                || !AreEqual(inSql.MatchedAt, converted.MatchedAt)
                || !AreEqual(inSql.ReservedLimitVolume, converted.ReservedLimitVolume);
            if (changed)
            {
                await _log.WriteInfoAsync(
                    nameof(MarketOrdersChecker),
                    nameof(UpdateItemAsync),
                    $"Updated {inSql.ToJson()}.");
                inSql.Price = converted.Price;
                inSql.Status = converted.Status;
                inSql.MatchedAt = converted.MatchedAt;
                inSql.ReservedLimitVolume = converted.ReservedLimitVolume;
            }
            bool childrenUpdated = await UpdateChildrenAsync(inSql, converted, context);

            return changed || childrenUpdated;
        }

        private async Task<string> GetOtherClientAsync(string limitOrderId)
        {
            var limitOrder = await _limitOrdersRepository.GetLimitOrderByIdAsync(limitOrderId, null);
            return limitOrder?.ClientId;
        }

        private async Task<bool> UpdateChildrenAsync(MarketOrder inSql, MarketOrder converted, DataContext context)
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
                    await _log.WriteWarningAsync(
                        nameof(MarketOrdersChecker),
                        nameof(UpdateChildrenAsync),
                        $"Found invalid child object - {child.ToJson()}!");
                context.TradeInfos.Add(child);
                added = true;
                await _log.WriteInfoAsync(
                    nameof(MarketOrdersChecker),
                    nameof(UpdateChildrenAsync),
                    $"Added trade {child.ToJson()} for MarketOrder {inSql.Id}");
            }
            return added;
        }

        private async Task<List<ClientTradeEntity>> GetChildrenAsync(IEnumerable<object> parentIds)
        {
            var result = await _tradesRepository.GetTradesByMarketOrdersAsync(parentIds.Select(i => i.ToString()));

            await _log.WriteInfoAsync(
                nameof(MarketOrdersChecker),
                nameof(GetChildrenAsync),
                $"Initially fetched {result.Count} trades.");

            var missingClientsDict = new Dictionary<string, List<ClientTradeEntity>>();
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
                        if (missingClientsDict.ContainsKey(trade.LimitOrderId))
                        {
                            var trades = missingClientsDict[trade.LimitOrderId];
                            if (!trades.Any(t => t.MarketOrderId == trade.MarketOrderId))
                                trades.Add(trade);
                        }
                        else
                        {
                            missingClientsDict.Add(trade.LimitOrderId, new List<ClientTradeEntity> { trade });
                        }
                    }
                }
            }

            var missingTrades = await _tradesRepository.GetTradesByLimitOrderIdsAsync(missingClientsDict.Keys);

            await _log.WriteInfoAsync(
                nameof(MarketOrdersChecker),
                nameof(GetChildrenAsync),
                $"Then fetched {missingTrades.Count} trades for missing clients.");

            foreach (var trade in missingTrades)
            {
                if (!string.IsNullOrWhiteSpace(trade.MarketOrderId))
                    continue;

                var clientTrades = missingClientsDict[trade.LimitOrderId];
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
