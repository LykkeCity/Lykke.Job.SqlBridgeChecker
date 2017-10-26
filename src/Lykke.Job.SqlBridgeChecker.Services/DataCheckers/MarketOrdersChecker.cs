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

        public MarketOrdersChecker(
            string sqlConnecctionString,
            IMarketOrdersRepository repository,
            ITradesRepository tradesRepository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
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
                var converted = await MarketOrder.FromModelAsync(item, children, m => _tradesRepository.GetOtherClientAsync(m), _log);
                result.Add(converted);
            }
            return result;
        }

        protected override async Task<bool> UpdateItemAsync(MarketOrder inSql, MarketOrder convertedItem, DataContext context)
        {
            if (convertedItem.Trades == null || convertedItem.Trades.Count == 0)
                return false;

            var childrenFromDb = context
                .Set<TradeInfo>()
                .Where(t => t.MarketOrderId == inSql.Id)
                .ToList();
            if (childrenFromDb.Count == convertedItem.Trades.Count)
                return false;

            bool added = false;
            foreach (var child in convertedItem.Trades)
            {
                var fromDb = childrenFromDb.FirstOrDefault(i => child.LimitOrderId == i.LimitOrderId);
                if (fromDb != null)
                    continue;

                if (!child.IsValid())
                    await _log.WriteWarningAsync(
                        nameof(MarketOrdersChecker),
                        nameof(UpdateItemAsync),
                        $"Found invalid child object - {child.ToJson()}!");
                context.Add(child);
                added = true;
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
