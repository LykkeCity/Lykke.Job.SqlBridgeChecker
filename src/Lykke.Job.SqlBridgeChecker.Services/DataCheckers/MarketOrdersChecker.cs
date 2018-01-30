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
    public class MarketOrdersChecker : DataCheckerBase<MarketOrderEntity, MarketOrder>
    {
        private readonly IUserWalletsMapper _userWalletsMapper;
        private readonly ITradesRepository _tradesRepository;
        private readonly ILimitOrdersRepository _limitOrdersRepository;

        public MarketOrdersChecker(
            string sqlConnecctionString,
            IUserWalletsMapper userWalletsMapper,
            IMarketOrdersRepository repository,
            ILimitOrdersRepository limitOrdersRepository,
            ITradesRepository tradesRepository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _userWalletsMapper = userWalletsMapper;
            _limitOrdersRepository = limitOrdersRepository;
            _tradesRepository = tradesRepository;
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
                string key = (item.Id ?? item.RowKey).ToString();
                if (byOrders.ContainsKey(key))
                    children = byOrders[key];
                var converted = await MarketOrder.FromModelAsync(item, children, GetLimitOrder, _log);
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

        protected override async Task<bool> UpdateItemAsync(MarketOrder inSql, MarketOrder converted, DataContext context)
        {
            bool changed = !AreEqual(inSql.Price, converted.Price)
                || inSql.Status != converted.Status
                || !AreEqual(inSql.MatchedAt, converted.MatchedAt)
                || !AreEqual(inSql.ReservedLimitVolume, converted.ReservedLimitVolume);
            if (changed)
            {
                await _log.WriteInfoAsync(nameof(UpdateItemAsync), Name, $"{inSql.ToJson()}");
                inSql.Price = converted.Price;
                inSql.Status = converted.Status;
                inSql.MatchedAt = converted.MatchedAt;
                inSql.ReservedLimitVolume = converted.ReservedLimitVolume;
            }
            bool childrenUpdated = await UpdateChildrenAsync(inSql, converted, context);

            return changed || childrenUpdated;
        }

        private async Task<LimitOrderEntity> GetLimitOrder(string limitOrderId)
        {
            var result = await _limitOrdersRepository.GetLimitOrderByIdAsync(limitOrderId);
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
            await _log.WriteWarningAsync(nameof(GetLimitOrder), Name, $"Could not find LimitOrder by id = {limitOrderId}");
            return null;
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
                    await _log.WriteWarningAsync(nameof(UpdateChildrenAsync), Name, $"Found invalid child object - {child.ToJson()}!");
                await _log.WriteInfoAsync(nameof(UpdateChildrenAsync), Name, $"Added trade {child.ToJson()} for MarketOrder {inSql.Id}");
                context.TradeInfos.Add(child);
                added = true;
            }
            return added;
        }

        private async Task<List<ClientTradeEntity>> GetChildrenAsync(IEnumerable<MarketOrderEntity> parents)
        {
            var result = await _tradesRepository.GetTradesByMarketOrdersAsync(parents.Select(i => (i.ClientId, i.Id ?? i.RowKey)));

            await _log.WriteInfoAsync(nameof(GetChildrenAsync), Name, $"Initially fetched {result.Count} trades.");

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

            var missingTrades = await _tradesRepository.GetTradesByLimitOrderKeysAsync(missingClientsDict.Keys);

            await _log.WriteInfoAsync(nameof(GetChildrenAsync), Name, $"Then fetched {missingTrades.Count} trades for missing clients.");

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
