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
    public class TradesChecker : DataCheckerBase<ClientTradeEntity, TradeLogItem>
    {
        private readonly ILimitOrdersRepository _limitOrdersRepository;
        private readonly Dictionary<string, LimitOrderEntity> _limitOrdersCache = new Dictionary<string, LimitOrderEntity>();

        public TradesChecker(
            string sqlConnecctionString,
            ITradesRepository repository,
            ILimitOrdersRepository limitOrdersRepository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _limitOrdersRepository = limitOrdersRepository;
        }

        protected override async Task<List<TradeLogItem>> ConvertItemsToSqlTypesAsync(IEnumerable<ClientTradeEntity> items)
        {
            var matchingIds = items
                .Where(i => i.OppositeLimitOrderId != null)
                .Select(i => i.OppositeLimitOrderId)
                .ToHashSet();
            await InitLimitOrdersCacheAsync(matchingIds);

            var result = new List<TradeLogItem>();
            var groups = items.GroupBy(i => new { i.MarketOrderId, i.LimitOrderId, i.OppositeLimitOrderId });
            foreach (var group in groups)
            {
                LimitOrderEntity oppositeLimitOrder = null;
                if (group.Key.OppositeLimitOrderId != null && _limitOrdersCache.ContainsKey(group.Key.OppositeLimitOrderId))
                    oppositeLimitOrder = _limitOrdersCache[group.Key.OppositeLimitOrderId];
                var trades = await TradeLogItem.FromModelAsync(group, oppositeLimitOrder, _log);
                result.AddRange(trades);
            }
            return result;
        }

        protected override async Task<TradeLogItem> FindInSqlDbAsync(TradeLogItem item, DataContext context)
        {
            string alternativeTradeId = _limitOrdersCache.ContainsKey(item.OppositeOrderId)
                ? TradeLogItem.GetTradeId(item.OrderId, _limitOrdersCache[item.OppositeOrderId].MatchingId) : null;
            var inSql = await TradeSqlFinder.FindInDbAsync(
                item,
                alternativeTradeId,
                context,
                _log);
            if (inSql == null)
                await _log.WriteInfoAsync(
                    nameof(TradesChecker),
                    nameof(FindInSqlDbAsync),
                    $"Added trade {item.ToJson()}.");
            return inSql;
        }

        protected override async Task<bool> UpdateItemAsync(TradeLogItem inSql, TradeLogItem convertedItem, DataContext context)
        {
            var changed = inSql.TradeId != convertedItem.TradeId
                || inSql.OppositeOrderId != convertedItem.OppositeOrderId
                || inSql.IsHidden != convertedItem.IsHidden;
            if (!changed)
                return false;
            inSql.TradeId = convertedItem.TradeId;
            inSql.OppositeOrderId = convertedItem.OppositeOrderId;
            inSql.IsHidden = convertedItem.IsHidden;
            return true;
        }

        private async Task InitLimitOrdersCacheAsync(IEnumerable<string> matchingIds)
        {
            var orders = await _limitOrdersRepository.GetOrdesByMatchingIds(matchingIds);
            foreach (var order in orders)
            {
                _limitOrdersCache[order.Id] = order;
                _limitOrdersCache[order.MatchingId] = order;
            }

            await _log.WriteInfoAsync(
                nameof(TradesChecker),
                nameof(InitLimitOrdersCacheAsync),
                $"Fetched {orders.Count} orders for LimitOrders cache");
        }
    }
}
