﻿using System.Linq;
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
    public class LimitOrdersChecker : DataCheckerBase<LimitOrderEntity, LimitOrder>
    {
        private readonly ITradesRepository _tradesRepository;
        private readonly IMarketOrdersRepository _marketOrdersRepository;

        private int _addedTradesCount;

        public LimitOrdersChecker(
            string sqlConnecctionString,
            ILimitOrdersRepository repository,
            ITradesRepository tradesRepository,
            IMarketOrdersRepository marketOrdersRepository,
            ILog log)
            : base(
                sqlConnecctionString,
                true,
                repository,
                log)
        {
            _tradesRepository = tradesRepository;
            _marketOrdersRepository = marketOrdersRepository;
        }

        protected override void ClearCaches(bool isDuringProcessing)
        {
            if (!isDuringProcessing)
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
            }

            return result;
        }

        protected override bool UpdateItem(LimitOrder fromSql, LimitOrder converted, DataContext context)
        {
            bool changed = fromSql.Status != converted.Status
                || fromSql.RemainingVolume != converted.RemainingVolume
                || !fromSql.LastMatchTime.HasValue && converted.LastMatchTime.HasValue
                || fromSql.LastMatchTime.HasValue && converted.LastMatchTime.HasValue && converted.LastMatchTime.Value.Subtract(fromSql.LastMatchTime.Value).TotalMilliseconds >= 3;
            if (changed)
            {
                _log.WriteInfo(nameof(UpdateItem), converted.AssetPairId, fromSql.ToJson());
                fromSql.Status = converted.Status;
                if (converted.LastMatchTime.HasValue)
                    fromSql.LastMatchTime = converted.LastMatchTime;
                fromSql.RemainingVolume = converted.RemainingVolume;
            }
            bool childrenUpdated = UpdateChildren(fromSql, converted, context);

            return changed || childrenUpdated;
        }

        protected override void LogAdded(int addedCount)
        {
            _log.WriteWarning(nameof(CheckAndFixDataAsync), "TotalAdded", $"Added {addedCount} item(s).");
            if (_addedTradesCount > 0)
                _log.WriteWarning(nameof(CheckAndFixDataAsync), "TotalAddedChildren", $"Added {_addedTradesCount} LimitTradeInfos.");
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

        private async Task<LimitOrderEntity> GetLimitOrderAsync(string clientId, string limitOrderId)
        {
            var clientIdByLimitOrder = await _tradesRepository.GetClientIdByLimitOrderAsync(limitOrderId, clientId);
            if (string.IsNullOrEmpty(clientIdByLimitOrder))
                return null;
            var result = await ((ILimitOrdersRepository)_repository).GetLimitOrderByIdAsync(clientIdByLimitOrder, limitOrderId);
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

        private bool UpdateChildren(LimitOrder inSql, LimitOrder converted, DataContext context)
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
                var fromDb = childrenFromDb.FirstOrDefault(i => child.OppositeOrderId == i.OppositeOrderId);
                if (fromDb != null)
                    continue;

                if (!child.IsValid())
                    _log.WriteWarning(nameof(UpdateChildren), "Invalid", $"Found invalid child object - {child.ToJson()}!");
                _log.WriteInfo(nameof(UpdateChildren), $"{child.LimitOrderId}", $"Added trade {child.ToJson()} for LimitOrder {inSql.Id} with trades {childrenFromDb.ToJson()}");
                context.LimitTradeInfos.Add(child);
                ++_addedTradesCount;
                added = true;
            }
            return added;
        }

        private async Task<List<ClientTradeEntity>> GetChildrenAsync(IEnumerable<string> parentIds)
        {
            var result = await _tradesRepository.GetTradesByLimitOrderKeysAsync(parentIds);

            _log.WriteInfo(nameof(GetChildrenAsync), "FetchedChildren", $"Fetched {result.Count} trades.");

            return result;
        }
    }
}
