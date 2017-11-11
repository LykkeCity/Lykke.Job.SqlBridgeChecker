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
    public class LimitOrdersChecker : DataCheckerBase<LimitOrderEntity, LimitOrder>
    {
        private readonly ITradesRepository _tradesRepository;
        private readonly IMarketOrdersRepository _marketOrdersRepository;
        private readonly string _childPartition = ClientTradeEntity.ByDt.GeneratePartitionKey();
        private const string _parentIdInChildField = nameof(ClientTradeEntity.MarketOrderId);

        public LimitOrdersChecker(
            string sqlConnecctionString,
            ILimitOrdersRepository repository,
            ITradesRepository tradesRepository,
            IMarketOrdersRepository marketOrdersRepository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _tradesRepository = tradesRepository;
            _marketOrdersRepository = marketOrdersRepository;
        }

        protected override async Task<List<LimitOrder>> ConvertItemsToSqlTypesAsync(IEnumerable<LimitOrderEntity> items)
        {
            List<LimitOrder> result = new List<LimitOrder>();
            var allChildren = await GetChildrenAsync(items
                .Select(m => m.Id ?? m.RowKey)
                .Where(i => i != null && i.ToString() != string.Empty));
            var byOrders = allChildren
                .Where(i => !i.IsHidden)
                .GroupBy(c => c.PartitionKey)
                .ToDictionary(i => i.Key, i => new List<ClientTradeEntity>(i));
            foreach (var item in items)
            {
                List<ClientTradeEntity> children = null;
                string key = (item.Id ?? item.RowKey).ToString();
                if (byOrders.ContainsKey(key))
                    children = byOrders[key];
                var converted = await LimitOrder.FromModelAsync(
                    item,
                    children,
                    l => ((ILimitOrdersRepository)_repository).GetLimitOrderById(l),
                    m => _marketOrdersRepository.GetMarketOrderById(m),
                    _log);
                result.Add(converted);
            }
            return result;
        }

        protected override async Task<bool> UpdateItemAsync(LimitOrder inSql, LimitOrder converted, DataContext context)
        {
            bool changed = inSql.Status != converted.Status
                || !AreEqual(inSql.LastMatchTime, converted.LastMatchTime)
                || inSql.RemainingVolume != converted.RemainingVolume;
            if (changed)
            {
                await _log.WriteInfoAsync(
                    nameof(LimitOrdersChecker),
                    nameof(UpdateItemAsync),
                    $"Updated {inSql.ToJson()}.");
                inSql.Status = converted.Status;
                inSql.LastMatchTime = converted.LastMatchTime;
                inSql.RemainingVolume = converted.RemainingVolume;
            }
            bool childrenUpdated = await UpdateChildrenAsync(inSql, converted, context);

            return changed || childrenUpdated;
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
                var fromDb = childrenFromDb.FirstOrDefault(i => child.OppositeOrderId == i.OppositeOrderId);
                if (fromDb != null)
                    continue;

                if (!child.IsValid())
                    await _log.WriteWarningAsync(
                        nameof(LimitOrdersChecker),
                        nameof(UpdateChildrenAsync),
                        $"Found invalid child object - {child.ToJson()}!");
                context.LimitTradeInfos.Add(child);
                added = true;
                await _log.WriteInfoAsync(
                    nameof(LimitOrdersChecker),
                    nameof(UpdateChildrenAsync),
                    $"Added trade {child.ToJson()} for LimitOrder {inSql.Id}");
            }
            return added;
        }

        private async Task<List<ClientTradeEntity>> GetChildrenAsync(IEnumerable<string> parentIds)
        {
            var result = await _tradesRepository.GetTradesByLimitOrderKeysAsync(parentIds);

            await _log.WriteInfoAsync(
                nameof(LimitOrdersChecker),
                nameof(GetChildrenAsync),
                $"Fetched {result.Count} trades.");

            return result;
        }
    }
}
