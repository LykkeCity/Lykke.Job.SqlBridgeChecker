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
            var result = new List<TradeLogItem>();
            var groups = items.GroupBy(i => new { i.MarketOrderId, i.LimitOrderId, i.OppositeLimitOrderId });
            foreach (var group in groups)
            {
                var trades = await TradeLogItem.FromModelAsync(group, _log);
                result.AddRange(trades);
            }
            return result;
        }

        protected override async Task<TradeLogItem> FindInSqlDbAsync(TradeLogItem item, DataContext context)
        {
            var inSql = await TradeSqlFinder.FindInDbAsync(item, context, _log);
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
                || inSql.Direction != convertedItem.Direction
                || inSql.IsHidden != convertedItem.IsHidden;
            if (!changed)
                return false;
            await _log.WriteInfoAsync(
                nameof(TradesChecker),
                nameof(FindInSqlDbAsync),
                $"Updated trade {inSql.ToJson()}.");
            inSql.TradeId = convertedItem.TradeId;
            inSql.OppositeOrderId = convertedItem.OppositeOrderId;
            inSql.Direction = convertedItem.Direction;
            inSql.IsHidden = convertedItem.IsHidden;
            return true;
        }
    }
}
