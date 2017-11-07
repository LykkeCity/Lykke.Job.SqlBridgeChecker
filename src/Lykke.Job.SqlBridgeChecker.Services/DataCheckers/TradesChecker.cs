using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class TradesChecker : DataCheckerBase<ClientTradeEntity, TradeLogItem>
    {
        public TradesChecker(
            string sqlConnecctionString,
            ITradesRepository repository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
        }

        protected override async Task<List<TradeLogItem>> ConvertItemsToSqlTypesAsync(IEnumerable<ClientTradeEntity> items)
        {
            return items
                .GroupBy(i => new { i.MarketOrderId, i.LimitOrderId, i.OppositeLimitOrderId })
                .SelectMany(i => TradeLogItem.FromModel(i, _log))
                .ToList();
        }

        protected override async Task<TradeLogItem> FindInSqlDbAsync(TradeLogItem item, DataContext context)
        {
            var inSql = await TradeSqlFinder.FindInDbAsync(item, context, _log);
            return inSql;
        }

        protected override async Task<bool> UpdateItemAsync(TradeLogItem inSql, TradeLogItem convertedItem, DataContext context)
        {
            if (inSql.IsHidden == convertedItem.IsHidden)
                return false;
            inSql.IsHidden = convertedItem.IsHidden;
            return true;
        }
    }
}
