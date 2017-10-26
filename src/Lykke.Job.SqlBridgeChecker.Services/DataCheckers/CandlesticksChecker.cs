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
    public class CandlesticksChecker : DataCheckerBase<FeedHistoryEntity, Candlestick>
    {
        public CandlesticksChecker(
            string sqlConnecctionString,
            ITableEntityRepository<FeedHistoryEntity> repository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
        }

        protected override async Task<List<Candlestick>> ConvertItemsToSqlTypesAsync(IEnumerable<FeedHistoryEntity> items)
        {
            var result = items
                .GroupBy(m =>
                    new {
                        RowKey = m.RowKey,
                        AssetPair = m.PartitionKey.Split('_')[0],
                    })
                .Select(g => Candlestick.FromModel(g, _log))
                .Where(c => c != null)
                .ToList();
            return result;
        }

        protected override async Task<Candlestick> FindInSqlDbAsync(Candlestick item, DataContext context)
        {
            return context
                .Set<Candlestick>()
                .FirstOrDefault(c =>
                    c.AssetPair == item.AssetPair
                    && c.IsAsk == item.IsAsk
                    && c.Start.RoundToSecond() == item.Start.RoundToSecond());
        }
    }
}
