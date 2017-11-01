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
        private readonly HashSet<string> _missingPairs = new HashSet<string>();

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
            var inSql = CandlestickSqlFinder.FindInDb(item, context);
            if (inSql == null)
                _missingPairs.Add(item.AssetPair);
            return inSql;
        }

        protected override async Task LogAddedAsync(int addedCount)
        {
            await _log.WriteWarningAsync(Name, nameof(CheckAndFixDataAsync), $"Added {addedCount} items.");
            if (_missingPairs.Count > 0)
            {
                string missingPairs = string.Join(",", _missingPairs);
                await _log.WriteInfoAsync(Name, nameof(CandlesticksChecker), $"Missing {missingPairs}.");
                _missingPairs.Clear();
            }
        }
    }
}
