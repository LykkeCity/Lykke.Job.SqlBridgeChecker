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
    public class BalanceUpdatesChecker : DataCheckerBase<ClientBalanceChangeLogRecordEntity, BalanceUpdate>
    {
        public BalanceUpdatesChecker(
            string sqlConnecctionString,
            ITableEntityRepository<ClientBalanceChangeLogRecordEntity> repository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
        }

        protected override async Task<List<BalanceUpdate>> ConvertItemsToSqlTypesAsync(IEnumerable<ClientBalanceChangeLogRecordEntity> items)
        {
            var result = items
                .GroupBy(m => m.TransactionId)
                .Where(g => g.Key != null)
                .Select(g => BalanceUpdate.FromModel(g))
                .ToList();
            return result;
        }

        protected override async Task<bool> UpdateItemAsync(BalanceUpdate inSql, BalanceUpdate convertedItem, DataContext context)
        {
            if (convertedItem.Balances == null || convertedItem.Balances.Count == 0)
                return false;

            var clientBalanceUpdates = context
                .Set<ClientBalanceUpdate>()
                .Where(t => t.BalanceUpdateId == inSql.Id)
                .ToList();

            if (clientBalanceUpdates.Count == convertedItem.Balances.Count)
                return false;

            bool added = false;
            foreach (var clientBalanceUpdate in convertedItem.Balances)
            {
                var fromDb = clientBalanceUpdates
                    .Where(c =>
                        c.ClientId == clientBalanceUpdate.ClientId
                        && c.Asset == clientBalanceUpdate.Asset)
                    .FirstOrDefault();
                if (fromDb != null)
                    continue;

                if (!fromDb.IsValid())
                    await _log.WriteWarningAsync(
                        nameof(BalanceUpdatesChecker),
                        nameof(UpdateItemAsync),
                        $"Found invalid child object - {fromDb.ToJson()}!");
                context.ClientBalanceUpdates.Add(clientBalanceUpdate);
                added = true;
            }
            return added;
        }
    }
}
