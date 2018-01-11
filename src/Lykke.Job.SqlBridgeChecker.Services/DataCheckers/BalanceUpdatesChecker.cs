using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.AzureRepositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class BalanceUpdatesChecker : DataCheckerBase<ClientBalanceChangeLogRecordEntity, BalanceUpdate>
    {
        private readonly IUserWalletsMapper _userWalletsMapper;

        public BalanceUpdatesChecker(
            string sqlConnecctionString,
            IUserWalletsMapper userWalletsMapper,
            ITableEntityRepository<ClientBalanceChangeLogRecordEntity> repository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _userWalletsMapper = userWalletsMapper;
        }

        protected override async Task<List<BalanceUpdate>> ConvertItemsToSqlTypesAsync(IEnumerable<ClientBalanceChangeLogRecordEntity> items)
        {
            var result = items
                .GroupBy(m => m.TransactionId)
                .Where(g => g.Key != null)
                .Select(g => BalanceUpdate.FromModel(g))
                .ToList();

            await _userWalletsMapper.AddWalletsAsync(items.Select(i => i.ClientId).ToHashSet());

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
            foreach (var child in convertedItem.Balances)
            {
                var fromDb = clientBalanceUpdates
                    .Where(c =>
                        c.ClientId == child.ClientId
                        && c.Asset == child.Asset
                        && c.NewBalance == child.NewBalance
                        && c.NewReserved == child.NewReserved)
                    .FirstOrDefault();
                if (fromDb != null)
                    continue;

                if (!child.IsValid())
                    await _log.WriteWarningAsync(nameof(UpdateItemAsync), Name, $"Found invalid child object - {child.ToJson()}!");
                context.ClientBalanceUpdates.Add(child);
                added = true;
                await _log.WriteInfoAsync(nameof(UpdateItemAsync), Name, $"Added update {child.ToJson()} for BalanceUpdate {inSql.Id}");
            }
            return added;
        }
    }
}
