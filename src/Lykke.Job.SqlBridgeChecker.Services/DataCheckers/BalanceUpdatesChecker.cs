using System;
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

        protected override bool UpdateItem(BalanceUpdate inSql, BalanceUpdate convertedItem, DataContext context)
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
                    .FirstOrDefault(c =>
                        c.ClientId == child.ClientId
                        && c.Asset == child.Asset
                        && Math.Abs(c.NewBalance - child.NewBalance) < 0.00000001
                        && Math.Abs(c.OldBalance - child.OldBalance) < 0.00000001
                        && c.NewReserved == child.NewReserved
                        && c.OldReserved == child.OldReserved);
                if (fromDb != null)
                    continue;

                if (!child.IsValid())
                    _log.WriteWarning(nameof(UpdateItem), "Invalid", $"Found invalid child object - {child.ToJson()}!");
                context.ClientBalanceUpdates.Add(child);
                added = true;
                _log.WriteInfo(nameof(UpdateItem), child.Asset, $"Added update {child.ToJson()} for BalanceUpdate {convertedItem.Id}");
            }
            return added;
        }
    }
}
