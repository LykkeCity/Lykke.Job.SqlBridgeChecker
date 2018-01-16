using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class CashOperationsChecker : DataCheckerBase<CashInOutOperationEntity, CashOperation>
    {
        private readonly IUserWalletsMapper _userWalletsMapper;

        public CashOperationsChecker(
            string sqlConnecctionString,
            IUserWalletsMapper userWalletsMapper,
            ITableEntityRepository<CashInOutOperationEntity> repository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _userWalletsMapper = userWalletsMapper;
        }

        protected override async Task<List<CashOperation>> ConvertItemsToSqlTypesAsync(IEnumerable<CashInOutOperationEntity> items)
        {
            var result = items
                .Where(i => !i.IsHidden)
                .GroupBy(i => i.TransactionId ?? i.RowKey)
                .Select(group => CashOperation.FromModel(group.First()))
                .ToList();

            await _userWalletsMapper.AddWalletsAsync(items.Select(i => i.ClientId).ToHashSet());

            return result;
        }
    }
}
