using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class CashOperationsChecker : DataCheckerBase<CashInOutOperationEntity, CashOperation>
    {
        public CashOperationsChecker(
            string sqlConnecctionString,
            ITableEntityRepository<CashInOutOperationEntity> repository,
            ILog log)
            : base(
                sqlConnecctionString,
                false,
                repository,
                log)
        {
        }

        protected override Task<List<CashOperation>> ConvertItemsToSqlTypesAsync(IEnumerable<CashInOutOperationEntity> items)
        {
            var result = items
                .Where(i => !i.IsHidden)
                .GroupBy(i => i.TransactionId ?? i.RowKey)
                .Select(group => CashOperation.FromModel(group, _log))
                .ToList();

            return Task.FromResult(result);
        }
    }
}
