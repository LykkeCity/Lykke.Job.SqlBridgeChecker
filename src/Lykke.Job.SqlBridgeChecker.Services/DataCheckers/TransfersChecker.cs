using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class TransfersChecker : DataCheckerBase<TransferEventEntity, CashTransferOperation>
    {
        public TransfersChecker(
            string sqlConnecctionString,
            ITableEntityRepository<TransferEventEntity> repository,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
        }

        protected override Task<List<CashTransferOperation>> ConvertItemsToSqlTypesAsync(IEnumerable<TransferEventEntity> items)
        {
            var result = new List<CashTransferOperation>();
            var toConvert = items
                .Where(i => !i.IsHidden)
                .GroupBy(m => m.TransactionId ?? m.RowKey);
            foreach (var group in toConvert)
            {
                var converted = CashTransferOperation.FromModel(group, _log);
                result.Add(converted);
            }

            return Task.FromResult(result);
        }
    }
}
