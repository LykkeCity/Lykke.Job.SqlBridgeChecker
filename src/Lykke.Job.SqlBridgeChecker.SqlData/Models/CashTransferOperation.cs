using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class CashTransferOperation : IValidatable, IDbEntity
    {
        public static int MaxStringFieldsLength { get { return 255; } }

        public string Id { get; set; }

        public string FromClientId { get; set; }

        public string ToClientId { get; set; }

        public DateTime DateTime { get; set; }

        public double Volume { get; set; }

        public string Asset { get; set; }

        public bool IsValid()
        {
            return Id != null && Id.Length <= MaxStringFieldsLength
                && FromClientId != null && FromClientId.Length <= MaxStringFieldsLength
                && ToClientId != null && ToClientId.Length <= MaxStringFieldsLength
                && FromClientId != ToClientId
                && Asset != null && Asset.Length <= MaxStringFieldsLength
                && Volume != 0;
        }

        public object GetEntityId()
        {
            return Id;
        }

        public static async Task<CashTransferOperation> FromModelAsync(IEnumerable<TransferEventEntity> models, ILog log)
        {
            var first = models.First();
            var result = new CashTransferOperation
            {
                Id = first.TransactionId ?? first.RowKey,
                Asset = first.AssetId,
                Volume = Math.Abs(first.Amount),
            };
            result.DateTime = models.Max(m => m.DateTime);
            var buy = models.FirstOrDefault(m => m.Amount > 0);
            if (buy != null)
                result.ToClientId = buy.ClientId;
            else
            {
                buy = models.FirstOrDefault(m => m.Amount == 0);
                if (buy == null)
                    await log.WriteWarningAsync(
                        nameof(CashTransferOperation),
                        nameof(FromModelAsync),
                        $"Buy part is not found for transfer transaction {result.Id}");
            }
            var sell = models.FirstOrDefault(m => m.Amount < 0);
            if (sell != null)
                result.FromClientId = sell.ClientId;
            else
            {
                sell = models.FirstOrDefault(m => m.Amount == 0 && m != buy);
                if (sell == null)
                    await log.WriteWarningAsync(
                        nameof(CashTransferOperation),
                        nameof(FromModelAsync),
                        $"Sell part is not found for transfer transaction {result.Id}");
            }
            return result;
        }
    }
}
