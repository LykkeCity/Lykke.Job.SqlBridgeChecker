using System;
using System.Linq;
using System.Collections.Generic;
using Common;
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
            return !string.IsNullOrEmpty(Id) && Id.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(FromClientId) && FromClientId.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(ToClientId) && ToClientId.Length <= MaxStringFieldsLength
                && FromClientId != ToClientId
                && !string.IsNullOrEmpty(Asset) && Asset.Length <= MaxStringFieldsLength
                && Volume != 0;
        }

        public object GetEntityId()
        {
            return Id;
        }

        public static CashTransferOperation FromModel(IEnumerable<TransferEventEntity> models, ILog log)
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
            {
                result.ToClientId = buy.ClientId;
            }
            else
            {
                buy = models.FirstOrDefault(m => m.Amount == 0);
                if (buy == null)
                {
                    log.WriteWarning(
                        "CashTransferOperation.FromModelAsync",
                        models.ToList().ToJson(),
                        $"Buy part is not found for client {first.ClientId} transfer {result.Id}");
                    result.ToClientId = "N/A";
                }
                else
                    result.ToClientId = buy.ClientId;
            }
            var sell = models.FirstOrDefault(m => m.Amount < 0);
            if (sell != null)
            {
                result.FromClientId = sell.ClientId;
            }
            else
            {
                sell = models.FirstOrDefault(m => m.Amount == 0 && m != buy);
                if (sell == null)
                {
                    log.WriteWarning(
                        "CashTransferOperation.FromModelAsync",
                        models.ToList().ToJson(),
                        $"Sell part is not found for client {first.ClientId} transfer {result.Id}");
                    result.FromClientId = "N/A";
                }
                else
                    result.FromClientId = sell.ClientId;
            }
            return result;
        }
    }
}
