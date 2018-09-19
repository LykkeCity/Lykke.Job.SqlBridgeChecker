using System;
using System.Linq;
using System.Collections.Generic;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class CashOperation : IValidatable, IDbEntity
    {
        public static int MaxStringFieldsLength => 255;

        public string Id { get; set; }

        public string ClientId { get; set; }

        public double Volume { get; set; }

        public string Asset { get; set; }

        public DateTime DateTime { get; set; }

        public object GetEntityId()
        {
            return Id;
        }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(Id) && Id.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(ClientId) && ClientId.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(Asset) && Asset.Length <= MaxStringFieldsLength
                && Volume != 0;
        }

        public static CashOperation FromModel(IEnumerable<CashInOutOperationEntity> models, ILog log)
        {
            var model = models.First();
            var otherAssetModel = models.FirstOrDefault(m => m.AssetId != model.AssetId);
            if (otherAssetModel != null)
                log.WriteWarning(
                    nameof(CashOperation),
                    nameof(FromModel),
                    $"For tx {model.TransactionId} found 2 assets - '{model.AssetId}' and '{otherAssetModel.AssetId}'");
            return new CashOperation
            {
                Id = model.TransactionId ?? model.RowKey,
                ClientId = model.ClientId,
                Volume = model.Amount,
                Asset = model.AssetId,
                DateTime = model.DateTime
            };
        }
    }
}
