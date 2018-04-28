using System;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class CashOperation : IValidatable, IDbEntity
    {
        public static int MaxStringFieldsLength { get { return 255; } }

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

        public static CashOperation FromModel(CashInOutOperationEntity model)
        {
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
