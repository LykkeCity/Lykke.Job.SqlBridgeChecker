using System;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class LimitTradeInfo : IValidatable
    {
        public static int MaxStringFieldsLength => 255;

        public long Id { get; set; }

        public string LimitOrderId { get; set; }

        public string ClientId { get; set; }

        public string Asset { get; set; }

        public double Volume { get; set; }

        public double Price { get; set; }

        public DateTime Timestamp { get; set; }

        public string OppositeOrderId { get; set; }

        public string OppositeOrderExternalId { get; set; }

        public string OppositeAsset { get; set; }

        public string OppositeClientId { get; set; }

        public double OppositeVolume { get; set; }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(ClientId) && ClientId.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(LimitOrderId) && LimitOrderId.Length <= MaxStringFieldsLength
                && Volume != 0
                && Price > 0
                && !string.IsNullOrEmpty(Asset) && Asset.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(OppositeClientId) && OppositeClientId.Length <= MaxStringFieldsLength
                && OppositeVolume != 0
                && !string.IsNullOrEmpty(OppositeAsset) && OppositeAsset.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(OppositeOrderId) && OppositeOrderId.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(OppositeOrderExternalId) && OppositeOrderExternalId.Length <= MaxStringFieldsLength;
        }
    }
}
