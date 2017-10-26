using System;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class LimitTradeInfo : IValidatable
    {
        public static int MaxStringFieldsLength { get { return 255; } }

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
            return ClientId != null && ClientId.Length <= MaxStringFieldsLength
                && LimitOrderId != null && LimitOrderId.Length <= MaxStringFieldsLength
                && Volume != 0
                && Price > 0
                && Asset != null && Asset.Length <= MaxStringFieldsLength
                && OppositeClientId != null && OppositeClientId.Length <= MaxStringFieldsLength
                && OppositeVolume != 0
                && OppositeAsset != null && OppositeAsset.Length <= MaxStringFieldsLength
                && OppositeOrderId != null && OppositeOrderId.Length <= MaxStringFieldsLength
                && OppositeOrderExternalId != null && OppositeOrderExternalId.Length <= MaxStringFieldsLength;
        }
    }
}
