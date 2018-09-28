using System;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class TradeInfo : IValidatable
    {
        public static int MaxStringFieldsLength => 255;

        public long Id { get; set; }

        public string MarketOrderId { get; set; }

        public string MarketClientId { get; set; }

        public double MarketVolume { get; set; }

        public string MarketAsset { get; set; }

        public double Price { get; set; }

        public string LimitOrderId { get; set; }

        public string LimitClientId { get; set; }

        public double LimitVolume { get; set; }

        public string LimitAsset { get; set; }

        public string LimitOrderExternalId { get; set; }

        public DateTime Timestamp { get; set; }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(MarketClientId) && MarketClientId.Length <= MaxStringFieldsLength && Guid.TryParse(MarketClientId, out _)
                && MarketVolume != 0
                && !string.IsNullOrEmpty(MarketAsset) && MarketAsset.Length <= MaxStringFieldsLength
                && Price > 0
                && !string.IsNullOrEmpty(LimitClientId) && LimitClientId.Length <= MaxStringFieldsLength && Guid.TryParse(LimitClientId, out _)
                && LimitVolume != 0
                && !string.IsNullOrEmpty(LimitAsset) && LimitAsset.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(LimitOrderId) && LimitOrderId.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(LimitOrderExternalId) && LimitOrderExternalId.Length <= MaxStringFieldsLength;
        }
    }
}
