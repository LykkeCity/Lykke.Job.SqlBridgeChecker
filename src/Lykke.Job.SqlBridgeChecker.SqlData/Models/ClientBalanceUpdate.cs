using System;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class ClientBalanceUpdate : IValidatable
    {
        public static int MaxStringFieldsLength => 255;

        public long Id { get; set; }

        public string ClientId { get; set; }

        public string BalanceUpdateId { get; set; }

        public string Asset { get; set; }

        public double OldBalance { get; set; }

        public double NewBalance { get; set; }

        public double? OldReserved { get; set; }

        public double? NewReserved { get; set; }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(ClientId) && ClientId.Length <= MaxStringFieldsLength && Guid.TryParse(ClientId, out _)
                && !string.IsNullOrEmpty(Asset) && Asset.Length <= MaxStringFieldsLength
                && (OldBalance != NewBalance || OldReserved != NewReserved);
        }
    }
}
