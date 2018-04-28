using System;
using System.Linq;
using System.Collections.Generic;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class BalanceUpdate : IDbEntity, IValidatable
    {
        public static int MaxStringFieldsLength { get { return 255; } }

        public string Id { get; set; }

        public string Type { get; set; }

        public DateTime Timestamp { get; set; }

        public List<ClientBalanceUpdate> Balances { get; set; }

        public object GetEntityId()
        {
            return Id;
        }

        public bool IsValid()
        {
            if (string.IsNullOrEmpty(Id) || Id.Length > MaxStringFieldsLength
                || string.IsNullOrEmpty(Type) || Type.Length > MaxStringFieldsLength
                || Balances == null || Balances.Count == 0)
                return false;

            foreach (var balance in Balances)
            {
                if (!balance.IsValid())
                    return false;
            }

            return true;
        }

        public static BalanceUpdate FromModel(IEnumerable<ClientBalanceChangeLogRecordEntity> models)
        {
            var first = models.First();
            var result = new BalanceUpdate
            {
                Id = first.TransactionId,
                Type = first.TransactionType,
            };
            if (int.TryParse(first.TransactionId, out _))
                result.Id = Guid.NewGuid().ToString();
            result.Timestamp = first.TransactionTimestamp;
            result.Balances = models
                .Select(m =>
                    new ClientBalanceUpdate
                    {
                        ClientId = m.ClientId,
                        BalanceUpdateId = first.TransactionId,
                        Asset = m.Asset,
                        OldBalance = m.OldBalance,
                        NewBalance = m.NewBalance,
                        OldReserved= m.OldReserved,
                        NewReserved = m.NewReserved,
                    })
                .ToList();
            return result;
        }
    }
}
