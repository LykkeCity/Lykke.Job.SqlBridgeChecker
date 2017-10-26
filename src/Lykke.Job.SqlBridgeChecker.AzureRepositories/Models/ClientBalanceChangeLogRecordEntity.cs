using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Models
{
    public class ClientBalanceChangeLogRecordEntity : TableEntity
    {
        public string ClientId { get; set; }
        public DateTime TransactionTimestamp { get; set; }
        public string TransactionId { get; set; }
        public string TransactionType { get; set; }
        public string Asset { get; set; }
        public double OldBalance { get; set; }
        public double NewBalance { get; set; }
        public double OldReserved { get; set; }
        public double NewReserved { get; set; }
    }
}
