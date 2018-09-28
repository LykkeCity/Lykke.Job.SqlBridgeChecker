using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Models
{
    public class TransferEventEntity : TableEntity
    {
        public DateTime DateTime { get; set; }
        public bool IsHidden { get; set; }
        public string FromId { get; set; }
        public string AssetId { get; set; }
        public double Amount { get; set; }
        public string BlockChainHash { get; set; }
        public string Multisig { get; set; }
        public string TransactionId { get; set; }
        public string AddressFrom { get; set; }
        public string AddressTo { get; set; }
        public bool? IsSettled { get; set; }
        public string StateField { get; set; }
        public string ClientId { get; set; }

        public static class ByClientId
        {
            public static string GeneratePartitionKey(string clientId)
            {
                return clientId;
            }

            public static string GenerateRowKey(string id)
            {
                return id;
            }
        }
    }
}
