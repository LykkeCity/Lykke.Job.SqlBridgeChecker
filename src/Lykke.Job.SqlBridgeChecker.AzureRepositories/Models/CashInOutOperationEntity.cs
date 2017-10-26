using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Models
{
    public class CashInOutOperationEntity : TableEntity
    {
        public string Id { get; set; }
        public DateTime DateTime { get; set; }
        public bool IsHidden { get; set; }
        public string AssetId { get; set; }
        public string ClientId { get; set; }
        public double Amount { get; set; }
        public string BlockChainHash { get; set; }
        public string Multisig { get; set; }
        public string TransactionId { get; set; }
        public string AddressFrom { get; set; }
        public string AddressTo { get; set; }
        public bool? IsSettled { get; set; }
        public bool IsRefund { get; set; }

        public static class ByClientId
        {
            public static string GeneratePartitionKey(string clientId)
            {
                return clientId;
            }

            internal static string GenerateRowKey(string id)
            {
                return id;
            }
        }

        public static class ByMultisig
        {
            public static string GeneratePartitionKey(string multisig)
            {
                return multisig;
            }

            internal static string GenerateRowKey(string id)
            {
                return id;
            }
        }
    }
}
