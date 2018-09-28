using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Models
{
    public class LimitOrderEntity : TableEntity
    {
        public DateTime CreatedAt { get; set; }

        public double Price { get; set; }
        public string AssetPairId { get; set; }

        public double Volume { get; set; }

        public string Status { get; set; }
        public bool Straight { get; set; }
        public string Id { get; set; }
        public string ClientId { get; set; }

        public double RemainingVolume { get; set; }
        public string MatchingId { get; set; }

        public static class ByClientId
        {
            public static string GeneratePartitionKey(string clientId)
            {
                return clientId;
            }

            public static string GenerateRowKey(string orderId)
            {
                return orderId;
            }
        }

        public static class ByDate
        {
            public static string GeneratePartitionKey(DateTime date)
            {
                return date.ToString("yyyy-MM-dd");
            }

            public static string GenerateRowKey(string orderId)
            {
                return orderId;
            }
        }
    }
}
