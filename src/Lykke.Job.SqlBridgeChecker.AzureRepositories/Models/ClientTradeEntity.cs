using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Models
{
    public class ClientTradeEntity : TableEntity
    {
        public string Id { get; set; }
        public DateTime DateTime { get; set; }
        public bool IsHidden { get; set; }
        public string LimitOrderId { get; set; }
        public string OppositeLimitOrderId { get; set; }
        public string MarketOrderId { get; set; }
        public double Price { get; set; }
        public int Confirmations { get; set; }
        public string AssetId { get; set; }
        public string Multisig { get; set; }
        public string AddressFrom { get; set; }
        public string AddressTo { get; set; }
        public bool? IsSettled { get; set; }
        public string StateField { get; set; }
        public double Volume { get; set; }
        public string ClientId { get; set; }
        public bool? IsLimitOrderResult { get; set; }

        public static class ByClientId
        {
            public static string GeneratePartitionKey(string clientId)
            {
                return clientId;
            }

            public static string GenerateRowKey(string tradeId)
            {
                return tradeId;
            }
        }

        public static class ByMultisig
        {
            public static string GeneratePartitionKey(string multisig)
            {
                return multisig;
            }

            public static string GenerateRowKey(string tradeId)
            {
                return tradeId;
            }
        }

        public static class ByDt
        {
            public static string GeneratePartitionKey()
            {
                return "dt";
            }

            public static string GenerateRowKey(string tradeId)
            {
                return tradeId;
            }

            public static string GetRowKeyPart(DateTime dt)
            {
                //ME rowkey format e.g. 20160812180446244_00130
                return $"{dt.Year}{dt.Month.ToString("00")}{dt.Day.ToString("00")}{dt.Hour.ToString("00")}{dt.Minute.ToString("00")}";
            }
        }

        public static class ByOrder
        {
            public static string GeneratePartitionKey(string orderId)
            {
                return orderId;
            }

            public static string GenerateRowKey(string tradeId)
            {
                return tradeId;
            }
        }
    }
}
