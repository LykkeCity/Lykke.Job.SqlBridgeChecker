using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Models
{
    public class FeedHistoryEntity : TableEntity
    {
        public string Data { get; set; }
    }
}
