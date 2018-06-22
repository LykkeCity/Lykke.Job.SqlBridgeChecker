using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.SqlBridgeChecker
{
    public class AppSettings
    {
        public SqlBridgeCheckerSettings SqlBridgeCheckerJob { get; set; }

        public SlackNotificationsSettings SlackNotifications { get; set; }

        public ClientAccountClientSettings ClientAccountServiceClient { get; set; }

        public AssetServiceClientSettings AssetsServiceClient { get; set; }
    }

    public class ClientAccountClientSettings
    {
        [HttpCheck("api/isalive")]
        public string ServiceUrl { get; set; }
    }

    public class SlackNotificationsSettings
    {
        public AzureQueuePublicationSettings AzureQueue { get; set; }
    }

    public class AzureQueuePublicationSettings
    {
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }

    public class AssetServiceClientSettings
    {
        [HttpCheck("api/isalive")]
        public string ServiceUrl { get; set; }
    }

    public class SqlBridgeCheckerSettings
    {
        [AzureTableCheck]
        public string LogsConnString { get; set; }

        [AzureTableCheck]
        public string ClientPersonalInfoConnString { get; set; }

        [AzureTableCheck]
        public string BalanceLogWriterMainConnection { get; set; }

        [AzureTableCheck]
        public string HMarketOrdersConnString { get; set; }

        [AzureTableCheck]
        public string HLiquidityConnString { get; set; }

        [AzureTableCheck]
        public string HTradesConnString { get; set; }

        [SqlCheck]
        public string SqlDbConnectionString { get; set; }
    }
}
