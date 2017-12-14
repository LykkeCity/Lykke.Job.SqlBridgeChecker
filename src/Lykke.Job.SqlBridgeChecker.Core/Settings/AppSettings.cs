namespace Lykke.Job.SqlBridgeChecker.Core.Settings
{
    public class AppSettings
    {
        public SqlBridgeCheckerSettings SqlBridgeCheckerJob { get; set; }

        public SlackNotificationsSettings SlackNotifications { get; set; }

        public ClientAccountClientSettings ClientAccountClient { get; set; }
    }

    public class ClientAccountClientSettings
    {
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

    public class SqlBridgeCheckerSettings
    {
        public string LogsConnString { get; set; }

        public string ClientPersonalInfoConnString { get; set; }

        public string BalanceLogWriterMainConnection { get; set; }

        public string HMarketOrdersConnString { get; set; }

        public string HLiquidityConnString { get; set; }

        public string HTradesConnString { get; set; }

        public string SqlDbConnectionString { get; set; }
    }
}
