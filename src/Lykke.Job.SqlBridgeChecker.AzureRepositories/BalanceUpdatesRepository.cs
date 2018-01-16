using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class BalanceUpdatesRepository : ItemsRepositoryBase<ClientBalanceChangeLogRecordEntity>
    {
        public BalanceUpdatesRepository(INoSQLTableStorage<ClientBalanceChangeLogRecordEntity> storage)
            : base(storage)
        {
        }

        protected override string GetDateColumn()
        {
            return nameof(ClientBalanceChangeLogRecordEntity.TransactionTimestamp);
        }
    }
}
