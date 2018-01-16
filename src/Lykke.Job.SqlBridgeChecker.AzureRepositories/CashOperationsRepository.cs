using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class CashOperationsRepository : ItemsRepositoryBase<CashInOutOperationEntity>
    {
        public CashOperationsRepository(INoSQLTableStorage<CashInOutOperationEntity> storage)
            : base(storage)
        {
        }

        protected override string GetDateColumn()
        {
            return nameof(CashInOutOperationEntity.DateTime);
        }
    }
}
