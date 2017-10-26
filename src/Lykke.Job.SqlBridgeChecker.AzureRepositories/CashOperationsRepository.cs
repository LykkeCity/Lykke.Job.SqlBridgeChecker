using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class CashOperationsRepository : ItemsRepositoryBase<CashInOutOperationEntity>
    {
        public CashOperationsRepository(INoSQLTableStorage<CashInOutOperationEntity> storage)
            : base(storage)
        {
        }

        protected override string GetAdditionalConditions()
        {
            return null;
        }

        protected override string GetDateColumn()
        {
            return nameof(CashInOutOperationEntity.DateTime);
        }
    }
}
