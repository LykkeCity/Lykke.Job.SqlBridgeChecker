using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class TransfersRepository : ItemsRepositoryBase<TransferEventEntity>
    {
        public TransfersRepository(INoSQLTableStorage<TransferEventEntity> storage)
            : base(storage)
        {
        }

        protected override string GetAdditionalConditions()
        {
            return null;
        }

        protected override string GetDateColumn()
        {
            return nameof(TransferEventEntity.DateTime);
        }
    }
}
