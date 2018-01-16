using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class TransfersRepository : ItemsRepositoryBase<TransferEventEntity>
    {
        public TransfersRepository(INoSQLTableStorage<TransferEventEntity> storage)
            : base(storage)
        {
        }

        protected override string GetDateColumn()
        {
            return nameof(TransferEventEntity.DateTime);
        }
    }
}
