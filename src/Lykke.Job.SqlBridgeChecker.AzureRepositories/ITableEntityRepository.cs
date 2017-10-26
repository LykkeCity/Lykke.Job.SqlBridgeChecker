using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public interface ITableEntityRepository<T> where T : TableEntity
    {
        Task<List<T>> GetItemsFromYesterdayAsync();
    }
}
