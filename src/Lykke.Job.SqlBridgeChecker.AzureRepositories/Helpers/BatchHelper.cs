using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Common.Log;
using AzureStorage;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Helpers
{
    internal static class BatchHelper
    {
        private const string _partitionKey = "PartitionKey";

        internal static async Task<List<T>> BatchGetDataAsync<T>(
            string partitionValue,
            string columnName,
            IEnumerable<string> columnValues,
            INoSQLTableStorage<T> storage,
            ILog log)
            where T : class, ITableEntity, new()
        {
            var result = new List<T>();
            int maxCount = string.IsNullOrWhiteSpace(partitionValue) && columnName == _partitionKey ? 1 : 75;

            int count = 0;
            int allCount = 0;
            string filter = null;
            foreach (var valiue in columnValues)
            {
                if (string.IsNullOrWhiteSpace(valiue))
                    continue;

                string columnFilter = TableQuery.GenerateFilterCondition(columnName, QueryComparisons.Equal, valiue);
                if (filter == null)
                    filter = columnFilter;
                else
                    filter = TableQuery.CombineFilters(filter, TableOperators.Or, columnFilter);
                ++count;
                ++allCount;
                if (count >= maxCount)
                {
                    await FetchDataAsync(
                        partitionValue,
                        filter,
                        allCount,
                        storage,
                        result,
                        log);
                    count = 0;
                    filter = null;
                }
            }
            if (count > 0)
                await FetchDataAsync(
                    partitionValue,
                    filter,
                    allCount,
                    storage,
                    result,
                    log);

            return result;
        }

        private static async Task FetchDataAsync<T>(
            string partitionValue,
            string filter,
            int allCount,
            INoSQLTableStorage<T> storage,
            List<T> items,
            ILog log)
            where T : class, ITableEntity, new()
        {
            if (!string.IsNullOrWhiteSpace(partitionValue))
            {
                string partitionFilter = TableQuery.GenerateFilterCondition(_partitionKey, QueryComparisons.Equal, partitionValue);
                filter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, filter);
            }
            var query = new TableQuery<T>().Where(filter);
            int countBefore = items.Count;
            var data = await storage.WhereAsync(query);
            items.AddRange(data);
            /*if (items.Count - countBefore > 0)
                log.WriteInfo(
                    nameof(BatchHelper),
                    nameof(BatchGetDataAsync),
                    $"Fetched batch of {items.Count - countBefore} items. Processed {allCount}.");*/
        }
    }
}
