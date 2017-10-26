using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorage;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public abstract class ItemsRepositoryBase<T> : ITableEntityRepository<T>
        where T : TableEntity, new()
    {
        protected readonly INoSQLTableStorage<T> _storage;

        public ItemsRepositoryBase(INoSQLTableStorage<T> storage)
        {
            _storage = storage;
        }

        public async Task<List<T>> GetItemsFromYesterdayAsync()
        {
            string queryText = GetQueryText();
            var query = new TableQuery<T>().Where(queryText);
            var items = await _storage.WhereAsync(query);
            return items.ToList();
        }

        protected virtual string GetQueryText()
        {
            var today = DateTime.UtcNow.Date.AddDays(-1);
            var yesterday = today.AddDays(-1);
            string dateColumn = GetDateColumn();
            string fromFilter = TableQuery.GenerateFilterConditionForDate(dateColumn, QueryComparisons.LessThan, today);
            string toFilter = TableQuery.GenerateFilterConditionForDate(dateColumn, QueryComparisons.GreaterThanOrEqual, yesterday);
            string filter = TableQuery.CombineFilters(fromFilter, TableOperators.And, toFilter);
            string otherConditions = GetAdditionalConditions();
            if (!string.IsNullOrWhiteSpace(otherConditions))
                filter = TableQuery.CombineFilters(otherConditions, TableOperators.And, filter);
            return filter;
        }

        abstract protected string GetDateColumn();

        abstract protected string GetAdditionalConditions();
    }
}
