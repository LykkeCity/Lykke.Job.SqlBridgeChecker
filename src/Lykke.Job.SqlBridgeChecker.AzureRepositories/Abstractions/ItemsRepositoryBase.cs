using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorage;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions
{
    public abstract class ItemsRepositoryBase<T> : ITableEntityRepository<T>
        where T : TableEntity, new()
    {
        protected readonly INoSQLTableStorage<T> _storage;

        public ItemsRepositoryBase(INoSQLTableStorage<T> storage)
        {
            _storage = storage;
        }

        public async Task<List<T>> GetItemsFromYesterdayAsync(DateTime start)
        {
            string queryText = GetQueryText(start);
            var query = new TableQuery<T>().Where(queryText);
            var items = await _storage.WhereAsync(query);
            return items.ToList();
        }

        protected virtual string GetQueryText(DateTime start)
        {
            var to = start;
            var from = to.AddDays(-1);
            string dateColumn = GetDateColumn();
            string fromFilter = TableQuery.GenerateFilterConditionForDate(dateColumn, QueryComparisons.LessThan, to);
            string toFilter = TableQuery.GenerateFilterConditionForDate(dateColumn, QueryComparisons.GreaterThanOrEqual, from);
            string filter = TableQuery.CombineFilters(fromFilter, TableOperators.And, toFilter);
            string otherConditions = GetAdditionalConditions(from, to);
            if (!string.IsNullOrWhiteSpace(otherConditions))
                filter = TableQuery.CombineFilters(otherConditions, TableOperators.And, filter);
            return filter;
        }

        abstract protected string GetDateColumn();

        virtual protected string GetAdditionalConditions(DateTime from, DateTime to)
        {
            return null;
        }
    }
}
