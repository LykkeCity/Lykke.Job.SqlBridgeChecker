using System;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorage;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class CandlestiсksRepository : ItemsRepositoryBase<FeedHistoryEntity>
    {
        public CandlestiсksRepository(INoSQLTableStorage<FeedHistoryEntity> storage)
            : base(storage)
        {
        }

        protected override string GetQueryText(DateTime start)
        {
            var today = start;
            var yesterday = today.AddDays(-1);
            string startStr = GenerateRowKey(yesterday);
            string finishStr = GenerateRowKey(today);
            string dateColumn = GetDateColumn();
            string dateFinishFilter = TableQuery.GenerateFilterCondition(dateColumn, QueryComparisons.LessThan, finishStr);
            string dateStartFilter = TableQuery.GenerateFilterCondition(dateColumn, QueryComparisons.GreaterThanOrEqual, startStr);
            string dateFilter = TableQuery.CombineFilters(dateFinishFilter, TableOperators.And, dateStartFilter);
            return dateFilter;
        }

        protected override string GetDateColumn()
        {
            return "RowKey";
        }

        private static string GenerateRowKey(DateTime feedTime)
        {
            return $"{feedTime.Year}{feedTime.Month.ToString("00")}{feedTime.Day.ToString("00")}{feedTime.Hour.ToString("00")}{feedTime.Minute.ToString("00")}";
        }
    }
}
