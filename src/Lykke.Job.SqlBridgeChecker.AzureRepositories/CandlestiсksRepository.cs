using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorage;
using Common;
using Common.Log;
using Lykke.Service.Assets.Client;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;

namespace Lykke.Job.SqlBridgeChecker.AzureRepositories
{
    public class CandlestiсksRepository : ITableEntityRepository<FeedHistoryEntity>
    {
        private const string _partitionKeyColumn = "PartitionKey";
        private const string _rowKeyColumn = "RowKey";

        private readonly INoSQLTableStorage<FeedHistoryEntity> _storage;
        private readonly IAssetsService _assetsService;
        private readonly ILog _log;

        private List<string> _lastAssetPairIds;

        public CandlestiсksRepository(
            INoSQLTableStorage<FeedHistoryEntity> storage,
            IAssetsService assetsService,
            ILog log)
        {
            _storage = storage;
            _assetsService = assetsService;
            _log = log;
        }

        public async Task<List<FeedHistoryEntity>> GetItemsFromYesterdayAsync(DateTime start)
        {
            try
            {
                var assetPairs = await _assetsService.AssetPairGetAllAsync();
                _lastAssetPairIds = assetPairs
                    .Select(i => i.Id)
                    .Where(i => i.IsValidPartitionOrRowKey())
                    .ToList();
            }
            catch (Exception ex)
            {
                _log.WriteWarning(nameof(CandlestiсksRepository), start, ex.Message);
            }

            var result = new List<FeedHistoryEntity>();

            if (_lastAssetPairIds == null || _lastAssetPairIds.Count == 0)
            {
                string queryText = GenerateQueryWithoutPartition(start);
                var query = new TableQuery<FeedHistoryEntity>().Where(queryText);
                var items = await _storage.WhereAsync(query);
                result.AddRange(items);
            }
            else
            {
                foreach (var assetPairId in _lastAssetPairIds)
                {
                    (string pk1, string pk2) = GetPartiotionsForAssetPair(assetPairId);

                    string queryText = GenerateQueryForPartition(start, pk1);
                    var query = new TableQuery<FeedHistoryEntity>().Where(queryText);
                    var items = await _storage.WhereAsync(query);
                    result.AddRange(items);

                    queryText = GenerateQueryForPartition(start, pk2);
                    query = new TableQuery<FeedHistoryEntity>().Where(queryText);
                    items = await _storage.WhereAsync(query);
                    result.AddRange(items);
                }
            }

            return result;
        }

        private string GenerateQueryForPartition(DateTime start, string partitionKey)
        {
            string dateFilter = GenerateQueryWithoutPartition(start);
            string partitionFilter = TableQuery.GenerateFilterCondition(_partitionKeyColumn, QueryComparisons.Equal, partitionKey);
            return TableQuery.CombineFilters(partitionFilter, TableOperators.And, dateFilter);
        }

        private string GenerateQueryWithoutPartition(DateTime start)
        {
            var today = start;
            var yesterday = today.AddDays(-1);
            string startStr = GenerateRowKey(yesterday);
            string finishStr = GenerateRowKey(today);
            string dateFinishFilter = TableQuery.GenerateFilterCondition(_rowKeyColumn, QueryComparisons.LessThan, finishStr);
            string dateStartFilter = TableQuery.GenerateFilterCondition(_rowKeyColumn, QueryComparisons.GreaterThanOrEqual, startStr);
            string dateFilter = TableQuery.CombineFilters(dateFinishFilter, TableOperators.And, dateStartFilter);
            return dateFilter;
        }

        private (string, string) GetPartiotionsForAssetPair(string assetPairId)
        {
            return ($"{assetPairId}_Ask", $"{assetPairId}_Bid");
        }

        private static string GenerateRowKey(DateTime feedTime)
        {
            return $"{feedTime.Year}{feedTime.Month.ToString("00")}{feedTime.Day.ToString("00")}{feedTime.Hour.ToString("00")}{feedTime.Minute.ToString("00")}";
        }
    }
}
