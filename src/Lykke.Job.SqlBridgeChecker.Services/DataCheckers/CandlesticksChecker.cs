﻿using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class CandlesticksChecker : DataCheckerBase<FeedHistoryEntity, Candlestick>
    {
        private readonly Dictionary<string, bool> _missingPairs = new Dictionary<string, bool>();

        public CandlesticksChecker(
            string sqlConnectionString,
            ITableEntityRepository<FeedHistoryEntity> repository,
            ILog log)
            : base(sqlConnectionString, repository, log)
        {
        }

        protected override void ClearCaches()
        {
            CandlestickSqlFinder.ClearCache();
            _missingPairs.Clear();
        }

        protected override Task<List<Candlestick>> ConvertItemsToSqlTypesAsync(IEnumerable<FeedHistoryEntity> items)
        {
            var result = items
                .GroupBy(m =>
                    new
                    {
                        m.RowKey,
                        m.PartitionKey,
                    })
                .Select(g => Candlestick.FromModel(g, _log))
                .Where(c => c != null)
                .ToList();
            return Task.FromResult(result);
        }

        protected override async Task<Candlestick> FindInSqlDbAsync(Candlestick item, DataContext context)
        {
            var inSql = await CandlestickSqlFinder.FindInDbAsync(item, context, _log);
            string key = $"{item.AssetPair}_{item.IsAsk}";
            if (inSql == null)
            {
                if (!_missingPairs.ContainsKey(key))
                {
                    _missingPairs.Add(key, true);
                    await _log.WriteInfoAsync(nameof(FindInSqlDbAsync), key, $"{item.ToJson()}");
                }
            }
            else if (_missingPairs.ContainsKey(key) && _missingPairs[key])
            {
                _missingPairs[key] = false;
            }
            return inSql;
        }

        protected override async Task<bool> UpdateItemAsync(Candlestick inSql, Candlestick convertedItem, DataContext context)
        {
            var changed = inSql.Start > convertedItem.Start
                || inSql.High < convertedItem.High
                || inSql.Low > convertedItem.Low;
            if (!changed)
                return false;
            await _log.WriteInfoAsync(nameof(UpdateItemAsync), convertedItem.AssetPair, $"{inSql.ToJson()}");
            if (inSql.Start > convertedItem.Start)
            {
                inSql.Start = convertedItem.Start;
                inSql.Open = convertedItem.Open;
            }
            if (inSql.High < convertedItem.High)
                inSql.High = convertedItem.High;
            if (inSql.Low > convertedItem.Low)
                inSql.Low = convertedItem.Low;
            return true;
        }

        protected override async Task LogAddedAsync(int addedCount)
        {
            await _log.WriteWarningAsync(nameof(CheckAndFixDataAsync), "TotalAdded", $"Added {addedCount} items.");
            if (_missingPairs.Count > 0)
            {
                string totallyMissingPairs = string.Join(",", _missingPairs.Where(p => p.Value).Select(p => p.Key).OrderBy(i => i));
                if (!string.IsNullOrEmpty(totallyMissingPairs))
                    await _log.WriteWarningAsync(nameof(CheckAndFixDataAsync), "WholeDayMissing", $"Whole day missing {totallyMissingPairs}.");
                string partiallyMissingPairs = string.Join(",", _missingPairs.Where(p => !p.Value).Select(p => p.Key).OrderBy(i => i));
                if (!string.IsNullOrEmpty(partiallyMissingPairs))
                    await _log.WriteWarningAsync(nameof(CheckAndFixDataAsync), "PartiallyMissing", $"Partially missing {partiallyMissingPairs}.");
            }
        }
    }
}
