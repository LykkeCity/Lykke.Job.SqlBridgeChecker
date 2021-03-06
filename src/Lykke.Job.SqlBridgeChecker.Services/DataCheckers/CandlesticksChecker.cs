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
            : base(
                  sqlConnectionString,
                  true,
                  repository,
                  log)
        {
        }

        protected override void ClearCaches(bool isDuringProcessing)
        {
            CandlestickSqlFinder.ClearCache();
            if (!isDuringProcessing)
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

        protected override Task<Candlestick> FindInSqlDbAsync(Candlestick item, DataContext context)
        {
            var inSql = CandlestickSqlFinder.FindInDb(item, context, _log);
            string key = $"{item.AssetPair}_{item.IsAsk}";
            if (inSql == null)
            {
                if (!_missingPairs.ContainsKey(key))
                {
                    _missingPairs.Add(key, true);
                    _log.WriteInfo(nameof(FindInSqlDbAsync), key, item.ToJson());
                }
            }
            else if (_missingPairs.ContainsKey(key) && _missingPairs[key])
            {
                _missingPairs[key] = false;
            }
            return Task.FromResult(inSql);
        }

        protected override bool UpdateItem(Candlestick fromSql, Candlestick convertedItem, DataContext context)
        {
            var changed = fromSql.Start > convertedItem.Start
                || convertedItem.High > 0 && fromSql.High < convertedItem.High
                || convertedItem.Low > 0 && fromSql.Low > convertedItem.Low;
            if (!changed)
                return false;
            _log.WriteInfo(nameof(UpdateItem), convertedItem.AssetPair, fromSql.ToJson());
            if (fromSql.Start > convertedItem.Start)
            {
                fromSql.Start = convertedItem.Start;
                if (convertedItem.Open > 0)
                    fromSql.Open = convertedItem.Open;
            }
            if (convertedItem.High > 0 && fromSql.High < convertedItem.High)
                fromSql.High = convertedItem.High;
            if (convertedItem.Low > 0 && fromSql.Low > convertedItem.Low)
                fromSql.Low = convertedItem.Low;
            return true;
        }

        protected override void LogAdded(int addedCount)
        {
            _log.WriteWarning(nameof(CheckAndFixDataAsync), "TotalAdded", $"Added {addedCount} items.");
            if (_missingPairs.Count <= 0)
                return;

            string totallyMissingPairs = string.Join(",", _missingPairs.Where(p => p.Value).Select(p => p.Key).OrderBy(i => i));
            if (!string.IsNullOrEmpty(totallyMissingPairs))
                _log.WriteWarning(nameof(CheckAndFixDataAsync), "WholeDayMissing", $"Whole day missing {totallyMissingPairs}.");
            string partiallyMissingPairs = string.Join(",", _missingPairs.Where(p => !p.Value).Select(p => p.Key).OrderBy(i => i));
            if (!string.IsNullOrEmpty(partiallyMissingPairs))
                _log.WriteWarning(nameof(CheckAndFixDataAsync), "PartiallyMissing", $"Partially missing {partiallyMissingPairs}.");
        }
    }
}
