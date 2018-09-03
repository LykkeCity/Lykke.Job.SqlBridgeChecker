using System;
using System.Linq;
using System.Collections.Generic;
using Common;
using Common.Log;
using Dapper;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;
using Microsoft.EntityFrameworkCore;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public static class CandlestickSqlFinder
    {
        private const string _format = "yyyy-MM-dd";
        private static DateTime? _cacheDate;
        private static Dictionary<string, List<Candlestick>> _dict;

        public static Candlestick FindInDb(Candlestick item, DataContext context, ILog log)
        {
            if (_dict == null
                || !_cacheDate.HasValue
                || item.Start.Date != _cacheDate.Value
                || !_dict.ContainsKey(item.AssetPair))
                FillAssetPairCache(item, context, log);

            if (!_dict.ContainsKey(item.AssetPair))
                return null;

            var roundedFinish = item.Finish.RoundToMinute();
            var fromDb = _dict[item.AssetPair].FirstOrDefault(c => c.IsAsk == item.IsAsk && c.Finish.RoundToMinute() == roundedFinish);
            return fromDb;
        }

        public static void ClearCache()
        {
            _dict?.Clear();
        }

        private static void FillAssetPairCache(Candlestick item, DataContext context, ILog log)
        {
            DateTime from = item.Start.Date;
            DateTime to = from.AddDays(1);
            string query = $"SELECT * FROM dbo.{DataContext.CandlesticksTable} WHERE AssetPair = '{item.AssetPair}' AND Start >= '{from.ToString(_format)}' AND Start < '{to.ToString(_format)}'";
            var items = context.Database.GetDbConnection().Query<Candlestick>(query).ToList();
            if (_dict == null)
                _dict = new Dictionary<string, List<Candlestick>>();
            _dict[item.AssetPair] = items;
            _cacheDate = from;
            log.WriteInfo(
                nameof(FillAssetPairCache),
                nameof(CandlestickSqlFinder),
                $"Cached {items.Count} items from sql for {item.AssetPair} on {from.ToString(_format)}.");
        }
    }
}
