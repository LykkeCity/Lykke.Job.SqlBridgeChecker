using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public static class CandlestickSqlFinder
    {
        private const string _format = "yyyy-MM-dd";
        private static DateTime _cacheDate = DateTime.MinValue;
        private static Dictionary<string, List<Candlestick>> _dict;

        public static async Task<Candlestick> FindInDbAsync(Candlestick item, DataContext context, ILog log)
        {
            if (_dict == null
                || _dict.Count == 0 && item.Start.Date != _cacheDate
                || _dict.Count > 0 && _dict.First().Value.First().Start.Date != item.Start.Date
                || !_dict.ContainsKey(item.AssetPair))
                await FillAssetPairCacheAsync(item, context, log);

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

        private static async Task FillAssetPairCacheAsync(Candlestick item, DataContext context, ILog log)
        {
            context.Database.SetCommandTimeout(TimeSpan.FromMinutes(15));

            DateTime from = item.Start.Date;
            DateTime to = from.AddDays(1);
            string query = $"SELECT * FROM dbo.{DataContext.CandlesticksTable} WHERE AssetPair = '{item.AssetPair}' AND Start >= '{from.ToString(_format)}' AND Start < '{to.ToString(_format)}'";
            var items = context.Candlesticks.FromSql(query).AsNoTracking().ToList();
            if (_dict == null)
                _dict = new Dictionary<string, List<Candlestick>>();
            _dict[item.AssetPair] = items;
            _cacheDate = from;
            await log.WriteInfoAsync(
                nameof(FillAssetPairCacheAsync),
                nameof(CandlestickSqlFinder),
                $"Cached {items.Count} items from sql for {item.AssetPair} on {from.ToString(_format)}.");
        }
    }
}
