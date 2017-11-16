using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
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
                || _dict.Count > 0 && _dict.First().Value.First().Start.Date != item.Start.Date)
                await InitCacheAsync(item, context, log);

            if (!_dict.ContainsKey(item.AssetPair))
                return null;

            var fromDb = _dict[item.AssetPair].FirstOrDefault(c => c.IsAsk == item.IsAsk && c.Start == item.Start);
            return fromDb;
        }

        private static async Task InitCacheAsync(Candlestick item, DataContext context, ILog log)
        {
            context.Database.SetCommandTimeout(TimeSpan.FromMinutes(10));

            DateTime from = item.Start.Date;
            DateTime to = from.AddDays(1);
            string query = $"SELECT * FROM dbo.Candlesticks2 WHERE Start >= '{from.ToString(_format)}' AND Start < '{to.ToString(_format)}'";
            var items = context.Candlesticks.FromSql(query).ToList();
            _dict = items.GroupBy(i => i.AssetPair).ToDictionary(g => g.Key, g => g.ToList());
            _cacheDate = from;
            await log.WriteInfoAsync(
                nameof(CandlestickSqlFinder),
                nameof(InitCacheAsync),
                $"Cached {items.Count} items from sql for {from.ToString(_format)}.");
        }
    }
}
