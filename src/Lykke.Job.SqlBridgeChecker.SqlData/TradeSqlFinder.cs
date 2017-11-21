using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public static class TradeSqlFinder
    {
        private const string _format = "yyyy-MM-dd";
        private static DateTime _cacheDate = DateTime.MinValue;
        private static Dictionary<string, List<TradeLogItem>> _dict;

        public static async Task<TradeLogItem> FindInDbAsync(TradeLogItem item, DataContext context, ILog log)
        {
            if (_dict == null
                || _dict.Count == 0 && item.DateTime.Date != _cacheDate
                || _dict.Count > 0 && _dict.First().Value.First().DateTime.Date != item.DateTime.Date)
                await InitCacheAsync(item, context, log);

            if (!_dict.ContainsKey(item.TradeId))
                return null;

            var fromDb = _dict[item.TradeId].FirstOrDefault(c =>
                c.WalletId == item.WalletId
                && c.Asset == item.Asset
                && c.OppositeAsset == item.OppositeAsset);
            return fromDb;
        }

        private static async Task InitCacheAsync(TradeLogItem item, DataContext context, ILog log)
        {
            DateTime from = item.DateTime.Date;
            DateTime to = from.AddDays(1);
            string query = $"SELECT * FROM dbo.{DataContext.TradesTable} WHERE DateTime >= '{from.ToString(_format)}' AND DateTime < '{to.ToString(_format)}'";
            var items = context.Trades.FromSql(query).ToList();
            _dict = items.GroupBy(i => i.TradeId).ToDictionary(g => g.Key, g => g.ToList());
            _cacheDate = from;
            await log.WriteInfoAsync(
                nameof(TradeSqlFinder),
                nameof(InitCacheAsync),
                $"Cached {items.Count} items from sql for {from.ToString(_format)}.");
        }
    }
}
