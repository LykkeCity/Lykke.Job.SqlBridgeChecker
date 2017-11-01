using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public static class CandlestickSqlFinder
    {
        private const string _format = "yyyy-MM-dd";

        private static Dictionary<string, List<Candlestick>> _dict;

        public static Candlestick FindInDb(Candlestick item, DataContext context)
        {
            if (_dict == null || _dict.First().Value.First().Start.Date != item.Start.Date)
                InitCache(item, context);

            if (!_dict.ContainsKey(item.AssetPair))
                return null;

            var fromDb = _dict[item.AssetPair].FirstOrDefault(c => c.IsAsk == item.IsAsk && c.Start == item.Start);
            return fromDb;
        }

        private static void InitCache(Candlestick item, DataContext context)
        {
            DateTime from = item.Start.Date;
            DateTime to = from.AddDays(1);
            string query = $"SELECT * FROM dbo.Candlesticks2 WHERE Start BETWEEN '{from.ToString(_format)}' AND '{to.ToString(_format)}'";
            var items = context.Candlesticks.FromSql(query).ToList();
            _dict = items.GroupBy(i => i.AssetPair).ToDictionary(g => g.Key, g => g.ToList());
        }
    }
}
