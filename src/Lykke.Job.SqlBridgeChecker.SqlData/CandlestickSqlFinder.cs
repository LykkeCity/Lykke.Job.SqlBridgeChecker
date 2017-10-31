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

        private static List<Candlestick> _cache = new List<Candlestick>();
        private static string _assetPair;
        private static DateTime _date;

        public static Candlestick FindInDb(Candlestick item, DataContext context)
        {
            if (item.AssetPair != _assetPair || item.Start.Date != _date)
                UpdateCache(item, context);

            var fromDb = _cache.FirstOrDefault(c => c.IsAsk == item.IsAsk && c.Start == item.Start);
            return fromDb;
        }

        private static void UpdateCache(Candlestick item, DataContext context)
        {
            DateTime from = item.Start.Date;
            DateTime to = from.AddDays(1);
            string query = $"SELECT * FROM dbo.Candlesticks2 WHERE AssetPair = '{item.AssetPair}' AND Start BETWEEN '{from.ToString(_format)}' AND '{to.ToString(_format)}'";
            _cache = context.Candlesticks.FromSql(query).ToList();
            _assetPair = item.AssetPair;
            _date = item.Start.Date;
        }
    }
}
