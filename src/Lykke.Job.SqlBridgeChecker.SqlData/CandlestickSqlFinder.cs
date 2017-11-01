﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Microsoft.EntityFrameworkCore;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public static class CandlestickSqlFinder
    {
        private const string _format = "yyyy-MM-dd";

        private static Dictionary<string, List<Candlestick>> _dict;

        public static async Task<Candlestick> FindInDbAsync(Candlestick item, DataContext context, ILog log)
        {
            if (_dict == null || _dict.First().Value.First().Start.Date != item.Start.Date)
                await InitCacheAsync(item, context, log);

            if (!_dict.ContainsKey(item.AssetPair))
                return null;

            var fromDb = _dict[item.AssetPair].FirstOrDefault(c => c.IsAsk == item.IsAsk && c.Start == item.Start);
            return fromDb;
        }

        private static async Task InitCacheAsync(Candlestick item, DataContext context, ILog log)
        {
            DateTime from = item.Start.Date;
            DateTime to = from.AddDays(1);
            string query = $"SELECT * FROM dbo.Candlesticks2 WHERE Start BETWEEN '{from.ToString(_format)}' AND '{to.ToString(_format)}'";
            var items = context.Candlesticks.FromSql(query).ToList();
            _dict = items.GroupBy(i => i.AssetPair).ToDictionary(g => g.Key, g => g.ToList());
            await log.WriteInfoAsync(
                nameof(CandlestickSqlFinder),
                nameof(InitCacheAsync),
                $"Cached {items.Count} items from sql for {from.ToString(_format)}.");
        }
    }
}
