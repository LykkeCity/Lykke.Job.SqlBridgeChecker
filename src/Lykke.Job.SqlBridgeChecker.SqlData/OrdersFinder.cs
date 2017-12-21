using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public static class OrdersFinder
    {
        public static LimitOrder GetLimitOrder(string orderId, string sqlConnectionString)
        {
            using (var context = new DataContext(sqlConnectionString))
            {
                var result = context.LimitOrders.Find(orderId);
                if (result != null)
                    return result;
                context.Database.SetCommandTimeout(TimeSpan.FromMinutes(15));
                string query = $"SELECT * FROM dbo.{DataContext.LimitOrdersTable} WHERE ExternalId = '{orderId}'";
                var items = context.LimitOrders.AsNoTracking().FromSql(query);
                return items.FirstOrDefault();
            }
        }
    }
}
