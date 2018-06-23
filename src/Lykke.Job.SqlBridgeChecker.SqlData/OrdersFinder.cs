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
                string query = $"SELECT * FROM dbo.{DataContext.LimitOrdersTable} WHERE ExternalId = '{orderId}'";
                var items = context.LimitOrders.AsNoTracking().FromSql(query);
                return items.FirstOrDefault();
            }
        }

        public static MarketOrder GetMarketOrder(string orderId, string sqlConnectionString)
        {
            using (var context = new DataContext(sqlConnectionString))
            {
                string query = $"SELECT * FROM dbo.{DataContext.MarketOrdersTable} WHERE ExternalId = '{orderId}'";
                var items = context.MarketOrders.AsNoTracking().FromSql(query);
                return items.FirstOrDefault();
            }
        }
    }
}
