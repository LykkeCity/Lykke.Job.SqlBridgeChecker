using System.Linq;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;
using Microsoft.EntityFrameworkCore;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public static class OrdersFinder
    {
        public static LimitOrder GetLimitOrder(string orderId, string sqlConnectionString)
        {
            string query = $"SELECT TOP 1 * FROM dbo.{DataContext.LimitOrdersTable} WHERE ExternalId = '{orderId}'";

            using (var context = new DataContext(sqlConnectionString))
            {
                var items = context.LimitOrders.AsNoTracking().FromSql(query);
                return items.FirstOrDefault();
            }
        }

        public static MarketOrder GetMarketOrder(string orderId, string sqlConnectionString)
        {
            string query = $"SELECT TOP 1 * FROM dbo.{DataContext.MarketOrdersTable} WHERE ExternalId = '{orderId}'";

            using (var context = new DataContext(sqlConnectionString))
            {
                var items = context.MarketOrders.AsNoTracking().FromSql(query);
                return items.FirstOrDefault();
            }
        }
    }
}
