using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Service.ClientAccount.Client;
using Lykke.Service.ClientAccount.Client.AutorestClient.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public class TradesChecker : DataCheckerBase<ClientTradeEntity, TradeLogItem>
    {
        private readonly IClientAccountClient _clientAccountClient;

        public TradesChecker(
            string sqlConnecctionString,
            ITradesRepository repository,
            IClientAccountClient clientAccountClient,
            ILog log)
            : base(sqlConnecctionString, repository, log)
        {
            _clientAccountClient = clientAccountClient;
        }

        protected override async Task<List<TradeLogItem>> ConvertItemsToSqlTypesAsync(IEnumerable<ClientTradeEntity> items)
        {
            var result = new List<TradeLogItem>();
            var groups = items.GroupBy(i => new { i.MarketOrderId, i.LimitOrderId, i.OppositeLimitOrderId });
            foreach (var group in groups)
            {
                var trades = await TradeLogItem.FromModelAsync(group, GetWalletInfoAsync, _log);
                result.AddRange(trades);
            }
            return result;
        }

        protected override async Task<TradeLogItem> FindInSqlDbAsync(TradeLogItem item, DataContext context)
        {
            var inSql = await TradeSqlFinder.FindInDbAsync(item, context, _log);
            if (inSql == null)
                await _log.WriteInfoAsync(
                    nameof(TradesChecker),
                    nameof(FindInSqlDbAsync),
                    $"{item.ToJson()}");
            return inSql;
        }

        protected override async Task<bool> UpdateItemAsync(TradeLogItem inSql, TradeLogItem convertedItem, DataContext context)
        {
            var changed = inSql.TradeId != convertedItem.TradeId
                || inSql.OppositeOrderId != convertedItem.OppositeOrderId
                || inSql.Direction != convertedItem.Direction
                || inSql.IsHidden != convertedItem.IsHidden;
            if (!changed)
                return false;
            await _log.WriteInfoAsync(
                nameof(TradesChecker),
                nameof(UpdateItemAsync),
                $"{inSql.ToJson()}");
            inSql.TradeId = convertedItem.TradeId;
            inSql.OppositeOrderId = convertedItem.OppositeOrderId;
            inSql.Direction = convertedItem.Direction;
            inSql.IsHidden = convertedItem.IsHidden;
            return true;
        }

        private async Task<(string, string)> GetWalletInfoAsync(string clientId)
        {
            var wallet = await _clientAccountClient.GetWalletAsync(clientId);
            if (wallet != null && wallet.Type != WalletType.Trading.ToString())
                return (wallet.ClientId, clientId);

            return (clientId, clientId);
            //var wallets = await _clientAccountClient.GetClientWalletsByTypeAsync(clientId, WalletType.Trading);
            //if (wallets == null || !wallets.Any())
            //    return (clientId, clientId);
            //var tradingWallet = wallets.First();
            //return (clientId, tradingWallet.Id);
        }
    }
}
