using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Service.ClientAccount.Client;
using Lykke.Service.ClientAccount.Client.AutorestClient.Models;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
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

        protected override void ClearCaches()
        {
            TradeSqlFinder.ClearCache();
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

        protected override Task<TradeLogItem> FindInSqlDbAsync(TradeLogItem item, DataContext context)
        {
            var inSql = TradeSqlFinder.FindInDb(item, context, _log);
            if (inSql == null)
                _log.WriteInfo(nameof(FindInSqlDbAsync), $"{item.OrderId}", $"{item.ToJson()}");
            return Task.FromResult(inSql);
        }

        protected override bool UpdateItem(TradeLogItem inSql, TradeLogItem convertedItem, DataContext context)
        {
            var changed = (inSql.Direction != convertedItem.Direction && convertedItem.Volume != 0)
                || inSql.IsHidden != convertedItem.IsHidden;
            if (!changed)
                return false;
            _log.WriteInfo(nameof(UpdateItem), $"{convertedItem.Asset}_{convertedItem.OppositeAsset}", $"{inSql.ToJson()}");
            if (convertedItem.Volume != 0)
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
