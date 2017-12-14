using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class TradeLogItem : IValidatable, IDbEntity
    {
        public static int MaxStringFieldsLength { get { return 255; } }

        public long Id { get; set; }

        public string TradeId { get; set; }

        public string UserId { get; set; }

        public string WalletId { get; set; }

        public string OrderId { get; set; }

        public string OrderType { get; set; }

        public string Direction { get; set; }

        public string Asset { get; set; }

        public decimal Volume { get; set; }

        public decimal Price { get; set; }

        public DateTime DateTime { get; set; }

        public string OppositeOrderId { get; set; }

        public string OppositeAsset { get; set; }

        public decimal? OppositeVolume { get; set; }

        public bool? IsHidden { get; set; }

        public bool IsValid()
        {
            return UserId != null && UserId.Length <= MaxStringFieldsLength
                && WalletId != null && WalletId.Length <= MaxStringFieldsLength
                && OrderId != null && OrderId.Length <= MaxStringFieldsLength
                && OrderType != null && OrderType.Length <= MaxStringFieldsLength
                && Direction != null && Direction.Length <= MaxStringFieldsLength
                && Asset != null && Asset.Length <= MaxStringFieldsLength
                && Volume > 0
                && Price > 0
                && OppositeOrderId != null && OppositeOrderId.Length <= MaxStringFieldsLength
                && OppositeAsset != null && OppositeAsset.Length <= MaxStringFieldsLength
                && OppositeVolume > 0;
        }

        public object GetEntityId()
        {
            return Id;
        }

        public static async Task<List<TradeLogItem>> FromModelAsync(
            IEnumerable<ClientTradeEntity> trades,
            Func<string, Task<(string, string)>> walletInfoAsyncGetter,
            ILog log)
        {
            var result = new List<TradeLogItem>();
            var walletInfoCache = new Dictionary<string, (string, string)>();
            foreach (var trade in trades)
            {
                if (!walletInfoCache.ContainsKey(trade.ClientId))
                {
                    (string userId, string walletId) = await walletInfoAsyncGetter(trade.ClientId);
                    walletInfoCache.Add(trade.ClientId, (userId, walletId));
                }
                var walletInfo = walletInfoCache[trade.ClientId];
                var item = await CreateInstanceAsync(
                    trade,
                    trades,
                    walletInfo.Item1,
                    walletInfo.Item2,
                    log);
                result.Add(item);
            }
            return result;
        }

        public static string GetTradeId(string id1, string id2)
        {
            return id1.CompareTo(id2) <= 0 ? $"{id1}_{id2}" : $"{id2}_{id1}";
        }

        private static async Task<TradeLogItem> CreateInstanceAsync(
            ClientTradeEntity model,
            IEnumerable<ClientTradeEntity> trades,
            string userId,
            string walletId,
            ILog log)
        {
            string orderId = model.LimitOrderId;
            string oppositeOrderId = model.MarketOrderId ?? model.OppositeLimitOrderId;
            string tradeId = GetTradeId(orderId, oppositeOrderId);
            var result = new TradeLogItem
            {
                TradeId = tradeId,
                DateTime = model.DateTime,
                UserId = userId,
                WalletId = walletId,
                Direction = model.Volume >= 0 ? "Buy" : "Sell",
                Asset = model.AssetId,
                Volume = (decimal)Math.Abs(model.Volume),
                Price = (decimal)model.Price,
                IsHidden = model.IsHidden,
            };
            if (!model.IsLimitOrderResult.HasValue || !string.IsNullOrWhiteSpace(model.MarketOrderId))
            {
                result.OrderId = oppositeOrderId;
                result.OrderType = "Market";
                result.OppositeOrderId = orderId;
            }
            else
            {
                result.OrderId = orderId;
                result.OrderType = "Limit";
                result.OppositeOrderId = oppositeOrderId;
            }
            var otherAssetTrade = trades.FirstOrDefault(t =>
                t.ClientId == model.ClientId && t.AssetId != model.AssetId);
            if (otherAssetTrade == null)
            {
                int otherAssetsCount = trades.Where(t => t.AssetId != model.AssetId).Distinct().Count();
                if (otherAssetsCount == 1)
                    otherAssetTrade = trades.First(t => t.AssetId != model.AssetId);
            }
            if (otherAssetTrade != null)
            {
                result.OppositeAsset = otherAssetTrade.AssetId;
                result.OppositeVolume = (decimal)Math.Abs(otherAssetTrade.Volume);
            }
            if (result.OppositeAsset == null)
                await log.WriteWarningAsync(
                    nameof(TradeLogItem),
                    nameof(CreateInstanceAsync),
                    $"Could not determine opposite asset for {model.ToJson()}!");
            else if (result.OppositeVolume == null)
                await log.WriteWarningAsync(
                    nameof(TradeLogItem),
                    nameof(CreateInstanceAsync),
                    $"Could not determine opposite volume for {model.ToJson()}!");
            return result;
        }
    }
}
