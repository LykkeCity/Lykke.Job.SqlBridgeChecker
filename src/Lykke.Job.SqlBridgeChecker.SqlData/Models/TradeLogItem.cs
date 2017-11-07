using System;
using System.Linq;
using System.Collections.Generic;
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

        public static List<TradeLogItem> FromModel(IEnumerable<ClientTradeEntity> trades, ILog log)
        {
            var result = trades
                .Select(i => CreateInstance(i, trades, log))
                .Where(i => i != null)
                .ToList();
            return result;
        }

        private static TradeLogItem CreateInstance(
            ClientTradeEntity model,
            IEnumerable<ClientTradeEntity> trades,
            ILog log)
        {
            string oppositeOrderId = model.MarketOrderId ?? model.OppositeLimitOrderId;
            string tradeId = model.LimitOrderId.CompareTo(oppositeOrderId) <= 0
                ? $"{model.LimitOrderId}_{oppositeOrderId}" : $"{oppositeOrderId}_{model.LimitOrderId}";
            var result = new TradeLogItem
            {
                TradeId = tradeId,
                DateTime = model.DateTime,
                UserId = model.ClientId,
                WalletId = model.ClientId,
                Direction = model.Volume >= 0 ? "Buy" : "Sell",
                Asset = model.AssetId,
                Volume = (decimal)Math.Abs(model.Volume),
                Price = (decimal)model.Price,
                IsHidden = model.IsHidden,
            };
            if (!model.IsLimitOrderResult.HasValue || !string.IsNullOrWhiteSpace(model.MarketOrderId))
            {
                result.OrderId = model.MarketOrderId;
                result.OrderType = "Market";
                result.OppositeOrderId = model.LimitOrderId;
            }
            else
            {
                result.OrderId = model.LimitOrderId;
                result.OrderType = "Limit";
                result.OppositeOrderId = model.OppositeLimitOrderId;
            }
            var otherAssetTrade = trades.FirstOrDefault(t =>
                t.ClientId == model.ClientId && t.AssetId != model.AssetId);
            if (otherAssetTrade == null)
            {
                int otherAssetsCount = trades.Where(t => t.AssetId != model.AssetId).Distinct().Count();
                if (otherAssetsCount == 1)
                {
                    otherAssetTrade = trades.First(t => t.AssetId != model.AssetId);
                }
                else
                {
                    log.WriteWarningAsync(
                        nameof(TradeLogItem),
                        nameof(CreateInstance),
                        $"Could not determine opposite asset for {model.ToJson()}!").Wait();
                    return null;
                }
            }
            result.OppositeAsset = otherAssetTrade.AssetId;
            result.OppositeVolume = (decimal)Math.Abs(otherAssetTrade.Volume);
            return result;
        }
    }
}
