﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class TradeLogItem : IValidatable, IDbEntity
    {
        public static int MaxStringFieldsLength => 255;

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
            return !string.IsNullOrEmpty(UserId) && UserId.Length <= MaxStringFieldsLength && Guid.TryParse(UserId, out _)
                && !string.IsNullOrEmpty(WalletId) && WalletId.Length <= MaxStringFieldsLength && Guid.TryParse(WalletId, out _)
                && !string.IsNullOrEmpty(OrderId) && OrderId.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(OrderType) && OrderType.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(Direction) && Direction.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(Asset) && Asset.Length <= MaxStringFieldsLength
                && Volume > 0
                && Price > 0
                && !string.IsNullOrEmpty(OppositeOrderId) && OppositeOrderId.Length <= MaxStringFieldsLength
                && !string.IsNullOrEmpty(OppositeAsset) && OppositeAsset.Length <= MaxStringFieldsLength
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
                var item = CreateInstance(
                    trade,
                    trades,
                    walletInfo.Item1,
                    walletInfo.Item2,
                    log);
                if (item != null)
                    result.Add(item);
            }
            return result;
        }

        public static string GetTradeId(string id1, string id2)
        {
            return id1.CompareTo(id2) <= 0 ? $"{id1}_{id2}" : $"{id2}_{id1}";
        }

        private static TradeLogItem CreateInstance(
            ClientTradeEntity model,
            IEnumerable<ClientTradeEntity> trades,
            string userId,
            string walletId,
            ILog log)
        {
            string orderId = model.LimitOrderId;
            string oppositeOrderId = model.MarketOrderId ?? model.OppositeLimitOrderId;
            string tradeId = GetTradeId(orderId, oppositeOrderId);
            var otherAssetTrade = trades.FirstOrDefault(t =>
                t.ClientId == model.ClientId && t.AssetId != model.AssetId);
            var result = new TradeLogItem
            {
                TradeId = tradeId,
                DateTime = model.DateTime,
                UserId = userId,
                WalletId = walletId,
                Asset = model.AssetId,
                Volume = (decimal)Math.Abs(model.Volume),
                Price = (decimal)model.Price,
                IsHidden = model.IsHidden,
            };
            if (model.Volume > 0)
                result.Direction = "Buy";
            else if (model.Volume < 0)
                result.Direction = "Sell";
            else if (otherAssetTrade == null)
                result.Direction = "Buy";
            else if (otherAssetTrade.Volume > 0)
                result.Direction = "Sell";
            else
                result.Direction = "Buy";
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
            if (otherAssetTrade == null)
            {
                int otherAssetsCount = trades.Where(t => t.AssetId != model.AssetId).Distinct().Count();
                if (otherAssetsCount == 1)
                    otherAssetTrade = trades.First(t => t.AssetId != model.AssetId);
            }
            if (otherAssetTrade != null)
            {
                if (Math.Sign(model.Volume) == Math.Sign(otherAssetTrade.Volume))
                {
                    log.WriteWarning(
                        nameof(CreateInstance),
                        nameof(TradeLogItem),
                        $"Same direction for {model.ToJson()} as in pair trade {otherAssetTrade.ToJson()}! Skipping.");
                    return null;
                }
                result.OppositeAsset = otherAssetTrade.AssetId;
                result.OppositeVolume = (decimal)Math.Abs(otherAssetTrade.Volume);
            }

            if (result.OppositeAsset == null)
            {
                log.WriteWarning(
                    nameof(CreateInstance),
                    nameof(TradeLogItem),
                    $"Could not determine opposite asset for {model.ToJson()}! Skipping.");
                return null;
            }
            if (result.OppositeVolume == null)
            {
                log.WriteWarning(
                    nameof(CreateInstance),
                    nameof(TradeLogItem),
                    $"Could not determine opposite volume for {model.ToJson()}! Skipping.");
            }

            return result;
        }
    }
}
