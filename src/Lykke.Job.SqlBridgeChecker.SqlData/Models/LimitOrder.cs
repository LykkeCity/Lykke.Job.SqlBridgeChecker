using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class LimitOrder : IDbEntity, IValidatable
    {
        public static int MaxStringFieldsLength { get { return 255; } }

        public string Id { get; set; }

        public string ExternalId { get; set; }

        public string AssetPairId { get; set; }

        public string ClientId { get; set; }

        public double Volume { get; set; }

        public double Price { get; set; }

        public string Status { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime Registered { get; set; }

        public DateTime? LastMatchTime { get; set; }

        public double RemainingVolume { get; set; }

        public bool Straight { get; set; } = true;

        public List<LimitTradeInfo> Trades { get; set; }

        public object GetEntityId()
        {
            return Id;
        }

        public bool IsValid()
        {
            return Id != null && Id.Length <= MaxStringFieldsLength
                && ExternalId != null && ExternalId.Length <= MaxStringFieldsLength
                && AssetPairId != null && AssetPairId.Length <= MaxStringFieldsLength
                && ClientId != null && ClientId.Length <= MaxStringFieldsLength
                && Status != null && Status.Length <= MaxStringFieldsLength
                && Volume != 0
                && Price > 0
                && (Trades == null || Trades.Count == 0 || Trades.All(t => t.IsValid()));
        }

        public static async Task<LimitOrder> FromModelAsync(
            LimitOrderEntity model,
            List<ClientTradeEntity> tradeItems,
            Func<string, Task<LimitOrderEntity>> limitOrderGetterAsync,
            Func<string, Task<MarketOrderEntity>> marketOrderGetterAsync,
            ILog log)
        {
            var result = new LimitOrder
            {
                Id = model.MatchingId,
                ExternalId = model.Id ?? model.RowKey,
                AssetPairId = model.AssetPairId,
                ClientId = model.ClientId,
                Volume = model.Volume,
                Price = model.Price,
                Status = model.Status,
                CreatedAt = model.CreatedAt,
                Registered = model.Timestamp.DateTime,
                Straight = model.Straight,
                RemainingVolume = model.RemainingVolume,
                LastMatchTime = model.Status == "Matched" ? model.Timestamp.DateTime : (DateTime?)null,
                Trades = new List<LimitTradeInfo>(),
            };
            if (tradeItems == null)
                return result;

            var tradeByLimitOrder = tradeItems.GroupBy(t => t.OppositeLimitOrderId ?? t.MarketOrderId);
            foreach (var trades in tradeByLimitOrder)
            {
                int clientCount = trades.Select(t => t.ClientId).Distinct().Count();
                if (clientCount > 1)
                    await log.WriteWarningAsync(
                        nameof(FromModelAsync),
                        nameof(LimitOrder),
                        $"Found {clientCount} clients in trades for LimitOrder {model.Id ?? model.RowKey}");
                var first = trades.First();
                var trade = new LimitTradeInfo
                {
                    LimitOrderId = result.Id,
                    ClientId = model.ClientId,
                    Timestamp = first.DateTime,
                    Price = first.Price,
                };
                foreach (var clientTrade in trades.OrderBy(t => t.Volume == 0 ? double.MaxValue : t.Volume))
                {
                    if (clientTrade.Volume < 0)
                    {
                        trade.Asset = clientTrade.AssetId;
                        trade.Volume = -clientTrade.Volume;
                    }
                    else if (clientTrade.Volume > 0)
                    {
                        trade.OppositeAsset = clientTrade.AssetId;
                        trade.OppositeVolume = clientTrade.Volume;
                    }
                    else
                    {
                        if (string.IsNullOrWhiteSpace(trade.Asset) && !string.IsNullOrWhiteSpace(trade.OppositeAsset))
                        {
                            trade.Asset = clientTrade.AssetId;
                            trade.Volume = -clientTrade.Volume;
                        }
                        else if (string.IsNullOrWhiteSpace(trade.OppositeAsset) && !string.IsNullOrWhiteSpace(trade.Asset))
                        {
                            trade.OppositeAsset = clientTrade.AssetId;
                            trade.OppositeVolume = clientTrade.Volume;
                        }
                        else if (string.IsNullOrWhiteSpace(trade.Asset) && string.IsNullOrWhiteSpace(trade.OppositeAsset))
                        {
                            if (model.Straight ^ model.AssetPairId.StartsWith(clientTrade.AssetId))
                            {
                                trade.Asset = clientTrade.AssetId;
                                trade.Volume = clientTrade.Volume;
                            }
                            else
                            {
                                trade.OppositeAsset = clientTrade.AssetId;
                                trade.OppositeVolume = clientTrade.Volume;
                            }
                        }
                    }
                }
                if (!string.IsNullOrWhiteSpace(first.OppositeLimitOrderId) || !string.IsNullOrWhiteSpace(first.MarketOrderId))
                {
                    var marketOrder = await marketOrderGetterAsync(first.OppositeLimitOrderId ?? first.MarketOrderId);
                    if (marketOrder != null)
                    {
                        trade.OppositeClientId = marketOrder.ClientId;
                        trade.OppositeOrderExternalId = marketOrder.Id ?? marketOrder.RowKey;
                        trade.OppositeOrderId = marketOrder.MatchingId ?? trade.OppositeOrderExternalId;
                    }
                    else if (!string.IsNullOrWhiteSpace(first.OppositeLimitOrderId))
                    {
                        var otherLimitOrder = await limitOrderGetterAsync(first.OppositeLimitOrderId);
                        if (otherLimitOrder != null)
                        {
                            trade.OppositeClientId = otherLimitOrder.ClientId;
                            trade.OppositeOrderId = otherLimitOrder.MatchingId;
                            trade.OppositeOrderExternalId = otherLimitOrder.Id ?? otherLimitOrder.RowKey;
                        }
                    }
                }
                if (trade.OppositeClientId == null)
                {
                    await log.WriteWarningAsync(
                        nameof(FromModelAsync),
                        nameof(LimitOrder),
                        $"For order {result.ExternalId} other order is not found for key {first.OppositeLimitOrderId ?? first.MarketOrderId}");
                    continue;
                }
                if (string.IsNullOrWhiteSpace(trade.Asset))
                {
                    await log.WriteWarningAsync(
                        nameof(FromModelAsync),
                        nameof(LimitOrder),
                        $"Asset not found for LimitOrder {result.Id}");
                    continue;
                }
                if (string.IsNullOrWhiteSpace(trade.OppositeAsset))
                {
                    await log.WriteWarningAsync(
                        nameof(FromModelAsync),
                        nameof(LimitOrder),
                        $"OppositeAsset not found for LimitOrder {result.Id}");
                    continue;
                }
                result.Trades.Add(trade);
            }

            return result;
        }
    }
}
