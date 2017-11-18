﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class MarketOrder : IDbEntity, IValidatable
    {
        public static int MaxStringFieldsLength { get { return 255; } }

        public string Id { get; set; }

        public string ExternalId { get; set; }

        public string AssetPairId { get; set; }

        public string ClientId { get; set; }

        public double Volume { get; set; }

        public double? Price { get; set; }

        public string Status { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime Registered { get; set; }

        public DateTime? MatchedAt { get; set; }

        public bool Straight { get; set; }

        public double? ReservedLimitVolume { get; set; }

        public List<TradeInfo> Trades { get; set; }

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
                && (Trades == null || Trades.Count == 0 || Trades.All(t => t.IsValid()));
        }

        public static async Task<MarketOrder> FromModelAsync(
            MarketOrderEntity model,
            List<ClientTradeEntity> tradeItems,
            Func<string, Task<string>> otherClientGetterAsync,
            ILog log)
        {
            var result = new MarketOrder
            {
                Id = model.MatchingId,
                ExternalId = model.Id ?? model.RowKey,
                AssetPairId = model.AssetPairId,
                ClientId = model.ClientId,
                Volume = model.Volume,
                Price = model.Price,
                Status = model.Status,
                CreatedAt = model.CreatedAt,
                Registered = model.Registered ?? model.MatchedAt ?? model.CreatedAt,
                MatchedAt = NormalizeDate(model.MatchedAt),
                Straight = model.Straight,
                Trades = new List<TradeInfo>(),
            };
            if (result.Id == null)
                result.Id = result.ExternalId;
            if (result.MatchedAt == default(DateTime))
                result.MatchedAt = null;
            if (tradeItems == null)
                return result;

            var tradeByLimitOrder = tradeItems.GroupBy(t => t.LimitOrderId);
            foreach (var trades in tradeByLimitOrder)
            {
                var first = trades.First();
                var trade = new TradeInfo
                {
                    MarketOrderId = result.Id,
                    MarketClientId = model.ClientId,
                    LimitOrderId = "N/A",
                    LimitOrderExternalId = trades.Key,
                    Timestamp = first.DateTime,
                    Price = first.Price,
                };
                var marketTrades = trades.Where(t => t.ClientId == model.ClientId);
                foreach (var marketTrade in marketTrades.OrderBy(t => t.Volume == 0 ? double.MaxValue : t.Volume))
                {
                    if (marketTrade.Volume < 0)
                    {
                        trade.MarketAsset = marketTrade.AssetId;
                        trade.MarketVolume = -marketTrade.Volume;
                    }
                    else if (marketTrade.Volume > 0)
                    {
                        trade.LimitAsset = marketTrade.AssetId;
                        trade.LimitVolume = marketTrade.Volume;
                    }
                    else
                    {
                        if (string.IsNullOrWhiteSpace(trade.MarketAsset) && !string.IsNullOrWhiteSpace(trade.LimitAsset))
                        {
                            trade.MarketAsset = marketTrade.AssetId;
                            trade.MarketVolume = -marketTrade.Volume;
                        }
                        else if (string.IsNullOrWhiteSpace(trade.LimitAsset) && !string.IsNullOrWhiteSpace(trade.MarketAsset))
                        {
                            trade.LimitAsset = marketTrade.AssetId;
                            trade.LimitVolume = marketTrade.Volume;
                        }
                        else if (string.IsNullOrWhiteSpace(trade.MarketAsset) && string.IsNullOrWhiteSpace(trade.LimitAsset))
                        {
                            if (model.Straight ^ model.AssetPairId.StartsWith(marketTrade.AssetId))
                            {
                                trade.MarketAsset = marketTrade.AssetId;
                                trade.MarketVolume = marketTrade.Volume;
                            }
                            else
                            {
                                trade.LimitAsset = marketTrade.AssetId;
                                trade.LimitVolume = marketTrade.Volume;
                            }
                        }
                    }
                }
                var limitTrades = trades.Where(t => t.ClientId != model.ClientId);
                var clients = limitTrades.Where(i => i.ClientId != null).Select(t => t.ClientId).Distinct().ToList();
                if (clients.Count == 1)
                {
                    trade.LimitClientId = clients[0];
                    foreach (var limitTrade in limitTrades.OrderBy(t => t.Volume == 0 ? double.MaxValue : t.Volume))
                    {
                        if (limitTrade.Volume > 0)
                        {
                            if (trade.MarketAsset != null)
                                continue;
                            trade.MarketAsset = limitTrade.AssetId;
                            trade.MarketVolume = limitTrade.Volume;
                        }
                        else if (limitTrade.Volume < 0)
                        {
                            if (trade.LimitAsset != null)
                                continue;
                            trade.LimitAsset = limitTrade.AssetId;
                            trade.LimitVolume = -limitTrade.Volume;
                        }
                        else
                        {
                            if (string.IsNullOrWhiteSpace(trade.MarketAsset) && !string.IsNullOrWhiteSpace(trade.LimitAsset))
                            {
                                trade.MarketAsset = limitTrade.AssetId;
                                trade.MarketVolume = limitTrade.Volume;
                            }
                            else if (string.IsNullOrWhiteSpace(trade.LimitAsset) && !string.IsNullOrWhiteSpace(trade.MarketAsset))
                            {
                                trade.LimitAsset = limitTrade.AssetId;
                                trade.LimitVolume = -limitTrade.Volume;
                            }
                            else if (string.IsNullOrWhiteSpace(trade.MarketAsset) && string.IsNullOrWhiteSpace(trade.LimitAsset))
                            {
                                if (model.Straight ^ model.AssetPairId.StartsWith(limitTrade.AssetId))
                                {
                                    trade.MarketAsset = limitTrade.AssetId;
                                    trade.MarketVolume = limitTrade.Volume;
                                }
                                else
                                {
                                    trade.LimitAsset = limitTrade.AssetId;
                                    trade.LimitVolume = limitTrade.Volume;
                                }
                            }
                        }
                    }
                }
                else if (clients.Count > 1)
                {
                    await log.WriteErrorAsync(
                        nameof(MarketOrder),
                        nameof(FromModelAsync),
                        new ArgumentOutOfRangeException($"Found too many LimitClients for MarketOrder {result.Id}"));
                    continue;
                }
                else
                {
                    trade.LimitClientId = await otherClientGetterAsync(first.LimitOrderId);
                    if (string.IsNullOrWhiteSpace(trade.LimitClientId))
                    {
                        await log.WriteErrorAsync(
                            nameof(MarketOrder),
                            nameof(FromModelAsync),
                            new ArgumentOutOfRangeException($"Couldn't find 2nd client from limitOrder {first.LimitOrderId} for marketOrder {result.Id}"));
                        continue;
                    }
                }
                if (string.IsNullOrWhiteSpace(trade.MarketAsset))
                {
                    await log.WriteErrorAsync(
                        nameof(MarketOrder),
                        nameof(FromModelAsync),
                        new ArgumentOutOfRangeException($"MarketAsset not found for MarketOrder {result.Id}"));
                    continue;
                }
                if (string.IsNullOrWhiteSpace(trade.LimitAsset))
                {
                    await log.WriteErrorAsync(
                        nameof(MarketOrder),
                        nameof(FromModelAsync),
                        new ArgumentOutOfRangeException($"LimitAsset not found for MarketOrder {result.Id}"));
                    continue;
                }
                result.Trades.Add(trade);
            }

            return result;
        }

        private static DateTime? NormalizeDate(DateTime? date)
        {
            return date.HasValue
                ? (date.Value == default(DateTime) ? null : date)
                : null;
        }
    }
}
