using System;
using System.Linq;
using System.Collections.Generic;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    internal class TradeCandle
    {
        public double Open { get; set; }
        public double Close { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public int Seconds { get; set; }
    }

    public class Candlestick : IValidatable, IDbEntity
    {
        public static int MaxStringFieldsLength { get { return 255; } }

        public long Id { get; set; }

        public string AssetPair { get; set; }

        public bool IsAsk { get; set; }

        public double High { get; set; }

        public double Low { get; set; }

        public double Open { get; set; }

        public double Close { get; set; }

        public DateTime Start { get; set; }

        public DateTime Finish { get; set; }

        public object GetEntityId()
        {
            return Id;
        }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(AssetPair) && AssetPair.Length <= MaxStringFieldsLength;
        }

        public static Candlestick FromModel(IEnumerable<FeedHistoryEntity> models, ILog log)
        {
            var first = models.First();
            var assetPriceTypeVals = first.PartitionKey.Split('_');
            var result = new Candlestick
            {
                AssetPair = assetPriceTypeVals[0],
                IsAsk = assetPriceTypeVals[1] == "Ask",
            };
            DateTime modelTime = ParseFeedTime(first.RowKey);
            var candles = new List<TradeCandle>();
            foreach (var model in models)
                candles.AddRange(ParseCandles(model.Data));
            int start = 61;
            int finish = -1;
            double high = 0, low = long.MaxValue, open = 0, close = 0;
            foreach(var candle in candles)
            {
                if (candle.Seconds < start)
                {
                    start = candle.Seconds;
                    open = candle.Open;
                }
                if (candle.Seconds > finish)
                {
                    finish = candle.Seconds;
                    close = candle.Close;
                }
                if (high < candle.High)
                    high = candle.High;
                if (low > candle.Low)
                    low = candle.Low;
            }
            if (start == 61 || finish == -1)
            {
                log.WriteWarningAsync(nameof(Candlestick), models.ToList().ToJson(), $"Could not set time mark for a candle")
                    .GetAwaiter().GetResult();
                return null;
            }

            result.Open = (Single)open;
            result.Close = (Single)close;
            result.High = (Single)high;
            result.Low = (Single)low;
            result.Start = modelTime.AddSeconds(start);
            result.Finish = modelTime.AddSeconds(59);
            return result;
        }

        private static DateTime ParseFeedTime(string rowKey)
        {
            //example: 201604290745
            int year = int.Parse(rowKey.Substring(0, 4));
            int month = int.Parse(rowKey.Substring(4, 2));
            int day = int.Parse(rowKey.Substring(6, 2));
            int hour = int.Parse(rowKey.Substring(8, 2));
            int min = int.Parse(rowKey.Substring(10, 2));
            return new DateTime(year, month, day, hour, min, 0, DateTimeKind.Utc);
        }

        private static List<TradeCandle> ParseCandles(string data)
        {
            var candlesList = new List<TradeCandle>();
            if (string.IsNullOrEmpty(data))
                return candlesList;

            var candles = data.Split('|');
            foreach (var candle in candles)
            {
                if (string.IsNullOrEmpty(candle))
                    continue;

                //parameters example: O=446.322;C=446.322;H=446.322;L=446.322;T=30
                var parameters = candle.Split(';');

                var tradeCandle = new TradeCandle();
                foreach (var nameValuePair in parameters.Select(parameter => parameter.Split('=')))
                {
                    switch (nameValuePair[0].ToUpper())
                    {
                        case "O":
                            tradeCandle.Open = nameValuePair[1].ParseAnyDouble();
                            break;
                        case "C":
                            tradeCandle.Close = nameValuePair[1].ParseAnyDouble();
                            break;
                        case "H":
                            tradeCandle.High = nameValuePair[1].ParseAnyDouble();
                            break;
                        case "L":
                            tradeCandle.Low = nameValuePair[1].ParseAnyDouble();
                            break;
                        case "T":
                            tradeCandle.Seconds = int.Parse(nameValuePair[1]);
                            break;
                        default:
                            throw new ArgumentException("unexpected value");
                    }
                }
                candlesList.Add(tradeCandle);
            }

            return candlesList;
        }
    }
}
