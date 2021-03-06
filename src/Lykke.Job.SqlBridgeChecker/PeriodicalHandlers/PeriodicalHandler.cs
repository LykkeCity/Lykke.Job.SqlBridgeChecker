﻿using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.SqlBridgeChecker.Core.Services;

namespace Lykke.Job.SqlBridgeChecker.PeriodicalHandlers
{
    [UsedImplicitly]
    public class PeriodicalHandler : TimerPeriod, IStartStop
    {
        private readonly IDataChecker _checker;

        public PeriodicalHandler(IDataChecker checker, ILog log)
            : base((int)TimeSpan.FromHours(24).TotalMilliseconds, log)
        {
            _checker = checker;
        }

        public override  Task Execute()
        {
            var today = DateTime.UtcNow.Date;
            Task.Run(() => _checker.CheckAndFixDataAsync(today).GetAwaiter().GetResult());
            return Task.CompletedTask;
        }
    }
}
