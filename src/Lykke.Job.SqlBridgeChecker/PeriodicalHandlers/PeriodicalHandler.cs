using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Services;

namespace Lykke.Job.SqlBridgeChecker.PeriodicalHandlers
{
    public class PeriodicalHandler : TimerPeriod
    {
        private readonly IDataChecker _checker;
        private readonly ILog _log;

        public PeriodicalHandler(IDataChecker checker, ILog log)
            : base((int)TimeSpan.FromHours(24).TotalMilliseconds, log)
        {
            _checker = checker;
            _log = log;
        }

        public override  Task Execute()
        {
            Task.Run(() => _checker.CheckAndFixDataAsync().GetAwaiter().GetResult());
            return Task.CompletedTask;
        }
    }
}
