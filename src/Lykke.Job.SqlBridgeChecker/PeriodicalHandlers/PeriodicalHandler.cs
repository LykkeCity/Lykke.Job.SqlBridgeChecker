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
            : base((int)TimeSpan.FromHours(12).TotalMilliseconds, log)
        {
            _checker = checker;
            _log = log;
        }

        public override async Task Execute()
        {
            await _log.WriteInfoAsync(
                nameof(PeriodicalHandler),
                nameof(Execute),
                "Periodic work has been started");

            await _checker.CheckAndFixDataAsync();

            await _log.WriteInfoAsync(
                nameof(PeriodicalHandler),
                nameof(Execute),
                "Periodic work is finished");
        }
    }
}
