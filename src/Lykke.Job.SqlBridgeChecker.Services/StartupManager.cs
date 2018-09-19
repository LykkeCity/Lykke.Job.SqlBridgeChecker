using System.Collections.Generic;
using System.Threading.Tasks;
using Autofac;
using JetBrains.Annotations;
using Lykke.Job.SqlBridgeChecker.Core.Services;

namespace Lykke.Job.SqlBridgeChecker.Services
{
    [UsedImplicitly]
    public class StartupManager : IStartupManager
    {
        private readonly List<IStartable> _startables = new List<IStartable>();

        public StartupManager(IEnumerable<IStartStop> startables)
        {
            _startables.AddRange(startables);
        }

        public Task StartAsync()
        {
            foreach (var item in _startables)
            {
                item.Start();
            }

            return Task.CompletedTask;
        }
    }
}
