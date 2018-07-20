using Autofac;
using JetBrains.Annotations;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.SqlBridgeChecker.Services
{
    [UsedImplicitly]
    public class StartupManager : IStartupManager
    {
        private readonly List<IStartable> _startables = new List<IStartable>();

        public async Task StartAsync()
        {
            foreach (var item in _startables)
            {
                item.Start();
            }

            await Task.CompletedTask;
        }

        public void Register(IStartable startable)
        {
            _startables.Add(startable);
        }
    }
}
