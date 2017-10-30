using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Services;

namespace Lykke.Job.SqlBridgeChecker.Services
{
    public class CheckersRepository : ICheckersRepository, IDataChecker
    {
        public readonly List<IDataChecker> _checkers = new List<IDataChecker>();
        private readonly ILog _log;

        public string Name => nameof(CheckersRepository);

        public CheckersRepository(ILog log)
        {
            _log = log;
        }

        public void AddChecker(IDataChecker checker)
        {
            if (checker == null)
                throw new ArgumentNullException(nameof(checker));

            _checkers.Add(checker);
        }

        public async Task CheckAndFixDataAsync()
        {
            await _log.WriteInfoAsync(
                nameof(CheckersRepository),
                nameof(CheckAndFixDataAsync),
                "Checking work has been started");

            foreach (var checker in _checkers)
            {
                try
                {
                    await _log.WriteInfoAsync(nameof(CheckersRepository), nameof(CheckAndFixDataAsync), $"{checker.Name} started.");
                    await checker.CheckAndFixDataAsync();
                    await _log.WriteInfoAsync(nameof(CheckersRepository), nameof(CheckAndFixDataAsync), $"{checker.Name} finished.");
                }
                catch (Exception exc)
                {
                    await _log.WriteErrorAsync("CheckersRepository.CheckAndFixDataAsync", checker.Name, exc);
                }
            }

            await _log.WriteInfoAsync(
                nameof(CheckersRepository),
                nameof(CheckAndFixDataAsync),
                "Checking work is finished");
        }

        public ReadOnlyCollection<IDataChecker> GetCheckers()
        {
            return _checkers.AsReadOnly();
        }
    }
}
