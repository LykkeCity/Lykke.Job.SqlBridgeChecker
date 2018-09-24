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
        private const int _maxRetryCount = 5;

        private readonly List<IDataChecker> _checkers = new List<IDataChecker>();
        private readonly ILog _log;

        public string Name => nameof(CheckersRepository);

        public CheckersRepository(ILog log)
        {
            _log = log.CreateComponentScope(Name);
        }

        public void AddChecker(IDataChecker checker)
        {
            if (checker == null)
                throw new ArgumentNullException(nameof(checker));

            _checkers.Add(checker);
        }

        public async Task CheckAndFixDataAsync(DateTime start)
        {
            _log.WriteInfo(nameof(CheckAndFixDataAsync), "AllStart", "Checking work has been started");

            foreach (var checker in _checkers)
            {
                int retryCount = 0;
                do
                {
                    try
                    {
                        _log.WriteInfo(nameof(CheckAndFixDataAsync), "CheckerStart", $"{checker.Name} started.");
                        await checker.CheckAndFixDataAsync(start);
                        _log.WriteInfo(nameof(CheckAndFixDataAsync), "CheckerFinish", $"{checker.Name} finished.");
                        break;
                    }
                    catch (Exception exc)
                    {
                        _log.WriteWarning("CheckersRepository.CheckAndFixDataAsync", checker.Name, $"Retrying after facing this error: {exc.Message}");
                        ++retryCount;
                    }
                }
                while (retryCount <= _maxRetryCount);
                if (retryCount > _maxRetryCount)
                    _log.WriteError("CheckersRepository.CheckAndFixDataAsync", checker.Name, new Exception($"Couldn't successfully validate after {_maxRetryCount} retries"));
            }

            _log.WriteInfo(nameof(CheckAndFixDataAsync), "AllFinish", "Checking work is finished");
        }

        public ReadOnlyCollection<IDataChecker> GetCheckers()
        {
            return _checkers.AsReadOnly();
        }
    }
}
