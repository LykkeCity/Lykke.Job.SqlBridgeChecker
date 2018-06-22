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
        private readonly IUserWalletsMapper _userWalletsMapper;
        private readonly ILog _log;

        public string Name => nameof(CheckersRepository);

        public CheckersRepository(IUserWalletsMapper userWalletsMapper, ILog log)
        {
            _userWalletsMapper = userWalletsMapper;
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
                        _log.WriteError("CheckersRepository.CheckAndFixDataAsync", checker.Name, exc);
                    }
                    ++retryCount;
                }
                while (retryCount <= _maxRetryCount);
            }

            _userWalletsMapper.ClearCaches();

            _log.WriteInfo(nameof(CheckAndFixDataAsync), "AllFinish", "Checking work is finished");
        }

        public ReadOnlyCollection<IDataChecker> GetCheckers()
        {
            return _checkers.AsReadOnly();
        }
    }
}
