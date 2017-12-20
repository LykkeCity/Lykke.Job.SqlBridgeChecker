using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Service.ClientAccount.Client;
using Lykke.Service.ClientAccount.Client.Models;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.Services
{
    public class UserWalletsMapper : IUserWalletsMapper
    {
        private readonly IClientAccountClient _clientAccountClient;
        private readonly string _sqlConnString;
        private readonly HashSet<string> _knownWalletIds = new HashSet<string>();
        private readonly HashSet<string> _knownUserIds = new HashSet<string>();

        public UserWalletsMapper(IClientAccountClient clientAccountClient, string sqlConnString)
        {
            _clientAccountClient = clientAccountClient;
            _sqlConnString = sqlConnString;
        }

        public void ClearCaches()
        {
            _knownUserIds.Clear();
            _knownWalletIds.Clear();
        }

        public async Task AddWalletsAsync(HashSet<string> walletIds)
        {
            var walletsDict = new Dictionary<string, WalletDtoModel>(walletIds.Count);
            foreach (var walletId in walletIds)
            {
                if (_knownWalletIds.Contains(walletId) || _knownUserIds.Contains(walletId))
                    continue;
                var wallet = await _clientAccountClient.GetWalletAsync(walletId);
                if (wallet == null)
                {
                    _knownUserIds.Add(walletId);
                    continue;
                }
                walletsDict[walletId] = wallet;
                _knownWalletIds.Add(walletId);
            }
            using (var dataContext = new DataContext(_sqlConnString))
            {
                foreach (var pair in walletsDict)
                {
                    var wallet = await dataContext.UserWallets.FindAsync(pair.Key);
                    if (wallet != null)
                    {
                        _knownWalletIds.Add(pair.Key);
                        continue;
                    }
                    dataContext.UserWallets.Add(
                        new UserWallet
                        {
                            Id = pair.Key,
                            UserId = pair.Value.ClientId,
                            Type = pair.Value.Type,
                        });
                }
                await dataContext.SaveChangesAsync();
            }
        }
    }
}
