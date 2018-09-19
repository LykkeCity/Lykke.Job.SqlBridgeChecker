using System.Threading.Tasks;

namespace Lykke.Job.SqlBridgeChecker.Core.Services
{
    public interface IStartupManager
    {
        Task StartAsync();
    }
}
