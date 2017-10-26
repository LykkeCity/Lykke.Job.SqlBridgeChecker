using System.Threading.Tasks;

namespace Lykke.Job.SqlBridgeChecker.Core.Services
{
    public interface IShutdownManager
    {
        Task StopAsync();
    }
}