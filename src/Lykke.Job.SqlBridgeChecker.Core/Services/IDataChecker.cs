using System.Threading.Tasks;

namespace Lykke.Job.SqlBridgeChecker.Core.Services
{
    public interface IDataChecker
    {
        Task CheckAndFixDataAsync();

        string Name { get; }
    }
}
