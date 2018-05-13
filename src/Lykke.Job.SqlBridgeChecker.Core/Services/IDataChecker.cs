using System;
using System.Threading.Tasks;

namespace Lykke.Job.SqlBridgeChecker.Core.Services
{
    public interface IDataChecker
    {
        Task CheckAndFixDataAsync(DateTime start);

        string Name { get; }
    }
}
