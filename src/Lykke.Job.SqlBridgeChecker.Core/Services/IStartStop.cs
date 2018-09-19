using Autofac;
using Common;

namespace Lykke.Job.SqlBridgeChecker.Core.Services
{
    public interface IStartStop : IStartable, IStopable
    {
    }
}
