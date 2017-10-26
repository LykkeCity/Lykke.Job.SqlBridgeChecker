using System.Collections.ObjectModel;

namespace Lykke.Job.SqlBridgeChecker.Core.Services
{
    public interface ICheckersRepository
    {
        ReadOnlyCollection<IDataChecker> GetCheckers();

        void AddChecker(IDataChecker checker);
    }
}
