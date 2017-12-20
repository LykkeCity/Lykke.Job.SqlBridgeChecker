using Lykke.Job.SqlBridgeChecker.Core.Repositories;

namespace Lykke.Job.SqlBridgeChecker.SqlData.Models
{
    public class UserWallet : IValidatable, IDbEntity
    {
        public static int MaxStringFieldsLength { get { return 255; } }

        public string Id { get; set; }

        public string UserId { get; set; }

        public object GetEntityId()
        {
            return Id;
        }

        public bool IsValid()
        {
            return true;
        }
    }
}
