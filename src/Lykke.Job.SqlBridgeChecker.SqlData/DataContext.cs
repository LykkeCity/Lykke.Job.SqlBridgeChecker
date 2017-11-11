using Microsoft.EntityFrameworkCore;
using Lykke.Job.SqlBridgeChecker.SqlData.Models;

namespace Lykke.Job.SqlBridgeChecker.SqlData
{
    public class DataContext : DbContext
    {
        private readonly string _connectionString;

        public virtual DbSet<CashOperation> CashOperations { get; set; }
        public virtual DbSet<BalanceUpdate> BalanceUpdates { get; set; }
        public virtual DbSet<ClientBalanceUpdate> ClientBalanceUpdates { get; set; }
        public virtual DbSet<CashTransferOperation> CashTransferOperations { get; set; }
        public virtual DbSet<MarketOrder> MarketOrders { get; set; }
        public virtual DbSet<TradeInfo> TradeInfos { get; set; }
        public virtual DbSet<Candlestick> Candlesticks { get; set; }
        public virtual DbSet<LimitOrder> LimitOrders { get; set; }
        public virtual DbSet<LimitTradeInfo> LimitTradeInfos { get; set; }
        public virtual DbSet<TradeLogItem> Trades { get; set; }

        public DataContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(_connectionString, opts => opts.EnableRetryOnFailure());
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CashOperation>(entity =>
            {
                entity.Property(e => e.Id).HasColumnType($"varchar({CashOperation.MaxStringFieldsLength})");
                entity.Property(e => e.ClientId).IsRequired().HasColumnType($"varchar({CashOperation.MaxStringFieldsLength})");
                entity.Property(e => e.DateTime).HasColumnType("datetime");
                entity.Property(e => e.Volume).HasColumnType("float");
                entity.Property(e => e.Asset).IsRequired().HasColumnType($"varchar({CashOperation.MaxStringFieldsLength})");
                entity.ToTable("CashOperations");
            });

            modelBuilder.Entity<BalanceUpdate>(entity =>
            {
                entity.Property(e => e.Id).HasColumnType($"varchar({ BalanceUpdate.MaxStringFieldsLength})");
                entity.Property(e => e.Type).IsRequired().HasColumnType($"varchar({ BalanceUpdate.MaxStringFieldsLength})");
                entity.Property(e => e.Timestamp).HasColumnType("datetime");
                entity.HasMany(i => i.Balances).WithOne().HasForeignKey(i => i.BalanceUpdateId);
                entity.HasKey(i => i.Id);
                entity.ToTable("BalanceUpdates");
            });

            modelBuilder.Entity<ClientBalanceUpdate>(entity =>
            {
                entity.Property(e => e.Id).UseSqlServerIdentityColumn().HasColumnType("bigint");
                entity.Property(e => e.ClientId).IsRequired().HasColumnType($"varchar({ ClientBalanceUpdate.MaxStringFieldsLength})");
                entity.Property(e => e.Asset).IsRequired().HasColumnType($"varchar({ClientBalanceUpdate.MaxStringFieldsLength})");
                entity.Property(e => e.BalanceUpdateId).IsRequired().HasColumnType($"varchar({ BalanceUpdate.MaxStringFieldsLength})");
                entity.Property(e => e.OldBalance).HasColumnType("float");
                entity.Property(e => e.NewBalance).HasColumnType("float");
                entity.Property(e => e.OldReserved).HasColumnType("float");
                entity.Property(e => e.NewReserved).HasColumnType("float");
                entity.ToTable("ClientBalanceUpdates");
            });

            modelBuilder.Entity<CashTransferOperation>(entity =>
            {
                entity.Property(e => e.Id).HasColumnType($"varchar({CashTransferOperation.MaxStringFieldsLength})");
                entity.Property(e => e.FromClientId).IsRequired().HasColumnType($"varchar({CashTransferOperation.MaxStringFieldsLength})");
                entity.Property(e => e.ToClientId).IsRequired().HasColumnType($"varchar({CashTransferOperation.MaxStringFieldsLength})");
                entity.Property(e => e.DateTime).HasColumnType("datetime");
                entity.Property(e => e.Volume).HasColumnType("float");
                entity.Property(e => e.Asset).IsRequired().HasColumnType($"varchar({CashTransferOperation.MaxStringFieldsLength})");
                entity.ToTable("CashTransferOperations");
            });

            modelBuilder.Entity<MarketOrder>(entity =>
            {
                entity.Property(e => e.Id).HasColumnType($"varchar({ MarketOrder.MaxStringFieldsLength})");
                entity.Property(e => e.ExternalId).IsRequired().HasColumnType($"varchar({ MarketOrder.MaxStringFieldsLength})");
                entity.Property(e => e.AssetPairId).IsRequired().HasColumnType($"varchar({ MarketOrder.MaxStringFieldsLength})");
                entity.Property(e => e.ClientId).IsRequired().HasColumnType($"varchar({ MarketOrder.MaxStringFieldsLength})");
                entity.Property(e => e.Volume).HasColumnType("float");
                entity.Property(e => e.Price).HasColumnType("float");
                entity.Property(e => e.Status).IsRequired().HasColumnType($"varchar({ MarketOrder.MaxStringFieldsLength})");
                entity.Property(e => e.CreatedAt).HasColumnType("datetime");
                entity.Property(e => e.Registered).HasColumnType("datetime");
                entity.Property(e => e.MatchedAt).HasColumnType("datetime");
                entity.Property(e => e.Straight).HasColumnType("bit");
                entity.Property(e => e.ReservedLimitVolume).HasColumnType("float");
                entity.HasMany(i => i.Trades).WithOne().HasForeignKey(i => i.MarketOrderId);
                entity.HasKey(i => i.Id);
                entity.ToTable("MarketOrders");
            });

            modelBuilder.Entity<TradeInfo>(entity =>
            {
                entity.Property(e => e.Id).UseSqlServerIdentityColumn().HasColumnType("bigint");
                entity.Property(e => e.MarketOrderId).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.MarketClientId).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.MarketVolume).HasColumnType("float");
                entity.Property(e => e.MarketAsset).IsRequired().HasColumnType($"varchar({TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.Price).HasColumnType("float");
                entity.Property(e => e.LimitClientId).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.LimitVolume).HasColumnType("float");
                entity.Property(e => e.LimitAsset).IsRequired().HasColumnType($"varchar({TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.LimitOrderId).IsRequired().HasColumnType($"varchar({TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.LimitOrderExternalId).IsRequired().HasColumnType($"varchar({TradeInfo.MaxStringFieldsLength})");
                entity.ToTable("TradeInfos");
            });

            modelBuilder.Entity<Candlestick>(entity =>
            {
                entity.Property(e => e.Id).UseSqlServerIdentityColumn().HasColumnType("bigint");
                entity.Property(e => e.AssetPair).IsRequired().HasColumnType($"varchar({Candlestick.MaxStringFieldsLength})");
                entity.Property(e => e.IsAsk).HasColumnType("bit");
                entity.Property(e => e.High).HasColumnType("float");
                entity.Property(e => e.Low).HasColumnType("float");
                entity.Property(e => e.Open).HasColumnType("float");
                entity.Property(e => e.Close).HasColumnType("float");
                entity.Property(e => e.Start).HasColumnType("datetime");
                entity.Property(e => e.Finish).HasColumnType("datetime");
                entity.ToTable("Candlesticks2");
            });

            modelBuilder.Entity<LimitOrder>(entity =>
            {
                entity.Property(e => e.Id).HasColumnType($"varchar({ LimitOrder.MaxStringFieldsLength})");
                entity.Property(e => e.ExternalId).IsRequired().HasColumnType($"varchar({ LimitOrder.MaxStringFieldsLength})");
                entity.Property(e => e.AssetPairId).IsRequired().HasColumnType($"varchar({ LimitOrder.MaxStringFieldsLength})");
                entity.Property(e => e.ClientId).IsRequired().HasColumnType($"varchar({ LimitOrder.MaxStringFieldsLength})");
                entity.Property(e => e.Volume).HasColumnType("float");
                entity.Property(e => e.Price).HasColumnType("float");
                entity.Property(e => e.Status).IsRequired().HasColumnType($"varchar({ LimitOrder.MaxStringFieldsLength})");
                entity.Property(e => e.CreatedAt).HasColumnType("datetime");
                entity.Property(e => e.Registered).HasColumnType("datetime");
                entity.Property(e => e.LastMatchTime).HasColumnType("datetime");
                entity.Property(e => e.RemainingVolume).HasColumnType("float");
                entity.Property(e => e.Straight).HasColumnType("bit");
                entity.HasMany(i => i.Trades).WithOne().HasForeignKey(i => i.LimitOrderId);
                entity.HasKey(i => i.Id);
                entity.ToTable("LimitOrders");
            });

            modelBuilder.Entity<LimitTradeInfo>(entity =>
            {
                entity.Property(e => e.Id).UseSqlServerIdentityColumn().HasColumnType("bigint");
                entity.Property(e => e.LimitOrderId).IsRequired().HasColumnType($"varchar({ LimitTradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.ClientId).IsRequired().HasColumnType($"varchar({ LimitTradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.Volume).HasColumnType("float");
                entity.Property(e => e.Asset).IsRequired().HasColumnType($"varchar({LimitTradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.Price).HasColumnType("float");
                entity.Property(e => e.Timestamp).HasColumnType("datetime");
                entity.Property(e => e.OppositeClientId).IsRequired().HasColumnType($"varchar({ LimitTradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.OppositeVolume).HasColumnType("float");
                entity.Property(e => e.OppositeAsset).IsRequired().HasColumnType($"varchar({LimitTradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.OppositeOrderId).IsRequired().HasColumnType($"varchar({LimitTradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.OppositeOrderExternalId).IsRequired().HasColumnType($"varchar({LimitTradeInfo.MaxStringFieldsLength})");
                entity.ToTable("LimitTradeInfos");
            });

            modelBuilder.Entity<TradeLogItem>(entity =>
            {
                entity.Property(e => e.Id).UseSqlServerIdentityColumn().HasColumnType("bigint");
                entity.Property(e => e.TradeId).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.UserId).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.WalletId).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.OrderId).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.OrderType).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.Direction).IsRequired().HasColumnType($"varchar({ TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.Asset).IsRequired().HasColumnType($"varchar({TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.Volume).IsRequired().HasColumnType("decimal(18,8)");
                entity.Property(e => e.Price).IsRequired().HasColumnType("decimal(18,8)");
                entity.Property(e => e.DateTime).IsRequired().HasColumnType("datetime");
                entity.Property(e => e.OppositeOrderId).IsRequired().HasColumnType($"varchar({TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.OppositeAsset).HasColumnType($"varchar({TradeInfo.MaxStringFieldsLength})");
                entity.Property(e => e.OppositeVolume).HasColumnType("decimal(18,8)");
                entity.Property(e => e.IsHidden).HasColumnType("bit");
                entity.ToTable("Trades");
            });

            base.OnModelCreating(modelBuilder);
        }
    }
}
