using System;
using Autofac;
using Common.Log;
using AzureStorage.Tables;
using Lykke.SettingsReader;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.Core.Settings;
using Lykke.Job.SqlBridgeChecker.Services;
using Lykke.Job.SqlBridgeChecker.Services.DataCheckers;
using Lykke.Job.SqlBridgeChecker.AzureRepositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.PeriodicalHandlers;

namespace Lykke.Job.SqlBridgeChecker.Modules
{
    public class JobModule : Module
    {
        private readonly TimeSpan _timeout = TimeSpan.FromMinutes(1);
        private readonly SqlBridgeCheckerSettings _settings;
        private readonly IReloadingManager<SqlBridgeCheckerSettings> _settingsManager;
        private readonly ILog _log;

        public JobModule(
            SqlBridgeCheckerSettings settings,
            IReloadingManager<SqlBridgeCheckerSettings> dbSettingsManager,
            ILog log)
        {
            _settings = settings;
            _log = log;
            _settingsManager = dbSettingsManager;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>();

            RegisterCheckers(builder);

            builder.RegisterType<PeriodicalHandler>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();
        }

        private void RegisterCheckers(ContainerBuilder builder)
        {
            var checkersRepository = new CheckersRepository(_log);
            builder
                .RegisterInstance(checkersRepository)
                .As<IDataChecker>()
                .SingleInstance();

            var balanceUpdatesStorage = AzureTableStorage<ClientBalanceChangeLogRecordEntity>.Create(
                _settingsManager.ConnectionString(i => i.BalanceLogWriterMainConnection),
                "UpdateBalanceLog",
                _log,
                _timeout);
            var balanceUpdatesRepository = new BalanceUpdatesRepository(balanceUpdatesStorage);
            var balanceUpdatesChecker = new BalanceUpdatesChecker(
                _settings.SqlDbConnectionString,
                balanceUpdatesRepository,
                _log);
            checkersRepository.AddChecker(balanceUpdatesChecker);

            var cashOperationsStorage = AzureTableStorage<CashInOutOperationEntity>.Create(
                _settingsManager.ConnectionString(i => i.ClientPersonalInfoConnString),
                "OperationsCash",
                _log,
                _timeout);
            var cashOperationsRepository = new CashOperationsRepository(cashOperationsStorage);
            var cashOperationsChecker = new CashOperationsChecker(
                _settings.SqlDbConnectionString,
                cashOperationsRepository,
                _log);
            checkersRepository.AddChecker(cashOperationsChecker);

            var transfersStorage = AzureTableStorage<TransferEventEntity>.Create(
                _settingsManager.ConnectionString(i => i.ClientPersonalInfoConnString),
                "Transfers",
                _log,
                _timeout);
            var transfersRepository = new TransfersRepository(transfersStorage);
            var transfersChecker = new TransfersChecker(
                _settings.SqlDbConnectionString,
                transfersRepository,
                _log);
            checkersRepository.AddChecker(transfersChecker);

            var marketOrdersStorage = AzureTableStorage<MarketOrderEntity>.Create(
                _settingsManager.ConnectionString(i => i.HMarketOrdersConnString),
                "MarketOrders",
                _log,
                _timeout);
            var marketOrdersRepository = new MarketOrdersRepository(marketOrdersStorage);
            var tradesStorage = AzureTableStorage<ClientTradeEntity>.Create(
                _settingsManager.ConnectionString(i => i.HTradesConnString),
                "Trades",
                _log,
                _timeout);
            var tradesRepository = new TradesRepository(tradesStorage, _log);
            var marketOrdersChecker = new MarketOrdersChecker(
                _settings.SqlDbConnectionString,
                marketOrdersRepository,
                tradesRepository,
                _log);
            checkersRepository.AddChecker(marketOrdersChecker);

            var limitOrdersStorage = AzureTableStorage<LimitOrderEntity>.Create(
                _settingsManager.ConnectionString(i => i.HMarketOrdersConnString),
                "LimitOrders",
                _log,
                _timeout);
            var limitOrdersRepository = new LimitOrdersRepository(limitOrdersStorage, _log);
            var limitOrdersChecker = new LimitOrdersChecker(
                _settings.SqlDbConnectionString,
                limitOrdersRepository,
                tradesRepository,
                marketOrdersRepository,
                _log);
            checkersRepository.AddChecker(limitOrdersChecker);

            var tradesChecker = new TradesChecker(
                _settings.SqlDbConnectionString,
                tradesRepository,
                limitOrdersRepository,
                _log);
            checkersRepository.AddChecker(tradesChecker);

            var candlesticksStorage = AzureTableStorage<FeedHistoryEntity>.Create(
                _settingsManager.ConnectionString(i => i.HLiquidityConnString),
                "FeedHistory",
                _log,
                _timeout);
            var candlesticksRepository = new CandlestiсksRepository(candlesticksStorage);
            var candlesticksChecker = new CandlesticksChecker(
                _settings.SqlDbConnectionString,
                candlesticksRepository,
                _log);
            checkersRepository.AddChecker(candlesticksChecker);
        }
    }
}
