using System;
using Microsoft.EntityFrameworkCore;
using Autofac;
using Common.Log;
using AzureStorage.Tables;
using Lykke.SettingsReader;
using Lykke.Service.ClientAccount.Client;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.Services;
using Lykke.Job.SqlBridgeChecker.Services.DataCheckers;
using Lykke.Job.SqlBridgeChecker.AzureRepositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Models;
using Lykke.Job.SqlBridgeChecker.SqlData;
using Lykke.Job.SqlBridgeChecker.PeriodicalHandlers;

namespace Lykke.Job.SqlBridgeChecker.Modules
{
    public class JobModule : Module
    {
        private readonly TimeSpan _timeout = TimeSpan.FromMinutes(1);
        private readonly AppSettings _appSettings;
        private readonly IReloadingManager<SqlBridgeCheckerSettings> _settingsManager;
        private readonly ILog _log;

        public JobModule(
            AppSettings appSettings,
            IReloadingManager<SqlBridgeCheckerSettings> dbSettingsManager,
            ILog log)
        {
            _appSettings = appSettings;
            _log = log;
            _settingsManager = dbSettingsManager;
        }

        protected override void Load(ContainerBuilder builder)
        {
            using (var context = new DataContext(_appSettings.SqlBridgeCheckerJob.SqlDbConnectionString))
            {
                context.Database.SetCommandTimeout(TimeSpan.FromMinutes(10));
                context.Database.Migrate();
            }

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

            var clientAccountClient = new ClientAccountClient(_appSettings.ClientAccountServiceClient.ServiceUrl);
            builder.RegisterInstance(clientAccountClient)
                .As<IClientAccountClient>()
                .SingleInstance();

            var userWalletsMapper = new UserWalletsMapper(clientAccountClient, _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString);
            builder.RegisterInstance(userWalletsMapper)
                .As<IUserWalletsMapper>()
                .SingleInstance();

            RegisterCheckers(
                builder,
                userWalletsMapper,
                clientAccountClient);

            builder.RegisterType<PeriodicalHandler>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();
        }

        private void RegisterCheckers(
            ContainerBuilder builder,
            IUserWalletsMapper userWalletsMapper,
            IClientAccountClient clientAccountClient)
        {
            var checkersRepository = new CheckersRepository(userWalletsMapper, _log);
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
                _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString,
                userWalletsMapper,
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
                _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString,
                userWalletsMapper,
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
                _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString,
                userWalletsMapper,
                transfersRepository,
                _log);
            checkersRepository.AddChecker(transfersChecker);

            var marketOrdersStorage = AzureTableStorage<MarketOrderEntity>.Create(
                _settingsManager.ConnectionString(i => i.HMarketOrdersConnString),
                "MarketOrders",
                _log,
                _timeout);
            var marketOrdersRepository = new MarketOrdersRepository(marketOrdersStorage);
            var limitOrdersStorage = AzureTableStorage<LimitOrderEntity>.Create(
                _settingsManager.ConnectionString(i => i.HMarketOrdersConnString),
                "LimitOrders",
                _log,
                _timeout);
            var limitOrdersRepository = new LimitOrdersRepository(limitOrdersStorage, _log);
            var tradesStorage = AzureTableStorage<ClientTradeEntity>.Create(
                _settingsManager.ConnectionString(i => i.HTradesConnString),
                "Trades",
                _log,
                _timeout);
            var tradesRepository = new TradesRepository(tradesStorage, _log);

            var marketOrdersChecker = new MarketOrdersChecker(
                _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString,
                userWalletsMapper,
                marketOrdersRepository,
                limitOrdersRepository,
                tradesRepository,
                _log);
            checkersRepository.AddChecker(marketOrdersChecker);

            var limitOrdersChecker = new LimitOrdersChecker(
                _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString,
                userWalletsMapper,
                limitOrdersRepository,
                tradesRepository,
                marketOrdersRepository,
                _log);
            checkersRepository.AddChecker(limitOrdersChecker);

            
            var tradesChecker = new TradesChecker(
                _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString,
                tradesRepository,
                clientAccountClient,
                _log);
            checkersRepository.AddChecker(tradesChecker);

            var candlesticksStorage = AzureTableStorage<FeedHistoryEntity>.Create(
                _settingsManager.ConnectionString(i => i.HLiquidityConnString),
                "FeedHistory",
                _log,
                _timeout);
            var candlesticksRepository = new CandlestiсksRepository(candlesticksStorage);
            var candlesticksChecker = new CandlesticksChecker(
                _appSettings.SqlBridgeCheckerJob.SqlDbConnectionString,
                candlesticksRepository,
                _log);
            checkersRepository.AddChecker(candlesticksChecker);
        }
    }
}
