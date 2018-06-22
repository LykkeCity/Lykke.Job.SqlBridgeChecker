using System;
using System.Net.Http;
using Microsoft.EntityFrameworkCore;
using Autofac;
using Common.Log;
using AzureStorage.Tables;
using Lykke.Common;
using Lykke.SettingsReader;
using Lykke.Service.ClientAccount.Client;
using Lykke.Service.Assets.Client;
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
                context.Database.SetCommandTimeout(TimeSpan.FromMinutes(15));
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

            builder.RegisterResourcesMonitoring(_log);

            var clientAccountClient = new ClientAccountClient(_appSettings.ClientAccountServiceClient.ServiceUrl);
            builder.RegisterInstance(clientAccountClient)
                .As<IClientAccountClient>()
                .SingleInstance();

            var assetsServiceClient = new AssetsService(new Uri(_appSettings.AssetsServiceClient.ServiceUrl), new HttpClient());
            builder.RegisterInstance(assetsServiceClient)
                .As<IAssetsService>()
                .SingleInstance();

            RegisterCheckers(
                builder,
                clientAccountClient,
                assetsServiceClient);

            builder.RegisterType<PeriodicalHandler>()
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();
        }

        private void RegisterCheckers(
            ContainerBuilder builder,
            IClientAccountClient clientAccountClient,
            IAssetsService assetsServiceClient)
        {
            var checkersRepository = new CheckersRepository(_log);
            builder
                .RegisterInstance(checkersRepository)
                .As<IDataChecker>()
                .SingleInstance();

            var settings = _appSettings.SqlBridgeCheckerJob;

            var balanceUpdatesStorage = AzureTableStorage<ClientBalanceChangeLogRecordEntity>.Create(
                _settingsManager.ConnectionString(i => i.BalanceLogWriterMainConnection),
                "UpdateBalanceLog",
                _log,
                _timeout);
            var balanceUpdatesRepository = new BalanceUpdatesRepository(balanceUpdatesStorage);
            var balanceUpdatesChecker = new BalanceUpdatesChecker(
                settings.SqlDbConnectionString,
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
                settings.SqlDbConnectionString,
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
                settings.SqlDbConnectionString,
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
            var limitOrdersRepository = new LimitOrdersRepository(limitOrdersStorage);
            var tradesStorage = AzureTableStorage<ClientTradeEntity>.Create(
                _settingsManager.ConnectionString(i => i.HTradesConnString),
                "Trades",
                _log,
                _timeout);
            var tradesRepository = new TradesRepository(tradesStorage, _log);

            var marketOrdersChecker = new MarketOrdersChecker(
                settings.SqlDbConnectionString,
                marketOrdersRepository,
                limitOrdersRepository,
                tradesRepository,
                _log);
            checkersRepository.AddChecker(marketOrdersChecker);

            var limitOrdersChecker = new LimitOrdersChecker(
                settings.SqlDbConnectionString,
                limitOrdersRepository,
                tradesRepository,
                marketOrdersRepository,
                _log);
            checkersRepository.AddChecker(limitOrdersChecker);

            var tradesChecker = new TradesChecker(
                settings.SqlDbConnectionString,
                tradesRepository,
                clientAccountClient,
                _log);
            checkersRepository.AddChecker(tradesChecker);

            var candlesticksStorage = AzureTableStorage<FeedHistoryEntity>.Create(
                _settingsManager.ConnectionString(i => i.HLiquidityConnString),
                "FeedHistory",
                _log,
                _timeout);
            var candlesticksRepository = new CandlestiсksRepository(candlesticksStorage, assetsServiceClient, _log);
            var candlesticksChecker = new CandlesticksChecker(
                settings.SqlDbConnectionString,
                candlesticksRepository,
                _log);
            checkersRepository.AddChecker(candlesticksChecker);
        }
    }
}
