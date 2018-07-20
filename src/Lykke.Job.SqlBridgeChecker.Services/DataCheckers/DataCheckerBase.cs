using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories.Abstractions;
using Lykke.Job.SqlBridgeChecker.SqlData;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public abstract class DataCheckerBase<TIn, TOut> : IDataChecker
        where TIn : TableEntity
        where TOut : class, IDbEntity, IValidatable
    {
        private readonly bool _canBeProcessedByBatches;

        private int _fetchedCount;
        private int _addedCount;
        private int _modifiedCount;

        protected readonly ITableEntityRepository<TIn> _repository;
        protected readonly ILog _log;
        protected readonly string _sqlConnectionString;

        public string Name => GetType().Name;

        public DataCheckerBase(
            string sqlConnecctionString,
            bool canBeProcessedByBatches,
            ITableEntityRepository<TIn> repository,
            ILog log)
        {
            _sqlConnectionString = sqlConnecctionString;
            _canBeProcessedByBatches = canBeProcessedByBatches;
            _repository = repository;
            _log = log.CreateComponentScope(Name);
        }

        public virtual async Task CheckAndFixDataAsync(DateTime start)
        {
            _fetchedCount = 0;
            _addedCount = 0;
            _modifiedCount = 0;

            if (_canBeProcessedByBatches)
            {
                await _repository.ProcessItemsFromYesterdayAsync(start, async batch =>
                {
                    using (var dbContext = new DataContext(_sqlConnectionString))
                    {
                        await ProcessBatchAsync(batch, dbContext);
                        await dbContext.SaveChangesAsync();
                    }
                });
            }
            else
            {
                using (var dbContext = new DataContext(_sqlConnectionString))
                {
                    var items = new List<TIn>();
                    await _repository.ProcessItemsFromYesterdayAsync(start, batch => { items.AddRange(batch); return Task.CompletedTask; });
                    await ProcessBatchAsync(items, dbContext);
                    await dbContext.SaveChangesAsync();
                }
            }

            if (_addedCount > 0)
                LogAdded(_addedCount);
            if (_modifiedCount > 0)
                LogModified(_modifiedCount);
            _log.WriteInfo(nameof(CheckAndFixDataAsync), "TotalFetched", $"Fetched {_fetchedCount} item(s).");

            ClearCaches(false);
        }

        protected abstract Task<List<TOut>> ConvertItemsToSqlTypesAsync(IEnumerable<TIn> items);

        protected virtual void ClearCaches(bool isDuringProcessing)
        {
        }

        protected virtual void LogAdded(int addedCount)
        {
            _log.WriteWarning(nameof(CheckAndFixDataAsync), "TotalAdded", $"Added {addedCount} item(s).");
        }

        protected virtual void LogModified(int modifiedCount)
        {
            _log.WriteWarning(nameof(CheckAndFixDataAsync), "TotalModified", $"Modified {modifiedCount} item(s).");
        }

        protected virtual async Task<TOut> FindInSqlDbAsync(TOut item, DataContext context)
        {
            object entityId = item.GetEntityId();
            if (entityId == null)
            {
                _log.WriteWarning(nameof(FindInSqlDbAsync), "Found", $"Found entity of type {typeof(TOut).Name} without Id!");
                return null;
            }
            var result = await context.Set<TOut>().FindAsync(entityId);
            if (result == null)
                _log.WriteInfo(nameof(FindInSqlDbAsync), $"{item.GetEntityId()}", $"Added {item.ToJson()}.");
            return result;
        }

        protected virtual bool UpdateItem(TOut inSql, TOut convertedItem, DataContext context)
        {
            return false;
        }

        protected bool AreEqual<T>(T? one, T? two)
            where T : struct, IEquatable<T>
        {
            if (!one.HasValue)
                return !two.HasValue || two.Value.Equals(default(T));
            if (!two.HasValue)
                return one.Value.Equals(default(T));
            return one.Value.Equals(two.Value);
        }

        protected bool AreEqual(DateTime? one, DateTime? two)
        {
            if (!one.HasValue)
                return !two.HasValue || two.Value.Equals(default(DateTime));
            if (!two.HasValue)
                return one.Value.Equals(default(DateTime));
            return one.Value.Subtract(two.Value).TotalMilliseconds <= 2;
        }

        private async Task ProcessBatchAsync(IEnumerable<TIn> items, DataContext dbContext)
        {
            if (!items.Any())
                return;

            int batchCount = items.Count();
            _fetchedCount += batchCount;

            _log.WriteInfo(nameof(CheckAndFixDataAsync), "Fetched", $"Fetched {batchCount} items from Azure.");
            var sqlItems = await ConvertItemsToSqlTypesAsync(items);
            _log.WriteInfo(nameof(CheckAndFixDataAsync), "Converted", $"Converted to {sqlItems.Count} items.");

            foreach (var sqlItem in sqlItems)
            {
                try
                {
                    bool isValid = sqlItem.IsValid();
                    var fromSql = await FindInSqlDbAsync(sqlItem, dbContext);
                    if (fromSql == null)
                    {
                        if (!isValid)
                            _log.WriteInfo(nameof(CheckAndFixDataAsync), "Adding Invalid", $"Adding invalid object - {sqlItem.ToJson()}!");
                        await dbContext.Set<TOut>().AddAsync(sqlItem);
                        ++_addedCount;
                    }
                    else
                    {
                        if (!isValid)
                        {
                            _log.WriteInfo(nameof(CheckAndFixDataAsync), "Skipping Invalid", $"Skipping invalid object - {sqlItem.ToJson()}!");
                        }
                        else if (UpdateItem(fromSql, sqlItem, dbContext))
                        {
                            dbContext.Set<TOut>().Update(fromSql);
                            ++_modifiedCount;
                        }
                    }
                }
                catch (Exception exc)
                {
                    _log.WriteError(Name, sqlItem.ToJson(), exc);
                }
            }

            ClearCaches(true);
        }
    }
}
