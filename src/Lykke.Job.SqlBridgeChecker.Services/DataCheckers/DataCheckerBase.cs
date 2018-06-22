﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
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
        protected internal readonly string _sqlConnectionString;
        
        protected readonly ITableEntityRepository<TIn> _repository;
        protected readonly ILog _log;

        public string Name => GetType().Name;

        public DataCheckerBase(
            string sqlConnecctionString,
            ITableEntityRepository<TIn> repository,
            ILog log)
        {
            _sqlConnectionString = sqlConnecctionString;
            _repository = repository;
            _log = log.CreateComponentScope(Name);
        }

        public virtual async Task CheckAndFixDataAsync(DateTime start)
        {
            await _repository.ProcessItemsFromYesterdayAsync(start, ProcessBatchAsync);
        }

        protected abstract Task<List<TOut>> ConvertItemsToSqlTypesAsync(IEnumerable<TIn> items);

        protected virtual void ClearCaches()
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

        private async Task ProcessBatchAsync(IEnumerable<TIn> items)
        {
            if (!items.Any())
                return;

            _log.WriteInfo(nameof(CheckAndFixDataAsync), "Fetched", $"Fetched {items.Count()} items from Azure.");
            var sqlItems = await ConvertItemsToSqlTypesAsync(items);
            _log.WriteInfo(nameof(CheckAndFixDataAsync), "Converted", $"Converted to {sqlItems.Count} items.");

            int modifiedCount = 0;
            int addedCount = 0;
            using (var dbContext = new DataContext(_sqlConnectionString))
            {
                dbContext.Database.SetCommandTimeout(TimeSpan.FromMinutes(15));

                foreach (var sqlItem in sqlItems)
                {
                    try
                    {
                        var fromSql = await FindInSqlDbAsync(sqlItem, dbContext);
                        if (fromSql == null)
                        {
                            if (!sqlItem.IsValid())
                                _log.WriteInfo(nameof(CheckAndFixDataAsync), "Invalid", $"Found invalid object - {sqlItem.ToJson()}!");
                            await dbContext.Set<TOut>().AddAsync(sqlItem);
                            ++addedCount;
                        }
                        else if (UpdateItem(fromSql, sqlItem, dbContext))
                        {
                            dbContext.Set<TOut>().Update(fromSql);
                            ++modifiedCount;
                        }
                    }
                    catch (Exception exc)
                    {
                        _log.WriteError(Name, sqlItem.ToJson(), exc);
                    }
                }
                await dbContext.SaveChangesAsync();
            }
            if (addedCount > 0)
                LogAdded(addedCount);
            if (modifiedCount > 0)
                LogModified(modifiedCount);

            ClearCaches();
        }
    }
}
