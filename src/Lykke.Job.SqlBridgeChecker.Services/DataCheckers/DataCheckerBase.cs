﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Common;
using Common.Log;
using Lykke.Job.SqlBridgeChecker.Core.Services;
using Lykke.Job.SqlBridgeChecker.Core.Repositories;
using Lykke.Job.SqlBridgeChecker.AzureRepositories;
using Lykke.Job.SqlBridgeChecker.SqlData;

namespace Lykke.Job.SqlBridgeChecker.Services.DataCheckers
{
    public abstract class DataCheckerBase<TIn, TOut> : IDataChecker
        where TIn : TableEntity
        where TOut : class, IDbEntity, IValidatable
    {
        private readonly string _sqlConnectionString;
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
            _log = log;
        }

        public virtual async Task CheckAndFixDataAsync()
        {
            var items = await _repository.GetItemsFromYesterdayAsync();
            await _log.WriteInfoAsync(Name, nameof(CheckAndFixDataAsync), $"Fetched {items.Count} items from Azure.");
            var sqlItems = await ConvertItemsToSqlTypesAsync(items);
            await _log.WriteInfoAsync(Name, nameof(CheckAndFixDataAsync), $"Converted to {sqlItems.Count} items.");
            int modifiedCount = 0;
            int addedCount = 0;
            using (var dbContext = new DataContext(_sqlConnectionString))
            {
                foreach (var sqlItem in sqlItems)
                {
                    var fromSql = await FindInSqlDbAsync(sqlItem, dbContext);
                    if (fromSql == null)
                    {
                        if (!sqlItem.IsValid())
                            await _log.WriteWarningAsync(
                                nameof(DataCheckerBase<TIn, TOut>),
                                nameof(CheckAndFixDataAsync),
                                $"Found invalid object - {sqlItem.ToJson()}!");
                        await dbContext.Set<TOut>().AddAsync(sqlItem);
                        ++addedCount;
                    }
                    else if (await UpdateItemAsync(fromSql, sqlItem, dbContext))
                    {
                        dbContext.Set<TOut>().Update(fromSql);
                        ++modifiedCount;
                    }
                }
                await dbContext.SaveChangesAsync();
            }
            if (addedCount > 0)
                await LogAddedAsync(addedCount);
            if (modifiedCount > 0)
                await LogModifiedAsync(modifiedCount);
        }

        protected abstract Task<List<TOut>> ConvertItemsToSqlTypesAsync(IEnumerable<TIn> items);

        protected virtual async Task LogAddedAsync(int addedCount)
        {
            await _log.WriteWarningAsync(nameof(CheckAndFixDataAsync), Name, $"Added {addedCount} items.");
        }

        protected virtual async Task LogModifiedAsync(int modifiedCount)
        {
            await _log.WriteWarningAsync(nameof(CheckAndFixDataAsync), Name, $"Modified {modifiedCount} items.");
        }

        protected virtual async Task<TOut> FindInSqlDbAsync(TOut item, DataContext context)
        {
            object entityId = item.GetEntityId();
            if (entityId == null)
            {
                await _log.WriteWarningAsync(
                    Name,
                    nameof(FindInSqlDbAsync),
                    $"Found entity of type {typeof(TOut).Name} without Id!");
                return null;
            }
            var result = await context.Set<TOut>().FindAsync(entityId);
            if (result == null)
                await _log.WriteInfoAsync(
                    nameof(Name),
                    nameof(FindInSqlDbAsync),
                    $"Added {item.ToJson()}.");
            return result;
        }

        protected virtual async Task<bool> UpdateItemAsync(TOut inSql, TOut convertedItem, DataContext context)
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
    }
}
