using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace LightCache.Remote
{
    /// <summary>
    /// LiteCache中远程缓存（redis）交互接口
    /// </summary>
    public class RemoteCache : CacheService, IDisposable
    {
        private IDatabase _db;
        private ConnectionMultiplexer _connector;
        private MemoryCache _locks;

        public IDatabase DB { get { return _db; } }
        public TimeSpan DefaultExpiry { get; private set; }
        public TimeSpan DefaultLockAbsoluteExpiry { get; private set; }

        public RemoteCache(string host, int expiration = 60)
        {
            _connector = ConnectionMultiplexer.Connect(host);
            _db = _connector.GetDatabase();
            _locks = new MemoryCache(new MemoryCacheOptions { SizeLimit = 1024 });
            DefaultExpiry = TimeSpan.FromSeconds(expiration);
            DefaultLockAbsoluteExpiry = TimeSpan.FromMinutes(5);
        }

        /// <summary>
        /// 判定指定键是否存在
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true存在，否则不存在</returns>
        public bool Exists(string key)
        {
            EnsureKey(key);

            return _db.KeyExists(key);
        }

        /// <summary>
        /// 异步判定指定键是否存在
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true存在，否则不存在</returns>
        public Task<bool> ExistsAsync(string key)
        {
            EnsureKey(key);

            return _db.KeyExistsAsync(key);
        }

        /// <summary>
        /// 移除指定键
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true操作成功，否则失败</returns>
        public bool Remove(string key)
        {
            EnsureKey(key);

            return _db.KeyDelete(key);
        }

        /// <summary>
        /// 异步移除指定键
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true操作成功，否则失败</returns>
        public Task<bool> RemoveAsync(string key)
        {
            EnsureKey(key);

            return _db.KeyDeleteAsync(key);
        }

        /// <summary>
        /// 批量移除指定键
        /// </summary>
        /// <param name="keys">指定的键集合</param>
        /// <returns>成功移除的key的个数</returns>
        public long RemoveAll(IEnumerable<string> keys)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            return _db.KeyDelete(redisKeys);
        }

        /// <summary>
        /// 异步批量移除指定键
        /// </summary>
        /// <param name="keys">指定的键集合</param>
        /// <returns>成功移除的key的个数</returns>
        public Task<long> RemoveAllAsync(IEnumerable<string> keys)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            return _db.KeyDeleteAsync(redisKeys);
        }

        // 说明：
        // redis中没有滑动过期的概念
        // link：https://redis.io/commands/expire
        /// <summary>
        /// 获取指定key对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <returns>若存在返回对应项，否则返回给定的默认值</returns>
        public T Get<T>(string key, T defaultVal = default)
        {
            EnsureKey(key);

            if (InnerGet(key, valFactory: null, useLock: false, expiry: null, out T value))
                return value;

            return defaultVal;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <returns>若存在返回对应项，否则返回给定的默认值</returns>
        public async Task<T> GetAsync<T>(string key, T defaultVal = default)
        {
            EnsureKey(key);

            var res = await InnerGetAsync<T>(key, null, useLock: false, expiry: null).ConfigureAwait(false);
            if (res.Success)
                return res.Value;

            return defaultVal;
        }

        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys, T defaultVal = default)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var res = _db.StringGet(redisKeys);

            var ret = new Dictionary<string, T>();
            for (var i = 0; i < redisKeys.Length; i++)
            {
                var value = res[i];
                ret.Add(redisKeys[i], value.HasValue ? JsonConvert.DeserializeObject<T>(value) : defaultVal);
            }

            return ret;
        }

        /// <summary>
        /// 异步批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public async Task<IDictionary<string, T>> GetAllAsync<T>(IEnumerable<string> keys, T defaultVal = default)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var res = await _db.StringGetAsync(redisKeys).ConfigureAwait(false);

            var ret = new Dictionary<string, T>();
            for (var i = 0; i < redisKeys.Length; i++)
            {
                var value = res[i];
                ret.Add(redisKeys[i], value.HasValue ? JsonConvert.DeserializeObject<T>(value) : defaultVal);
            }

            return ret;
        }

        /// <summary>
        /// 缓存一个值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool Add<T>(string key, T value, TimeSpan? expiry = null)
        {
            EnsureKey(key);

            // link: https://stackoverflow.com/questions/25898333/how-to-add-generic-list-to-redis-via-stackexchange-redis
            return _db.StringSet(key, JsonConvert.SerializeObject(value), expiry);
        }

        /// <summary>
        /// 异步缓存一个值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAsync<T>(string key, T value, TimeSpan? expiry = null)
        {
            EnsureKey(key);

            return _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiry);
        }

        /// <summary>
        /// 批量缓存值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, TimeSpan? expiry = null)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            var ret = _db.StringSet(values);
            if (ret)
            {
                foreach (var value in values)
                    _db.KeyExpire(value.Key, expiry, CommandFlags.FireAndForget);
            }

            return ret;
        }

        /// <summary>
        /// 异步批量缓存值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public async Task<bool> AddAllAsync<T>(IDictionary<string, T> items, TimeSpan? expiry = null)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            var ret = await _db.StringSetAsync(values).ConfigureAwait(false);
            if (ret)
            {
                foreach (var value in values)
                    _db.KeyExpire(value.Key, expiry, CommandFlags.FireAndForget);
            }

            return ret;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<T> valFactory, TimeSpan? expiry = null, bool useLock = true)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            if (InnerGet(key, valFactory, useLock, expiry, out T value))
                return value;

            return default;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan? expiry = null, bool useLock = true)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, useLock, expiry).ConfigureAwait(false);
            if (res.Success)
                return res.Value;

            return default;
        }

        internal bool InnerGet<T>(string key, Func<T> valFactory, bool useLock, TimeSpan? expiry, out T value)
        {
            // 说明：
            // 这里是一个典型的get-and-set，需要考虑两个地方的保护：
            // 1. set的那一下根据业务需要可能需要保护（redis WATCH），也可能不需要（此时最后一次set获胜）
            // 2. 当对应键不存在的时候valFactory被执行多次（这个看情况也会需要保护）
            //    两个层面可能存在多次执行：
            //    a. 进程内多个线程同时调用该方法
            //    b. 多个进程间的某个线程调用该方法
            //    前者走正常的本地线程同步即可，后者需要一个类似分布式锁的存在以便能协调多个进程（本地，跨机器都有可能）
            // （useLock参数是针对第二种情况设计的）
            var res = _db.StringGet(key);
            if (!res.HasValue)
            {
                if (valFactory == null)
                {
                    value = default;
                    return false;
                }

                if (useLock)
                {
                    var ret = RetryWithLock(key, valFactory, expiry);
                    value = ret.value;
                    return ret.success;
                }
                else
                {
                    lock (string.Intern(key))
                    {
                        res = _db.StringGet(key);
                        if (!res.HasValue)
                        {
                            value = valFactory();
                            return _db.StringSet(key, JsonConvert.SerializeObject(value), expiry, When.NotExists);
                        }
                        value = JsonConvert.DeserializeObject<T>(res);
                    }
                }
            }
            else
            {
                value = JsonConvert.DeserializeObject<T>(res);
            }

            return true;
        }

        internal async Task<AsyncResult<T>> InnerGetAsync<T>(string key, Func<Task<T>> valFactory, bool useLock, TimeSpan? expiry)
        {
            var res = await _db.StringGetAsync(key).ConfigureAwait(false);
            if (!res.HasValue)
            {
                if (valFactory == null)
                    return new AsyncResult<T> { Success = false, Value = default };

                if (useLock)
                {
                    var ret = await RetryWithLockAsync(key, valFactory, expiry).ConfigureAwait(false);
                    return new AsyncResult<T> { Success = ret.success, Value = ret.value };
                }
                else
                {
                    // 这里锁的获取仍然会存在valFactory被多次执行的可能，取决于业务逻辑这里
                    // 可能会有问题；
                    // 如果只是创建一个值的话那么是没什么的，但是麻烦就在于此处我们是要使用
                    // 锁来同步线程，这把锁对于并发过来的线程来说一定得是同一把才行；因此正确
                    // 的做法是得再在外面套一层lock :(
                    SemaphoreSlim mylock;
                    lock (string.Intern(key))
                    {
                        mylock = _locks.GetOrCreate(key, entry =>
                        {
                            var semaphore = new SemaphoreSlim(1, 1);
                            entry.Size = 1;
                            entry.SetSlidingExpiration(DefaultLockAbsoluteExpiry);
                            entry.RegisterPostEvictionCallback(PostEvictionCallback, semaphore);
                            return semaphore;
                        });
                    }
                    try
                    {
                        await mylock.WaitAsync().ConfigureAwait(false);

                        res = await _db.StringGetAsync(key).ConfigureAwait(false);
                        if (!res.HasValue)
                        {
                            var value = await valFactory().ConfigureAwait(false);
                            var success = await _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiry, When.NotExists)
                                .ConfigureAwait(false);

                            return new AsyncResult<T> { Success = success, Value = value };
                        }
                        return new AsyncResult<T> { Success = true, Value = JsonConvert.DeserializeObject<T>(res) };
                    }
                    finally
                    {
                        mylock.Release();
                    }
                }
            }

            return new AsyncResult<T> { Success = true, Value = JsonConvert.DeserializeObject<T>(res) };
        }

        #region lock
        private int _retryCount = 20;
        private TimeSpan _lockTimeout = TimeSpan.FromSeconds(10);
        // link：https://stackoverflow.com/questions/25127172/stackexchange-redis-locktake-lockrelease-usage
        private (bool success, T value) RetryWithLock<T>(string key, Func<T> valFactory, TimeSpan? expiry)
        {
            var lockName = $"{key}_lock___";
            // We tend to use the machine-name (or a munged version of the machine name if multiple processes
            // could be competing on the same machine).
            var lockToken = Environment.MachineName;
            var count = 0;
            while (count < _retryCount)
            {
                // check for access to cache object, trying to lock it
                if (!_db.LockTake(lockName, lockToken, _lockTimeout))
                {
                    count++;
                    Thread.Sleep(100); // sleep for 100 milliseconds for next lock try. you can play with that
                    continue;
                }

                // double check goes here
                var res = _db.StringGet(key);
                if (!res.HasValue)
                {
                    try
                    {
                        var val = valFactory();
                        var ret = _db.StringSet(key, JsonConvert.SerializeObject(val), expiry, When.NotExists);
                        return (ret, val);
                    }
                    finally
                    {
                        _db.LockRelease(lockName, lockToken);
                    }
                }
                else
                {
                    return (true, JsonConvert.DeserializeObject<T>(res));
                }
            }

            return (false, default);
        }

        private async Task<(bool success, T value)> RetryWithLockAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan? expiry)
        {
            var lockName = $"{key}_lock___";
            var lockToken = Environment.MachineName;
            var count = 0;
            while (count < _retryCount)
            {
                if (!_db.LockTake(lockName, lockToken, _lockTimeout))
                {
                    count++;
                    Thread.Sleep(100);
                    continue;
                }

                // double check goes here
                var res = await _db.StringGetAsync(key).ConfigureAwait(false);
                if (!res.HasValue)
                {
                    try
                    {
                        var val = await valFactory().ConfigureAwait(false);
                        var ret = await _db.StringSetAsync(key, JsonConvert.SerializeObject(val), expiry, When.NotExists)
                            .ConfigureAwait(false);
                        return (ret, val);
                    }
                    finally
                    {
                        _db.LockRelease(lockName, lockToken);
                    }
                }
                else
                {
                    return (true, JsonConvert.DeserializeObject<T>(res));
                }
            }

            return (false, default);
        }
        #endregion

        private static void PostEvictionCallback(object cacheKey, object cacheValue, EvictionReason evictionReason, object state)
        {
            var semaphore = state as SemaphoreSlim;
            if (semaphore != null)
                semaphore?.Dispose();
        }

        // for test only
        internal MemoryCache GetLockCache()
        {
            return _locks;
        }

        public void Dispose()
        {
            FlushDatabase();
            _connector?.Dispose();
            _locks?.Dispose();
        }

        private void FlushDatabase()
        {
            var endPoints = _db.Multiplexer.GetEndPoints();
            foreach (EndPoint endpoint in endPoints)
            {
                var server = _db.Multiplexer.GetServer(endpoint);
                server.FlushDatabase();
            }
        }
    }
}
