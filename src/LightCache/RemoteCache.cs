using LightCache.Common;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
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
        private ConcurrentDictionary<string, SemaphoreSlim> _locks;

        public IDatabase DB { get { return _db; } }

        internal RemoteCache(string host)
        {
            _connector = ConnectionMultiplexer.Connect(host);
            _db = _connector.GetDatabase();
            _locks = new ConcurrentDictionary<string, SemaphoreSlim>();
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
        // redis中没有滑动过期的概念，但是好在stackexchange.redis提供了一个自带的GetWithExpire方法
        // 通过该方法我们可以返回key对应的过期时间，如果需要支持滑动过期那么每次get之后再setexpire一下即可
        // link：https://redis.io/commands/expire
        // 下同。
        /// <summary>
        /// 获取指定key对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <param name="isSlidingExp">指定的键是否是滑动过期的</param>
        /// <returns>若存在返回对应项，否则返回给定的默认值</returns>
        public T Get<T>(string key, T defaultVal = default, bool isSlidingExp = false)
        {
            EnsureKey(key);

            if (InnerGet(key, null, false, null, out T value, isSlidingExp))
                return value;

            return defaultVal;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <param name="isSlidingExp">指定的键是否是滑动过期的</param>
        /// <returns>若存在返回对应项，否则返回给定的默认值</returns>
        public async Task<T> GetAsync<T>(string key, T defaultVal = default, bool isSlidingExp = false)
        {
            EnsureKey(key);

            var res = await InnerGetAsync<T>(key, null, false, null, isSlidingExp).ConfigureAwait(false);
            if (res.Success)
                return res.Value;

            return defaultVal;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<Task<T>> valFactory)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, false, null, out T value);

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定绝对过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiresAt">绝对过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<Task<T>> valFactory, DateTimeOffset expiresAt)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, false, expiresAt.ToTimeSpan(), out T value, false);

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiresIn">滑动过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<Task<T>> valFactory, TimeSpan expiresIn)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, false, expiresIn, out T value, true);

            return value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, false, null).ConfigureAwait(false);

            return res.Value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定绝对过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiresAt">绝对过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, DateTimeOffset expiresAt)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, false, expiresAt.ToTimeSpan()).ConfigureAwait(false);

            return res.Value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiresIn">滑动过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan expiresIn)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, false, expiresIn, true).ConfigureAwait(false);

            return res.Value;
        }

        /// <summary>
        /// 缓存一个值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <returns>true为成功，否则失败</returns>
        public bool Add<T>(string key, T value)
        {
            EnsureKey(key);
            return _db.StringSet(key, JsonConvert.SerializeObject(value), null);
        }

        /// <summary>
        /// 缓存一个值，并设定绝对过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiresAt">绝对过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool Add<T>(string key, T value, DateTimeOffset expiresAt)
        {
            // 说明：expiresAt转成timespan并不影响，这里不关心过期语义
            // 下同。
            return Add(key, value, expiresAt.ToTimeSpan());
        }

        /// <summary>
        /// 缓存一个值，并设定滑动过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiresIn">滑动过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool Add<T>(string key, T value, TimeSpan expiresIn)
        {
            EnsureKey(key);
            // link: https://stackoverflow.com/questions/25898333/how-to-add-generic-list-to-redis-via-stackexchange-redis
            return _db.StringSet(key, JsonConvert.SerializeObject(value), expiresIn);
        }

        /// <summary>
        /// 异步缓存一个值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAsync<T>(string key, T value)
        {
            EnsureKey(key);
            return _db.StringSetAsync(key, JsonConvert.SerializeObject(value), null);
        }

        /// <summary>
        /// 异步缓存一个值，并设定绝对过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiresAt">绝对过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAsync<T>(string key, T value, DateTimeOffset expiresAt)
        {
            return AddAsync(key, value, expiresAt.ToTimeSpan());
        }

        /// <summary>
        /// 异步缓存一个值，并设定滑动过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiresIn">滑动过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAsync<T>(string key, T value, TimeSpan expiresIn)
        {
            EnsureKey(key);
            return _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiresIn);
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
                ret.Add(redisKeys[i], value.HasValue ? defaultVal : JsonConvert.DeserializeObject<T>(value));
            }

            return ret;
        }

        // 说明：
        // 这里接口的样子本来应该是类似前面提供一个isSlidingExp参数表明这一批key是滑动过期的。
        // 无奈stackexchange.redis不支持批量StringGetWithExpiry，所以只能要求调用方传入明确的
        // 滑动过期时间。
        // 下同。
        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="expiresIn">通过指定expiry以刷新缓存项的过期时间</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys, TimeSpan expiresIn, T defaultVal = default)
        {
            var ret = GetAll(keys, defaultVal);

            UpdateExpiryAll(keys.ToArray(), expiresIn);

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

            var ret = new Dictionary<string, T>(StringComparer.Ordinal);
            for (var i = 0; i < redisKeys.Length; i++)
            {
                var value = res[i];
                ret.Add(redisKeys[i], value.HasValue ? defaultVal : JsonConvert.DeserializeObject<T>(value));
            }

            return ret;
        }

        /// <summary>
        /// 异步批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="expiresIn">通过指定expiry以刷新缓存项的过期时间</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public async Task<IDictionary<string, T>> GetAllAsync<T>(IEnumerable<string> keys, TimeSpan expiresIn, T defaultVal = default)
        {
            var ret = await GetAllAsync(keys, defaultVal).ConfigureAwait(false);

            UpdateExpiryAllAsync(keys.ToArray(), expiresIn);

            return ret;
        }

        /// <summary>
        /// 批量缓存值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            return _db.StringSet(values);
        }

        /// <summary>
        /// 批量缓存值，并设定绝对过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiresAt">绝对过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, DateTimeOffset expiresAt)
        {
            return AddAll(items, expiresAt.ToTimeSpan());
        }

        /// <summary>
        /// 批量缓存值，并设定滑动过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiresAt">滑动过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, TimeSpan expiresIn)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            var ret = _db.StringSet(values);
            foreach (var value in values)
                _db.KeyExpire(value.Key, expiresIn, CommandFlags.FireAndForget);

            return ret;
        }

        /// <summary>
        /// 异步批量缓存值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAllAsync<T>(IDictionary<string, T> items)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            return _db.StringSetAsync(values);
        }

        /// <summary>
        /// 异步批量缓存值，并设定绝对过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiresAt">绝对过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAllAsync<T>(IDictionary<string, T> items, DateTimeOffset expiresAt)
        {
            return AddAllAsync(items, expiresAt.ToTimeSpan());
        }

        /// <summary>
        /// 异步批量缓存值，并设定滑动过期
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiresIn">滑动过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public async Task<bool> AddAllAsync<T>(IDictionary<string, T> items, TimeSpan expiresIn)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            var ret = await _db.StringSetAsync(values).ConfigureAwait(false);
            foreach (var value in values)
                _db.KeyExpire(value.Key, expiresIn, CommandFlags.FireAndForget);

            return ret;
        }

        internal bool InnerGet<T>(string key, Func<Task<T>> valFactory, bool useLock, TimeSpan? expiry, out T value, bool isSlidingExp = false)
        {
            // 说明：
            // 这里有一个get-and-set，取决于具体场景在某些情况下是有可能存在race condition的。
            // 所以讲道理此处应该做并发保护，stackexchange.redis也提供了非常方便的事务支持。
            // 但是这里还是先分开get了，原因是如果真的存在的话那么可以省掉一次json序列化开销，
            // 而当发现不存在的时候我们再调用StringSet + When.NotExists即可。
            TimeSpan? exp = default;
            RedisValue val;
            if (isSlidingExp)
            {
                var res = _db.StringGetWithExpiry(key);
                exp = res.Expiry;
                val = res.Value;
            }
            else
            {
                var res = _db.StringGet(key);
                val = res;
            }

            if (!val.HasValue)
            {
                if (valFactory == null)
                {
                    value = default;
                    return false;
                }

                if (!useLock)
                {
                    value = valFactory().Result;
                    return _db.StringSet(key, JsonConvert.SerializeObject(value), expiry, When.NotExists);
                }
                else
                {
                    var ret = RetryWithLock(key, valFactory, expiry);
                    value = ret.value;
                    return ret.success;
                }
            }

            if (isSlidingExp)
            {
                // 进一步确定这是一个需要滑动过期的key（上层调用这可能记错了传了个isSlidingExp=true进来）
                if (exp.HasValue)
                    _db.KeyExpire(key, exp.Value, CommandFlags.FireAndForget);
            }
            value = JsonConvert.DeserializeObject<T>(val);

            return true;
        }

        internal async Task<AsyncResult<T>> InnerGetAsync<T>(string key, Func<Task<T>> valFactory, bool useLock, TimeSpan? expiry, bool isSlidingExp = false)
        {
            TimeSpan? exp = default;
            RedisValue val;
            if (isSlidingExp)
            {
                var res = await _db.StringGetWithExpiryAsync(key);
                exp = res.Expiry;
                val = res.Value;
            }
            else
            {
                var res = await _db.StringGetAsync(key);
                val = res;
            }

            if (!val.HasValue)
            {
                if (valFactory == null)
                    return new AsyncResult<T> { Success = false, Value = default };

                if (!useLock)
                {
                    var value = await valFactory();
                    var success = await _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiry, When.NotExists);
                    return new AsyncResult<T> { Success = success, Value = value };
                }
                else
                {
                    var ret = await RetryWithLockAsync(key, valFactory, expiry);
                    return new AsyncResult<T> { Success = ret.success, Value = ret.value };
                }
            }

            if (isSlidingExp)
            {
                // 进一步确定这是一个需要滑动过期的key（上层调用这可能记错了传了个isSlidingExp=true进来）
                if (exp.HasValue)
                    _db.KeyExpire(key, exp.Value, CommandFlags.FireAndForget);
            }

            return new AsyncResult<T> { Success = true, Value = JsonConvert.DeserializeObject<T>(val) };
        }

        private void UpdateExpiryAll(string[] keys, TimeSpan expiresIn)
        {
            for (var i = 0; i < keys.Length; i++)
                _db.KeyExpire(keys[i], expiresIn, CommandFlags.FireAndForget);
        }

        private void UpdateExpiryAllAsync(string[] keys, TimeSpan expiresIn)
        {
            for (var i = 0; i < keys.Length; i++)
                _db.KeyExpireAsync(keys[i], expiresIn, CommandFlags.FireAndForget);
        }

        #region lock
        private TimeSpan _lockTimeout;
        // link：https://stackoverflow.com/questions/25127172/stackexchange-redis-locktake-lockrelease-usage
        private (bool success, T value) RetryWithLock<T>(string key, Func<Task<T>> valFactory, TimeSpan? expiry)
        {
            var lockName = $"{key}_lock___";
            var lockToken = Environment.MachineName;
            var count = 0;
            while (count < 5)
            {
                //check for access to cache object, trying to lock it
                if (!_db.LockTake(lockName, lockToken, _lockTimeout))
                {
                    count++;
                    Thread.Sleep(100); //sleep for 100 milliseconds for next lock try. you can play with that
                    continue;
                }

                var res = _db.StringGet(key);
                if (res.HasValue)
                    return (true, JsonConvert.DeserializeObject<T>(res));

                try
                {
                    var val = valFactory().Result;
                    return (_db.StringSet(key, JsonConvert.SerializeObject(val), expiry, When.NotExists), val);
                }
                finally
                {
                    _db.LockRelease(lockName, lockToken);
                }
            }

            return (false, default);
        }

        private async Task<(bool success, T value)> RetryWithLockAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan? expiry)
        {
            var lockName = $"{key}_lock___";
            var lockToken = Environment.MachineName;
            var count = 0;
            while (count < 5)
            {
                if (!_db.LockTake(lockName, lockToken, _lockTimeout))
                {
                    count++;
                    Thread.Sleep(100);
                    continue;
                }

                var res = await _db.StringGetAsync(key);
                if (res.HasValue)
                    return (true, JsonConvert.DeserializeObject<T>(res));

                try
                {
                    var val = await valFactory();
                    return (await _db.StringSetAsync(key, JsonConvert.SerializeObject(val), expiry, When.NotExists), val);
                }
                finally
                {
                    _db.LockRelease(lockName, lockToken);
                }
            }

            return (false, default);
        }
        #endregion

        #region Pub/Sub支持
        internal void Subscribe(RedisChannel channel, Action<RedisValue> handler)
        {
            EnsureNotNull(nameof(handler), handler);

            var sub = _connector.GetSubscriber();
            // 事件的顺序不做保证，如果需要请注册channel的onmessage
            // link: https://stackexchange.github.io/StackExchange.Redis/PubSubOrder
            sub.Subscribe(channel, (redisChannel, value) => handler(value));
        }

        internal Task<long> PublishAsync(RedisChannel channel, string key)
        {
            EnsureKey(key);

            var sub = _connector.GetSubscriber();
            return sub.PublishAsync(channel, key);
        }
        #endregion

        public void Dispose()
        {
            FlushDatabase();
            _db.Multiplexer.GetSubscriber().UnsubscribeAll();
            if (_locks != null)
            {
                foreach (var @lock in _locks)
                    @lock.Value.Dispose();
            }
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
