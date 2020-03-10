using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using LightCache.Common;

namespace LightCache.Remote
{
    /// <summary>
    /// LiteCache中远程缓存（redis）交互接口
    /// </summary>
    public class RemoteCache : CacheService, IDisposable
    {
        private IDatabase _db;
        private static ConnectionMultiplexer _connector;
        private ConcurrentDictionary<string, SemaphoreSlim> _locks = new ConcurrentDictionary<string, SemaphoreSlim>();

        internal RemoteCache(string host)
        {
            _connector = ConnectionMultiplexer.Connect(host);
            _db = _connector.GetDatabase();
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
        /// <returns>true操作成功，否则失败</returns>
        public void RemoveAll(IEnumerable<string> keys)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            _db.KeyDelete(redisKeys);
        }

        /// <summary>
        /// 异步批量移除指定键
        /// </summary>
        /// <param name="keys">指定的键集合</param>
        /// <returns>true操作成功，否则失败</returns>
        public Task RemoveAllAsync(IEnumerable<string> keys)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();

            return _db.KeyDeleteAsync(redisKeys);
        }

        /// <summary>
        /// 获取指定key对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="expiry">通过指定expiry以刷新缓存项的过期时间</param>
        /// <returns>若存在返回对应项，否则返回T类型的默认值</returns>
        public T Get<T>(string key, TimeSpan? expiry = null)
        {
            // 说明：
            // redis中没有滑动过期的概念，所以如果需要滑动过期就需要自己实现
            // 通常是在访问的时候通过keyexpire重新设置一次
            // 框架本身并不处理这个问题，因为这意味着需要记录哪些key含有滑动
            // 过期的语义，这些信息交给上层调用者去维护。下同。
            EnsureKey(key);

            InnerGet(key, null, expiry, out T value);

            return value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="expiry">通过指定expiry以刷新缓存项的过期时间</param>
        /// <returns>若存在返回对应项，否则返回T类型的默认值</returns>
        public async Task<T> GetAsync<T>(string key, TimeSpan? expiry)
        {
            EnsureKey(key);

            var res = await InnerGetAsync<T>(key, null, expiry).ConfigureAwait(false);

            return (T)res.Value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiryAt">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<T> valFactory, DateTimeOffset? expiryAt)
        {
            return GetOrAdd(key, valFactory, expiryAt.ToTimeSpan());
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiryAt">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public Task<T> GetOrAddAsync<T>(string key, Func<T> valFactory, DateTimeOffset? expiryAt)
        {
            return GetOrAddAsync(key, valFactory, expiryAt.ToTimeSpan());
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiryAt">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, DateTimeOffset? expiryAt)
        {
            return GetOrAddAsync(key, valFactory, expiryAt.ToTimeSpan());
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        /// <remarks>
        /// expiry此时并不包含滑动过期语义，仅出于方便的目的同时提供了DateTimeOffset与TimeSpan两种方式
        /// </remarks>
        public T GetOrAdd<T>(string key, Func<T> valFactory, TimeSpan? expiry)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, expiry, out T value);

            return value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        /// <remarks>
        /// expiry此时并不包含滑动过期语义，仅出于方便的目的同时提供了DateTimeOffset与TimeSpan两种方式
        /// </remarks>
        public async Task<T> GetOrAddAsync<T>(string key, Func<T> valFactory, TimeSpan? expiry)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, () => Task.FromResult(valFactory()), expiry)
                .ConfigureAwait(false);

            return (T)res.Value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        /// <remarks>
        /// expiry此时并不包含滑动过期语义，仅出于方便的目的同时提供了DateTimeOffset与TimeSpan两种方式
        /// </remarks>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan? expiry)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, expiry).ConfigureAwait(false);

            return (T)res.Value;
        }

        /// <summary>
        /// 缓存一个值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiryAt">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool Add<T>(string key, T value, DateTimeOffset? expiryAt)
        {
            return Add(key, value, expiryAt.ToTimeSpan());
        }

        /// <summary>
        /// 异步缓存一个值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiryAt">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAsync<T>(string key, T value, DateTimeOffset? expiryAt)
        {
            return AddAsync(key, value, expiryAt.ToTimeSpan());
        }

        /// <summary>
        /// 缓存一个值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        /// <remarks>
        /// expiry此时并不包含滑动过期语义，仅出于方便的目的同时提供了DateTimeOffset与TimeSpan两种方式
        /// </remarks>
        public bool Add<T>(string key, T value, TimeSpan? expiry)
        {
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
        /// <remarks>
        /// expiry此时并不包含滑动过期语义，仅出于方便的目的同时提供了DateTimeOffset与TimeSpan两种方式
        /// </remarks>
        public Task<bool> AddAsync<T>(string key, T value, TimeSpan? expiry)
        {
            return _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiry);
        }

        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="expiry">通过指定expiry以刷新缓存项的过期时间</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys, TimeSpan? expiry)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var res = _db.StringGet(redisKeys);

            var ret = new Dictionary<string, T>();
            for (var index = 0; index < redisKeys.Length; index++)
            {
                var value = res[index];
                ret.Add(redisKeys[index], value == RedisValue.Null ? default(T) : JsonConvert.DeserializeObject<T>(value));
            }

            if (expiry.HasValue)
                UpdateExpiryAll(keys.ToArray(), expiry.Value);

            return ret;
        }

        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="expiry">通过指定expiry以刷新缓存项的过期时间</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public async Task<IDictionary<string, T>> GetAllAsync<T>(IEnumerable<string> keys, TimeSpan? expiry)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var res = await _db.StringGetAsync(redisKeys).ConfigureAwait(false);

            var ret = new Dictionary<string, T>(StringComparer.Ordinal);
            for (var index = 0; index < redisKeys.Length; index++)
            {
                var value = res[index];
                ret.Add(redisKeys[index], value == RedisValue.Null ? default(T) : JsonConvert.DeserializeObject<T>(value));
            }

            if (expiry.HasValue)
                await UpdateExpiryAllAsync(keys.ToArray(), expiry.Value).ConfigureAwait(false);

            return ret;
        }

        /// <summary>
        /// 批量缓存值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiryAt">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, DateTimeOffset? expiryAt)
        {
            return AddAll(items, expiryAt.ToTimeSpan());
        }

        /// <summary>
        /// 异步批量缓存值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiryAt">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public Task<bool> AddAllAsync<T>(IDictionary<string, T> items, DateTimeOffset? expiryAt)
        {
            return AddAllAsync(items, expiryAt.ToTimeSpan());
        }

        /// <summary>
        /// 批量缓存值，并设定过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="expiry">过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        /// <remarks>
        /// expiry此时并不包含滑动过期语义，仅出于方便的目的同时提供了DateTimeOffset与TimeSpan两种方式
        /// </remarks>
        public bool AddAll<T>(IDictionary<string, T> items, TimeSpan? expiry)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            var ret = _db.StringSet(values);
            if (expiry.HasValue)
            {
                foreach (var value in values)
                    _db.KeyExpire(value.Key, expiry);
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
        /// <remarks>
        /// expiry此时并不包含滑动过期语义，仅出于方便的目的同时提供了DateTimeOffset与TimeSpan两种方式
        /// </remarks>
        public async Task<bool> AddAllAsync<T>(IDictionary<string, T> items, TimeSpan? expiry)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            var ret = await _db.StringSetAsync(values).ConfigureAwait(false);
            if (expiry.HasValue)
                Parallel.ForEach(values, async value =>
                    await _db.KeyExpireAsync(value.Key, expiry).ConfigureAwait(false));

            return ret;
        }

        internal bool InnerGet<T>(string key, Func<T> valFactory, TimeSpan? expiry, out T value)
        {
            lock (string.Intern($"___rcache_key_{key}")) // key处理下，避免意外lock
            {
                var res = _db.StringGet(key);
                if (!res.HasValue)
                {
                    if (valFactory == null)
                    {
                        value = default(T);
                        return false;
                    }

                    value = valFactory();
                    _db.StringSet(key, JsonConvert.SerializeObject(value), expiry, When.NotExists, CommandFlags.FireAndForget);
                    return true;
                }

                if (expiry.HasValue)
                    _db.KeyExpire(key, expiry);
                value = JsonConvert.DeserializeObject<T>(res);
            }

            return true;
        }

        internal async Task<AsyncResult> InnerGetAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan? expiry)
        {
            RedisValue res;
            var mylock = _locks.GetOrAdd(key, k => new SemaphoreSlim(1, 1));
            await mylock.WaitAsync();
            try
            {
                res = await _db.StringGetAsync(key);
                if (!res.HasValue)
                {
                    if (valFactory == null)
                        return new AsyncResult { Success = false, Value = default(T) };

                    var value = await valFactory();
                    await _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiry, When.NotExists, CommandFlags.FireAndForget);
                    return new AsyncResult { Success = true, Value = value };
                }

                if (expiry.HasValue)
                    await _db.KeyExpireAsync(key, expiry);
            }
            finally
            {
                mylock.Release();
            }

            return new AsyncResult { Success = true, Value = JsonConvert.DeserializeObject<T>(res) };
        }

        #region pub/sub支持
        internal void Subscribe(RedisChannel channel, Action<RedisValue> handler)
        {
            EnsureNotNull(nameof(handler), handler);

            var sub = _connector.GetSubscriber();
            // 事件的顺序不做保证，如果需要请注册channel的onmessage
            // link: https://stackexchange.github.io/StackExchange.Redis/PubSubOrder
            sub.Subscribe(channel, (redisChannel, value) => handler(value), CommandFlags.FireAndForget);
        }

        internal Task PublishAsync(RedisChannel channel, string key)
        {
            EnsureKey(key);

            var sub = _connector.GetSubscriber();
            return sub.PublishAsync(channel, key, CommandFlags.FireAndForget);
        }
        #endregion

        private IDictionary<string, bool> UpdateExpiryAll(string[] keys, TimeSpan expiry)
        {
            var ret = new Dictionary<string, bool>();
            for (int i = 0; i < keys.Length; i++)
                ret.Add(keys[i], UpdateExpiry(keys[i], expiry));

            return ret;
        }

        private async Task<IDictionary<string, bool>> UpdateExpiryAllAsync(string[] keys, TimeSpan expiry)
        {
            var ret = new Dictionary<string, bool>();
            for (int i = 0; i < keys.Length; i++)
                ret.Add(keys[i], await UpdateExpiryAsync(keys[i], expiry));

            return ret;
        }

        private bool UpdateExpiry(string key, TimeSpan expiry)
        {
            if (_db.KeyExists(key))
                return _db.KeyExpire(key, expiry);

            return false;
        }

        private async Task<bool> UpdateExpiryAsync(string key, TimeSpan expiry)
        {
            if (await _db.KeyExistsAsync(key))
                return await _db.KeyExpireAsync(key, expiry);

            return false;
        }

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
