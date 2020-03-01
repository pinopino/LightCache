using Newtonsoft.Json;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace LightCache.Remote
{
    /// <summary>
    /// LiteCache中远程缓存（redis）交互接口
    /// </summary>
    public class RemoteCache : CacheService
    {
        private readonly object _lockobj = new object();
        private IDatabase _db;
        private static ConnectionMultiplexer _connector;
        private RedLockFactory _redlockFactory;
        private readonly string _redlock_key = "#i-am-redlock#";
        private readonly TimeSpan _redlock_expiry;
        private readonly TimeSpan _redlock_wait;
        private readonly TimeSpan _redlock_retry;
        private ConcurrentDictionary<string, SemaphoreSlim> _locks = new ConcurrentDictionary<string, SemaphoreSlim>();

        public RemoteCache()
        {
            // 初始化redis cient
            var host = ConfigurationManager.AppSettings["litecache_remote_redis"];
            _connector = ConnectionMultiplexer.Connect(host);
            _db = _connector.GetDatabase();

            // 初始化分布式锁相关
            _redlock_expiry = TimeSpan.FromSeconds(30);
            _redlock_wait = TimeSpan.FromSeconds(10);
            _redlock_retry = TimeSpan.FromSeconds(1);
            _redlockFactory = RedLockFactory.Create(new RedLockMultiplexer[] { new RedLockMultiplexer(_db.Multiplexer) });
        }

        /// <summary>
        /// 判定指定键是否存在
        /// </summary>
        public bool Exists(string key)
        {
            EnsureKey(key);

            return _db.KeyExists(key);
        }

        /// <summary>
        /// 判定指定键是否存在，异步
        /// </summary>
        public Task<bool> ExistsAsync(string key)
        {
            EnsureKey(key);

            return _db.KeyExistsAsync(key);
        }

        /// <summary>
        /// 移除指定键
        /// </summary>
        public bool Remove(string key)
        {
            EnsureKey(key);

            return _db.KeyDelete(key);
        }

        /// <summary>
        /// 移除指定键，异步
        /// </summary>
        public Task<bool> RemoveAsync(string key)
        {
            EnsureKey(key);

            return _db.KeyDeleteAsync(key);
        }

        /// <summary>
        /// 批量移除指定键
        /// </summary>
        public void RemoveAll(IEnumerable<string> keys)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            _db.KeyDelete(redisKeys);
        }

        /// <summary>
        /// 批量移除指定键，异步
        /// </summary>
        public Task RemoveAllAsync(IEnumerable<string> keys)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();

            return _db.KeyDeleteAsync(redisKeys);
        }

        /// <summary>
        /// 获取指定key对应的缓存项
        /// </summary>
        public T Get<T>(string key, TimeSpan? expiry = null)
        {
            EnsureKey(key);

            var success = InnerGet(key, null, expiry, out T value);
            if (success)
                return value;

            return default(T);
        }

        /// <summary>
        /// 获取指定key对应的缓存项，异步
        /// </summary>
        public async Task<T> GetAsync<T>(string key, TimeSpan? expiry)
        {
            EnsureKey(key);

            var res = await InnerGetAsync<T>(key, null, expiry);
            if (res.Success)
                return (T)res.Value;

            return default(T);
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入valFactory产生的值并设定过期时间
        /// 例：GetOrAdd("key", () => new object(), DateTimeOffset.Now.AddSeconds(60))
        /// </summary>
        public T GetOrAdd<T>(string key, Func<T> valFactory, TimeSpan expiry)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, expiry, out T value);

            return value;
        }

        public async Task<T> GetOrAddAsync<T>(string key, Func<T> valFactory, TimeSpan expiry)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, () => Task.FromResult(valFactory()), expiry);

            return (T)res.Value;
        }

        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan expiry)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, expiry);

            return (T)res.Value;
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入value并设定过期时间
        /// </summary>
        public bool Add<T>(string key, T value, TimeSpan? expiry)
        {
            // link: https://stackoverflow.com/questions/25898333/how-to-add-generic-list-to-redis-via-stackexchange-redis
            return _db.StringSet(key, JsonConvert.SerializeObject(value), expiry);
        }

        public Task<bool> AddAsync<T>(string key, T value, TimeSpan? expiry)
        {
            return _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiry);
        }

        /// <summary>
        /// 批量获取指定键对应的object
        /// </summary>
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

        public async Task<IDictionary<string, T>> GetAllAsync<T>(IEnumerable<string> keys, TimeSpan? expiry)
        {
            EnsureNotNull(nameof(keys), keys);

            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var res = await _db.StringGetAsync(redisKeys);

            var ret = new Dictionary<string, T>(StringComparer.Ordinal);
            for (var index = 0; index < redisKeys.Length; index++)
            {
                var value = res[index];
                ret.Add(redisKeys[index], value == RedisValue.Null ? default(T) : JsonConvert.DeserializeObject<T>(value));
            }

            if (expiry.HasValue)
                await UpdateExpiryAllAsync(keys.ToArray(), expiry.Value);

            return ret;
        }

        /// <summary>
        /// 批量缓存object列表并设定过期时间
        /// </summary>
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

        public async Task<bool> AddAllAsync<T>(IDictionary<string, T> items, TimeSpan? expiry)
        {
            var values = items
                .Select(p => new KeyValuePair<RedisKey, RedisValue>(p.Key, JsonConvert.SerializeObject(p.Value)))
                .ToArray();

            var ret = await _db.StringSetAsync(values);
            if (expiry.HasValue)
                Parallel.ForEach(values, async value => await _db.KeyExpireAsync(value.Key, expiry));

            return ret;
        }

        internal bool InnerGet<T>(string key, Func<T> valFactory, TimeSpan? expiry, out T value)
        {
            var res = _db.StringGet(key);
            if (!res.HasValue)
            {
                if (valFactory == null)
                {
                    value = default(T);
                    return false;
                }

                lock (string.Intern($"___cache_key_{key}")) // key处理下，避免意外lock
                {
                    value = valFactory();
                    _db.StringSet(key, JsonConvert.SerializeObject(value), expiry, When.NotExists, CommandFlags.FireAndForget);
                    return true;
                }
            }

            if (expiry.HasValue)
                _db.KeyExpire(key, expiry);
            value = JsonConvert.DeserializeObject<T>(res);

            return true;
        }

        internal async Task<AsyncResult> InnerGetAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan? expiry)
        {
            var res = await _db.StringGetAsync(key);
            if (!res.HasValue)
            {
                if (valFactory == null)
                    return new AsyncResult { Success = false, Value = default(T) };

                var mylock = _locks.GetOrAdd(key, k => new SemaphoreSlim(1, 1));
                await mylock.WaitAsync();
                try
                {
                    var value = await valFactory();
                    await _db.StringSetAsync(key, JsonConvert.SerializeObject(value), expiry, When.NotExists, CommandFlags.FireAndForget);
                    return new AsyncResult { Success = true, Value = value };
                }
                finally
                {
                    mylock.Release();
                }
            }

            if (expiry.HasValue)
                await _db.KeyExpireAsync(key, expiry);

            return new AsyncResult { Success = true, Value = JsonConvert.DeserializeObject<T>(res) };
        }

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
            _redlockFactory.Dispose();
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
