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
        private readonly object _lockint = new object();
        private readonly object _locklong = new object();
        private readonly object _lockdouble = new object();
        private readonly object _lockdecimal = new object();
        private readonly object _lockbool = new object();
        private readonly object _lockdatetime = new object();
        private readonly object _lockstring = new object();
        private IDatabase _db;
        private static ConnectionMultiplexer _connector;
        private RedLockFactory _redlockFactory;
        private readonly string _redlock_key = "#i-am-redlock#";
        private readonly TimeSpan _redlock_expiry;
        private readonly TimeSpan _redlock_wait;
        private readonly TimeSpan _redlock_retry;
        public static readonly TimeSpan DefaultExpiry = TimeSpan.FromMinutes(5);

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
            return _db.KeyExists(key);
        }

        /// <summary>
        /// 移除指定键
        /// </summary>
        public bool Remove(string key)
        {
            return _db.KeyDelete(key);
        }

        /// <summary>
        /// 批量移除指定键
        /// </summary>
        public void RemoveAll(IEnumerable<string> keys)
        {
            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            _db.KeyDelete(redisKeys);
        }

        /// <summary>
        /// 获取指定key对应的object
        /// </summary>
        public T Get<T>(string key)
        {
            EnsureKey(key);

            var success = InnerGet(key, null, null, out T value);
            if (success)
                return value;

            return default(T);
        }

        /// <summary>
        /// 获取指定key对应的object
        /// </summary>
        public async Task<T> GetAsync<T>(string key)
        {
            EnsureKey(key);

            var res = await InnerGetAsync<T>(key, null, null);
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
            if (res.Success)
                return (T)res.Value;

            return default(T);
        }

        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan expiry)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, expiry);
            if (res.Success)
                return (T)res.Value;

            return default(T);
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入value并设定过期时间（默认60s）
        /// </summary>
        public bool AddObject<T>(string key, T value)
        {
            // link: https://stackoverflow.com/questions/25898333/how-to-add-generic-list-to-redis-via-stackexchange-redis
            return _db.StringSet(key, JsonConvert.SerializeObject(value), DefaultExpiry);
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入value并设定过期时间
        /// </summary>
        public bool AddObject<T>(string key, T value, DateTimeOffset expiry)
        {
            return _db.StringSet(key, JsonConvert.SerializeObject(value), expiry.DateTime.Subtract(DateTime.Now));
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入value并设定过期时间
        /// </summary>
        public bool AddObject<T>(string key, T value, TimeSpan expiry)
        {
            return _db.StringSet(key, JsonConvert.SerializeObject(value), expiry);
        }

        /// <summary>
        /// 批量获取指定键对应的object
        /// </summary>
        public IDictionary<string, T> GetAllObject<T>(IEnumerable<string> keys)
        {
            var redisKeys = keys.Select(x => (RedisKey)x).ToArray();
            var result = _db.StringGet(redisKeys);

            var ret = new Dictionary<string, T>();
            for (var index = 0; index < redisKeys.Length; index++)
            {
                var value = result[index];
                ret.Add(redisKeys[index], value == RedisValue.Null ? default(T) : JsonConvert.DeserializeObject<T>(value));
            }

            return ret;
        }

        /// <summary>
        /// 批量缓存object列表并设定过期时间（默认60s）
        /// </summary>
        public bool AddAllObject<T>(IList<(string key, T val)> items)
        {
            return AddAllObject(items, DefaultExpiry);
        }

        /// <summary>
        /// 批量缓存object列表并设定过期时间
        /// </summary>
        public bool AddAllObject<T>(IList<(string key, T val)> items, DateTimeOffset expiry)
        {
            return AddAllObject(items, expiry.DateTime.Subtract(DateTime.Now));
        }

        /// <summary>
        /// 批量缓存object列表并设定过期时间
        /// </summary>
        public bool AddAllObject<T>(IList<(string key, T val)> items, TimeSpan expiry)
        {
            var values = items
                .Select(item => new KeyValuePair<RedisKey, RedisValue>(item.key, JsonConvert.SerializeObject(item.val)))
                .ToArray();

            var ret = _db.StringSet(values);
            foreach (var value in values)
                _db.KeyExpire(value.Key, expiry);

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

            value = JsonConvert.DeserializeObject<T>(res);
            return true;
        }

        private ConcurrentDictionary<string, SemaphoreSlim> _locks = new ConcurrentDictionary<string, SemaphoreSlim>();
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

            return new AsyncResult { Success = true, Value = JsonConvert.DeserializeObject<T>(res) };
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
