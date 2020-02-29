using LightCache.Remote;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LightCache
{
    /// <summary>
    /// LightCacher缓存交互接口
    /// </summary>
    public class LightCacher : CacheService
    {
        private readonly object _lockobj;
        private bool _useL2; // 是否启用二级缓存
        private MemoryCache _cache;
        private RemoteCache _remote;
        public RemoteCache Remote { get { return _remote; } }
        public readonly TimeSpan DefaultExpiry;

        public LightCacher(long? capacity, int expiration = 60, bool useL2 = false)
        {
            _lockobj = new object();
            DefaultExpiry = TimeSpan.FromSeconds(expiration);
            _useL2 = useL2;
            _cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = capacity });
            if (_useL2)
                _remote = new RemoteCache();
        }

        /// <summary>
        /// 如果想单独使用remotecache，则先初始化一波
        /// </summary>
        public void InitRemote()
        {
            if (_remote != null)
                _remote = new RemoteCache();
        }

        /// <summary>
        /// 判定指定键是否存在
        /// </summary>
        public bool Exists(string key)
        {
            return _cache.TryGetValue(key, out _);
        }

        /// <summary>
        /// 移除指定键
        /// </summary>
        public bool Remove(string key)
        {
            _cache.Remove(key);
            return true;
        }

        /// <summary>
        /// 批量移除指定键
        /// </summary>
        public bool RemoveAll(IEnumerable<string> keys)
        {
            foreach (var item in keys)
                _cache.Remove(item);
            return true;
        }

        #region object
        /// <summary>
        /// 获取指定key对应的缓存项
        /// </summary>
        public T Get<T>(string key)
        {
            EnsureKey(key);

            var success = InnerGet(key, null, null, null, out T value);
            if (success)
                return value;

            return default(T);
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定绝对过期时间
        /// 例：GetOrAdd("key", () => new object(), DateTimeOffset.Now.AddSeconds(60))
        /// </summary>
        /// <remarks>
        /// valFactory执行代价较大时应该调用GetOrAddAsync函数
        /// </remarks>
        public T GetOrAdd<T>(string key, Func<T> valFactory, DateTimeOffset absExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, absExp, null, out T value);

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期时间
        /// 例：GetOrAdd("key", () => new object(), TimeSpan.FromSeconds(60))
        /// </summary>
        /// <remarks>
        /// 在valFactory执行代价较大时应该调用GetOrAddAsync函数
        /// </remarks>
        public T GetOrAdd<T>(string key, Func<T> valFactory, TimeSpan slidingExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, null, slidingExp, out T value);

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定绝对过期时间
        /// 例：GetOrAdd("key", () => new object(), DateTimeOffset.Now.AddSeconds(60))
        /// </summary>
        /// <remarks>
        /// valFactory执行代价较大时应该调用GetOrAddAsync函数
        /// </remarks>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, DateTimeOffset absExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, absExp, null);
            if (res.Success)
                return (T)res.Value;

            return default(T);
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期时间
        /// 例：GetOrAdd("key", () => new object(), TimeSpan.FromSeconds(60))
        /// </summary>
        /// <remarks>
        /// 在valFactory执行代价较大时且开启L2时函数倾向于从二级缓存获取计算好的数据（尽量保证不给上层业务添负担）
        /// </remarks>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan slidingExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, null, slidingExp);
            if (res.Success)
                return (T)res.Value;

            return default(T);
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入value并设定滑动过期时间（默认60s）
        /// </summary>
        public void Add<T>(string key, T value)
        {
            EnsureKey(key);
            InnerSet(key, value, null, null);
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入value并设定绝对过期时间
        /// </summary>
        public void Add<T>(string key, T value, DateTimeOffset absExp)
        {
            EnsureKey(key);
            InnerSet(key, value, absExp, null);
        }

        /// <summary>
        /// 获取指定key对应的object，若不存在则填入value并设定滑动过期时间
        /// </summary>
        public void Add<T>(string key, T value, TimeSpan slidingExp)
        {
            EnsureKey(key);
            InnerSet(key, value, null, slidingExp);
        }

        /// <summary>
        /// 批量获取指定键对应的object
        /// </summary>
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys)
            where T : class
        {
            if (keys == null || keys.Any(p => string.IsNullOrEmpty(p)))
                throw new InvalidOperationException("keys集合不能为空且不允许出现空值的key");

            var ret = new Dictionary<string, T>();
            foreach (var key in keys)
                ;//ret[key] = InnerGet<T>(key, null, null, null);

            return ret;
        }

        /// <summary>
        /// 批量缓存object列表并设定滑动过期时间（默认60s）
        /// </summary>
        public bool AddAll<T>(IDictionary<string, T> items)
            where T : class
        {
            return AddAll(items, DefaultExpiry);
        }

        /// <summary>
        /// 批量缓存object列表并设定绝对过期时间
        /// </summary>
        public bool AddAll<T>(IDictionary<string, T> items, DateTimeOffset absExp)
            where T : class
        {
            return AddAll(items, absExp.DateTime.Subtract(DateTime.Now));
        }

        /// <summary>
        /// 批量缓存object列表并设定滑动过期时间
        /// </summary>
        public bool AddAll<T>(IDictionary<string, T> items, TimeSpan slidingExp)
            where T : class
        {
            if (items == null || items.Keys.Count == 0)
                throw new InvalidOperationException("集合不允许为空");

            foreach (var key in items.Keys)
                InnerSet(key, items[key], null, slidingExp);

            return true;
        }
        #endregion


        private bool InnerGet<T>(string key, Func<T> valFactory, DateTimeOffset? absExp, TimeSpan? slidingExp, out T value)
        {
            // memorycache本身是线程安全的
            var success = _cache.TryGetValue(key, out value);
            if (!success)
            {
                if (valFactory == null)
                {
                    value = default(T);
                    return false;
                }

                lock (string.Intern($"___cache_key_{key}")) // key处理下，避免意外lock
                {
                    success = _cache.TryGetValue(key, out value);
                    if (!success)
                    {
                        value = valFactory();

                        MemoryCacheEntryOptions cep = new MemoryCacheEntryOptions();
                        cep.Priority = CacheItemPriority.Normal;
                        cep.SetSize(1);
                        if (absExp.HasValue)
                            cep.AbsoluteExpiration = absExp.Value;
                        if (slidingExp.HasValue)
                            cep.SlidingExpiration = slidingExp.Value;

                        _cache.Set(key, value, cep);
                        return true;
                    }
                }
            }

            return true;
        }


        private ConcurrentDictionary<string, SemaphoreSlim> _locks = new ConcurrentDictionary<string, SemaphoreSlim>();
        private async Task<AsyncResult> InnerGetAsync<T>(string key, Func<Task<T>> valFactory, DateTimeOffset? absExp, TimeSpan? slidingExp)
        {
            // memorycache本身是线程安全的
            var success = _cache.TryGetValue(key, out object value);
            if (!success)
            {
                if (valFactory == null)
                    return new AsyncResult { Success = false, Value = default(T) };

                var mylock = _locks.GetOrAdd(key, k => new SemaphoreSlim(1, 1));
                await mylock.WaitAsync();
                try
                {
                    success = _cache.TryGetValue(key, out value);
                    if (!success)
                    {
                        value = await valFactory();

                        MemoryCacheEntryOptions cep = new MemoryCacheEntryOptions();
                        cep.Priority = CacheItemPriority.Normal;
                        cep.SetSize(1);
                        if (absExp.HasValue)
                            cep.AbsoluteExpiration = absExp.Value;
                        if (slidingExp.HasValue)
                            cep.SlidingExpiration = slidingExp.Value;

                        _cache.Set(key, value, cep);
                        return new AsyncResult { Success = true, Value = value };
                    }
                }
                finally
                {
                    mylock.Release();
                }
            }

            return new AsyncResult { Success = true, Value = value };
        }

        private void InnerSet(string key, object value, DateTimeOffset? absExp, TimeSpan? slidingExp)
        {
            MemoryCacheEntryOptions cep = new MemoryCacheEntryOptions();
            cep.Priority = CacheItemPriority.Normal;
            cep.SetSize(1);
            if (absExp.HasValue)
                cep.AbsoluteExpiration = absExp.Value;
            if (slidingExp.HasValue)
                cep.SlidingExpiration = slidingExp.Value;
            _cache.Set(key, value, cep);
        }

        public void Dispose()
        {
            _cache.Dispose();
            _remote.Dispose();
        }
    }
}
