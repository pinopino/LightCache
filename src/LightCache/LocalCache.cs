using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("LightCache.Test")]
namespace LightCache
{
    /// <summary>
    /// LightCache缓存交互接口
    /// </summary>
    public class LocalCache : CacheService, IDisposable
    {
        private MemoryCache _locks;
        private MemoryCache _cache;

        public TimeSpan DefaultSlidingExpiry { get; private set; }
        public TimeSpan DefaultAbsoluteExpiry { get; private set; }
        public TimeSpan DefaultLockAbsoluteExpiry { get; private set; }

        /// <summary>
        /// 实例化一个lightcacher对象
        /// </summary>
        /// <param name="capacity">缓存容量大小</param>
        /// <param name="expiration">默认滑动过期时间（单位秒）</param>
        public LocalCache(long capacity = 1024, int expiration = 60, int absoluteExpiration = 600)
        {
            _cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = capacity });
            _locks = new MemoryCache(new MemoryCacheOptions { SizeLimit = capacity });
            DefaultSlidingExpiry = TimeSpan.FromSeconds(expiration);
            DefaultAbsoluteExpiry = TimeSpan.FromSeconds(absoluteExpiration);
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

            return _cache.TryGetValue(key, out _);
        }

        /// <summary>
        /// 移除指定键
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true操作成功，否则失败</returns>
        public bool Remove(string key)
        {
            EnsureKey(key);

            _cache.Remove(key);

            return true;
        }

        /// <summary>
        /// 批量移除指定键
        /// </summary>
        /// <param name="keys">指定的键集合</param>
        /// <returns>true操作成功，否则失败</returns>
        public bool RemoveAll(IEnumerable<string> keys)
        {
            EnsureNotNull(nameof(keys), keys);

            foreach (var item in keys)
                _cache.Remove(item);

            return true;
        }

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

            if (!InnerGet(key, valFactory: null, absExp: null, slidingExp: null, priority: null, out T value))
                return defaultVal;

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定默认的滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<T> valFactory, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, absExp: null, DefaultSlidingExpiry, priority, out T value);

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定绝对过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="absExp">绝对过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<T> valFactory, DateTimeOffset absExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, absExp, slidingExp: null, priority, out T value);

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="slidingExp">滑动过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<T> valFactory, TimeSpan slidingExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, absExp: null, slidingExp, priority, out T value);

            return value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定默认的滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, absExp: null, DefaultSlidingExpiry, priority).ConfigureAwait(false);

            return res.Value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定绝对过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="absExp">绝对过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, DateTimeOffset absExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, absExp, slidingExp: null, priority).ConfigureAwait(false);

            return res.Value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="slidingExp">滑动过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan slidingExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, absExp: null, slidingExp, priority).ConfigureAwait(false);

            return res.Value;
        }

        /// <summary>
        /// 缓存一个值，并设定默认过期时间（滑动过期）
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="priority">缓存项的优先级</param>
        public void Add<T>(string key, T value, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);

            InnerSet(key, value, null, DefaultSlidingExpiry, priority);
        }

        /// <summary>
        /// 缓存一个值，并设定绝对过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="absExp">绝对过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        public void Add<T>(string key, T value, DateTimeOffset absExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);

            InnerSet(key, value, absExp, null, priority);
        }

        /// <summary>
        /// 缓存一个值，并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="slidingExp">滑动过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        public void Add<T>(string key, T value, TimeSpan slidingExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureKey(key);

            InnerSet(key, value, null, slidingExp, priority);
        }

        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys, T defaultVal = default, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureNotNull(nameof(keys), keys);

            var ret = new Dictionary<string, T>();
            foreach (var key in keys)
            {
                if (InnerGet(key, null, null, null, priority, out T value))
                    ret[key] = value;
                else
                    ret[key] = defaultVal;
            }

            return ret;
        }

        /// <summary>
        /// 批量缓存值，并设定默认过期时间（滑动过期）
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            return AddAll(items, DefaultSlidingExpiry, priority);
        }

        /// <summary>
        /// 批量缓存值，并设定绝对过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="absExp">绝对过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, DateTimeOffset absExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureNotNull(nameof(items), items);

            foreach (var key in items.Keys)
                InnerSet(key, items[key], absExp, null, priority);

            return true;
        }

        /// <summary>
        /// 批量缓存值，并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="slidingExp">滑动过期时间</param>
        /// <param name="priority">缓存项的优先级</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, TimeSpan slidingExp, CacheItemPriority priority = CacheItemPriority.Normal)
        {
            EnsureNotNull(nameof(items), items);

            foreach (var key in items.Keys)
                InnerSet(key, items[key], null, slidingExp, priority);

            return true;
        }

        internal bool InnerGet<T>(string key, Func<T> valFactory, DateTimeOffset? absExp, TimeSpan? slidingExp, CacheItemPriority? priority, out T value)
        {
            // memorycache本身是线程安全的
            var success = _cache.TryGetValue(key, out value);
            if (!success)
            {
                if (valFactory == null)
                {
                    value = default;
                    return false;
                }

                lock (string.Intern(key))
                {
                    success = _cache.TryGetValue(key, out value);
                    if (!success)
                    {
                        value = valFactory();
                        InnerSet(key, value, absExp, slidingExp, priority);
                    }
                }
            }

            return true;
        }

        internal async Task<AsyncResult<T>> InnerGetAsync<T>(string key, Func<Task<T>> valFactory, DateTimeOffset? absExp, TimeSpan? slidingExp, CacheItemPriority? priority)
        {
            // memorycache本身是线程安全的
            var success = _cache.TryGetValue(key, out T value);
            if (!success)
            {
                if (valFactory == null)
                    return new AsyncResult<T> { Success = false, Value = default };

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
                        entry.SetAbsoluteExpiration(DefaultLockAbsoluteExpiry);
                        entry.RegisterPostEvictionCallback(PostEvictionCallback, semaphore);
                        return semaphore;
                    });
                }
                try
                {
                    await mylock.WaitAsync().ConfigureAwait(false);

                    success = _cache.TryGetValue(key, out value);
                    if (!success)
                    {
                        value = await valFactory().ConfigureAwait(false);
                        InnerSet(key, value, absExp, slidingExp, priority);
                    }
                }
                finally
                {
                    mylock.Release();
                }
            }

            return new AsyncResult<T> { Success = true, Value = value };
        }

        internal void InnerSet(string key, object value, DateTimeOffset? absExp, TimeSpan? slidingExp, CacheItemPriority? priority)
        {
            MemoryCacheEntryOptions cep = new MemoryCacheEntryOptions();
            cep.SetSize(1);
            if (priority.HasValue)
                cep.Priority = priority.Value;
            if (absExp.HasValue)
                cep.AbsoluteExpiration = absExp.Value;
            else
                cep.AbsoluteExpirationRelativeToNow = DefaultAbsoluteExpiry; // 兜底，防止滑动过期导致缓存项永不过期
            if (slidingExp.HasValue)
                cep.SlidingExpiration = slidingExp.Value;

            _cache.Set(key, value, cep);
        }

        private DateTimeOffset GetDefaultFor(DateTimeOffset? absExp, bool utc = false)
        {
            return absExp.HasValue ?
                absExp.Value :
                ((utc ? DateTimeOffset.UtcNow : DateTimeOffset.Now) + DefaultSlidingExpiry);
        }

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

        // for test only
        internal void SetLockAbsoluteExpiration(TimeSpan expiration)
        {
            DefaultLockAbsoluteExpiry = expiration;
        }

        public void Dispose()
        {
            _cache?.Dispose();
            _locks?.Dispose();
        }
    }
}
