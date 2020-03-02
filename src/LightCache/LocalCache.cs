using LightCache.Remote;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace LightCache
{
    /// <summary>
    /// LightCacher缓存交互接口
    /// </summary>
    public class LightCacher : CacheService, IDisposable
    {
        private volatile int _remote_inited;
        private MemoryCache _cache;
        private RemoteCache _remote;
        public TimeSpan DefaultExpiry { get; private set; }
        private ConcurrentDictionary<string, SemaphoreSlim> _locks;

        public LightCacher(long? capacity, int expiration = 60)
        {
            DefaultExpiry = TimeSpan.FromSeconds(expiration);
            _locks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = capacity });
        }

        public RemoteCache Remote
        {
            private set { _remote = value; }
            get
            {
                if (_remote_inited == 1)
                    return _remote;
                throw new InvalidOperationException("RemoteCache还未初始化，请先调用InitRemote函数");
            }
        }

        /// <summary>
        /// 如果想单独使用remotecache，则先初始化一波
        /// </summary>
        public void InitRemote(string host)
        {
            if (Interlocked.Exchange(ref _remote_inited, 1) == 0)
            {
                if (Remote != null)
                    Remote = new RemoteCache(host);
            }
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
        /// <returns>若存在返回对应项，否则返回T类型的默认值</returns>
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
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="absExp">绝对过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<T> valFactory, DateTimeOffset absExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, absExp, null, out T value);

            return value;
        }

        /// <summary>
        /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="absExp">滑动过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public T GetOrAdd<T>(string key, Func<T> valFactory, TimeSpan slidingExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            InnerGet(key, valFactory, null, slidingExp, out T value);

            return value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定绝对过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="absExp">绝对过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, DateTimeOffset absExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, absExp, null);

            return (T)res.Value;
        }

        /// <summary>
        /// 异步获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="valFactory">缓存值的构造factory</param>
        /// <param name="slidingExp">滑动过期时间</param>
        /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
        public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan slidingExp)
        {
            EnsureKey(key);
            EnsureNotNull(nameof(valFactory), valFactory);

            var res = await InnerGetAsync(key, valFactory, null, slidingExp);

            return (T)res.Value;
        }

        /// <summary>
        /// 缓存一个值，并设定默认过期时间（滑动过期）
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        public void Add<T>(string key, T value)
        {
            EnsureKey(key);
            InnerSet(key, value, null, DefaultExpiry);
        }

        /// <summary>
        /// 缓存一个值，并设定绝对过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="absExp">绝对过期时间</param>
        public void Add<T>(string key, T value, DateTimeOffset absExp)
        {
            EnsureKey(key);
            InnerSet(key, value, absExp, null);
        }

        /// <summary>
        /// 缓存一个值，并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <param name="slidingExp">滑动过期时间</param>
        public void Add<T>(string key, T value, TimeSpan slidingExp)
        {
            EnsureKey(key);
            InnerSet(key, value, null, slidingExp);
        }

        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys)
            where T : class
        {
            EnsureNotNull(nameof(keys), keys);

            var ret = new Dictionary<string, T>();
            foreach (var key in keys)
            {
                InnerGet(key, null, null, null, out T value);
                ret[key] = value;
            }

            return ret;
        }

        /// <summary>
        /// 批量缓存值，并设定默认过期时间（滑动过期）
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items)
        {
            return AddAll(items, DefaultExpiry);
        }

        /// <summary>
        /// 批量缓存值，并设定绝对过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="absExp">绝对过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, DateTimeOffset absExp)
        {
            return AddAll(items, absExp.DateTime.Subtract(DateTime.Now));
        }

        /// <summary>
        /// 批量缓存值，并设定滑动过期时间
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <param name="slidingExp">滑动过期时间</param>
        /// <returns>true为成功，否则失败</returns>
        public bool AddAll<T>(IDictionary<string, T> items, TimeSpan slidingExp)
        {
            EnsureNotNull(nameof(items), items);

            foreach (var key in items.Keys)
                InnerSet(key, items[key], null, slidingExp);

            return true;
        }

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
            if (_locks != null)
            {
                foreach (var @lock in _locks)
                    @lock.Value.Dispose();
            }
            Remote.Dispose();
        }
    }
}
