using LightCache.Common;
using LightCache.Remote;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LightCache.Hybird
{
    /// <summary>
    /// 混合缓存，同时包含一级本地缓存和二级远程缓存
    /// </summary>
    public sealed class HybirdCache : CacheService, IDisposable
    {
        LightCache _local;
        RemoteCache _remote;

        public HybirdCache(string host, long? capacity, int expiration = 60)
        {
            _remote = new RemoteCache(host);
            _local = new LightCache(capacity, expiration);
            Subscribe();
        }

        /// <summary>
        /// 判定指定键是否存在
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true存在，否则不存在</returns>
        public bool Exists(string key)
        {
            if (!_local.Exists(key))
                return _remote.Exists(key);

            return true;
        }

        /// <summary>
        /// 异步判定指定键是否存在
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true存在，否则不存在</returns>
        public Task<bool> ExistsAsync(string key)
        {
            if (!_local.Exists(key))
                return _remote.ExistsAsync(key);

            return Task.FromResult(true);
        }

        /// <summary>
        /// 移除指定键
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true操作成功，否则失败</returns>
        public bool Remove(string key)
        {
            // 说明：先删除remote的，尽可能减少缓存不一致的可能
            if (_remote.Remove(key))
                return _local.Remove(key);

            return false;
        }

        /// <summary>
        /// 异步移除指定键
        /// </summary>
        /// <param name="key">指定的键</param>
        /// <returns>true操作成功，否则失败</returns>
        public async Task<bool> RemoveAsync(string key)
        {
            if (await _remote.RemoveAsync(key).ConfigureAwait(false))
                return _local.Remove(key);

            return false;
        }

        /// <summary>
        /// 批量移除指定键
        /// </summary>
        /// <param name="keys">指定的键集合</param>
        /// <returns>true操作成功，否则失败</returns>
        public bool RemoveAll(IEnumerable<string> keys)
        {
            // TODO：返回值很麻烦啊
            _remote.RemoveAll(keys);
            _local.RemoveAll(keys);

            return true;
        }

        /// <summary>
        /// 异步批量移除指定键
        /// </summary>
        /// <param name="keys">指定的键集合</param>
        /// <returns>true操作成功，否则失败</returns>
        public async Task<bool> RemoveAllAsync(IEnumerable<string> keys)
        {
            // TODO：返回值很麻烦啊
            await _remote.RemoveAllAsync(keys).ConfigureAwait(false);
            _local.RemoveAll(keys);

            return true;
        }

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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                success = _remote.InnerGet(key, null, true/*这里真假无所谓，因为factory为null的*/,
                    null, out value, isSlidingExp);
                if (success)
                {
                    _local.Add(key, value);
                    return value;
                }
                return defaultVal;
            }

            return value;
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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                var asyncRes = await _remote.InnerGetAsync<T>(key, null, true/*这里真假无所谓，因为factory为null的*/,
                    null, isSlidingExp);
                if (asyncRes.Success)
                {
                    _local.Add(key, asyncRes.Value);
                    return asyncRes.Value;
                }
            }

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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                success = _remote.InnerGet(key, valFactory, true, null, out value);
                if (success)
                    _local.Add(key, value);
            }

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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                success = _remote.InnerGet(key, valFactory, true, expiresAt.ToTimeSpan(), out value);
                if (success)
                    _local.Add(key, value);
            }

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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                success = _remote.InnerGet(key, valFactory, true, expiresIn, out value);
                if (success)
                    _local.Add(key, value);
            }

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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                var asyncRes = await _remote.InnerGetAsync(key, valFactory, true, null);
                value = asyncRes.Value;
                if (asyncRes.Success)
                    _local.Add(key, value);
            }

            return value;
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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                var asyncRes = await _remote.InnerGetAsync(key, valFactory, true, expiresAt.ToTimeSpan());
                value = asyncRes.Value;
                if (asyncRes.Success)
                    _local.Add(key, value);
            }

            return value;
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

            var success = _local.InnerGet(key, null, null, null, null, out T value);
            if (!success)
            {
                var asyncRes = await _remote.InnerGetAsync(key, valFactory, true, expiresIn, true);
                value = asyncRes.Value;
                if (asyncRes.Success)
                    _local.Add(key, value);
            }

            return value;
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
            _local.Add(key, value);
            _remote.Add(key, value);

            return true;
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
            _local.Add(key, value);
            _remote.Add(key, value, expiresIn);

            return true;
        }

        /// <summary>
        /// 异步缓存一个值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="key">指定的键</param>
        /// <param name="value">要缓存的值</param>
        /// <returns>true为成功，否则失败</returns>
        public async Task<bool> AddAsync<T>(string key, T value)
        {
            _local.Add(key, value);
            await _remote.AddAsync(key, value);

            return true;
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
        public async Task<bool> AddAsync<T>(string key, T value, TimeSpan expiresIn)
        {
            _local.Add(key, value, expiresIn);
            await _remote.AddAsync(key, value, expiresIn);

            return true;
        }

        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <param name="isSlidingExp">指定的键是否是滑动过期的</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys, T defaultVal = default, bool isSlidingExp = false)
        {
            EnsureNotNull(nameof(keys), keys);

            var ret = new Dictionary<string, T>();
            foreach (var key in keys)
            {
                var success = _local.InnerGet(key, null, null, null, null, out T value);
                if (!success)
                {
                    success = _remote.InnerGet(key, null, true/*这里真假无所谓，因为factory为null的*/,
                        null, out value, isSlidingExp);
                    if (success)
                    {
                        _local.Add(key, value);
                        ret[key] = value;
                    }
                    else
                        ret[key] = defaultVal;
                }
            }

            return ret;
        }

        /// <summary>
        /// 批量获取指定键对应的缓存项
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="keys">指定的键集合</param>
        /// <param name="defaultVal">当键不存在时返回的指定值</param>
        /// <param name="isSlidingExp">指定的键是否是滑动过期的</param>
        /// <returns>一个字典包含键和对应的值</returns>
        public async Task<IDictionary<string, T>> GetAllAsync<T>(IEnumerable<string> keys, T defaultVal = default, bool isSlidingExp = false)
        {
            EnsureNotNull(nameof(keys), keys);

            var ret = new Dictionary<string, T>();
            foreach (var key in keys)
            {
                var success = _local.InnerGet(key, null, null, null, null, out T value);
                if (!success)
                {
                    var asyncRes = await _remote.InnerGetAsync<T>(key, null, true/*这里真假无所谓，因为factory为null的*/,
                        null, isSlidingExp);
                    value = asyncRes.Value;
                    if (asyncRes.Success)
                    {
                        _local.Add(key, value);
                        ret[key] = value;
                    }
                    else
                        ret[key] = defaultVal;
                }
            }

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
            _local.AddAll(items);
            _remote.AddAll(items);

            return true;
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
            _local.AddAll(items, expiresIn);
            _remote.AddAll(items, expiresIn);

            return true;
        }

        /// <summary>
        /// 异步批量缓存值
        /// </summary>
        /// <typeparam name="T">类型参数T</typeparam>
        /// <param name="items">要缓存的键值集合</param>
        /// <returns>true为成功，否则失败</returns>
        public async Task<bool> AddAllAsync<T>(IDictionary<string, T> items)
        {
            _local.AddAll(items);
            await _remote.AddAllAsync(items);

            return true;
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
            _local.AddAll(items, expiresIn);
            await _remote.AddAllAsync(items, expiresIn);

            return true;
        }

        #region 缓存更新通知
        private readonly string channel_key = "event:key:changed";
        private readonly string channel_prefix = $"{Environment.MachineName}";

        private void Subscribe()
        {
            // link: https://github.com/StackExchange/StackExchange.Redis/issues/859
            var channel = new RedisChannel(channel_key, RedisChannel.PatternMode.Literal);
            _remote.Subscribe(channel, p =>
            {
                if (!p.StartsWith(channel_prefix))
                    _local.Remove(p);
            });
        }

        public Task<long> NotifyChangeFor(string key)
        {
            EnsureKey(key);

            var channel = new RedisChannel(channel_key, RedisChannel.PatternMode.Literal);
            return _remote.PublishAsync(channel, $"{channel_prefix}:{key}");
        }
        #endregion

        public void Dispose()
        {
            _local.Dispose();
            _remote.Dispose();
        }
    }
}
