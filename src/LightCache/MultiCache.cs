using LightCache.Remote;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LightCache
{
    public partial class LightCacher
    {
        public class MultiCache
        {
            LightCacher _local;
            RemoteCache _remote;

            public MultiCache(LightCacher local, RemoteCache remote)
            {
                _local = local;
                _remote = remote;
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
                // 即使本地remove失败也需要尝试remove远程的，
                // 因此只要不抛出异常就认为是成功，下同
                _local.Remove(key);
                _remote.Remove(key);

                return true;
            }

            /// <summary>
            /// 异步移除指定键
            /// </summary>
            /// <param name="key">指定的键</param>
            /// <returns>true操作成功，否则失败</returns>
            public async Task<bool> RemoveAsync(string key)
            {
                _local.Remove(key);
                await _remote.RemoveAsync(key);

                return true;
            }

            /// <summary>
            /// 批量移除指定键
            /// </summary>
            /// <param name="keys">指定的键集合</param>
            /// <returns>true操作成功，否则失败</returns>
            public bool RemoveAll(IEnumerable<string> keys)
            {
                _local.RemoveAll(keys);
                _remote.RemoveAll(keys);

                return true;
            }

            /// <summary>
            /// 异步批量移除指定键
            /// </summary>
            /// <param name="keys">指定的键集合</param>
            /// <returns>true操作成功，否则失败</returns>
            public async Task<bool> RemoveAllAsync(IEnumerable<string> keys)
            {
                _local.RemoveAll(keys);
                await _remote.RemoveAllAsync(keys);

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
                var success = _local.InnerGet(key, null, null, null, out T value);
                if (!success)
                {
                    success = _remote.InnerGet(key, null, null, out value);
                    if (success)
                    {
                        _local.Add(key, value);
                        return value;
                    }
                    return default(T);
                }

                return value;
            }

            /// <summary>
            /// 异步获取指定key对应的缓存项
            /// </summary>
            /// <typeparam name="T">类型参数T</typeparam>
            /// <param name="key">指定的键</param>
            /// <param name="expiry">过期时间</param>
            /// <returns>若存在返回对应项，否则返回T类型的默认值</returns>
            public async Task<T> GetAsync<T>(string key)
            {
                var success = _local.InnerGet(key, null, null, null, out T value);
                if (!success)
                {
                    success = await Task.Run(() => _remote.InnerGet(key, null, null, out value));
                    if (success)
                    {
                        _local.Add(key, value);
                        return value;
                    }
                    return default(T);
                }

                return value;
            }

            /// <summary>
            /// 获取指定key对应的缓存项，若不存在则填入valFactory产生的值并设定过期时间
            /// </summary>
            /// <typeparam name="T">类型参数T</typeparam>
            /// <param name="key">指定的键</param>
            /// <param name="valFactory">缓存值的构造factory</param>
            /// <param name="expiry">过期时间</param>
            /// <returns>若存在返回对应项，否则缓存构造的值并返回</returns>
            public T GetOrAdd<T>(string key, Func<T> valFactory, TimeSpan expiry)
            {
                var success = _local.InnerGet(key, null, null, null, out T value);
                if (!success)
                {
                    success = _remote.InnerGet(key, valFactory, expiry, out value);
                    if (success)
                    {
                        _local.Add(key, value, expiry);
                        return value;
                    }
                    return default(T);
                }

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
            public async Task<T> GetOrAddAsync<T>(string key, Func<T> valFactory, TimeSpan expiry)
            {
                var success = _local.InnerGet(key, null, null, null, out T value);
                if (!success)
                {
                    success = await Task.Run(() => _remote.InnerGet(key, valFactory, expiry, out value));
                    if (success)
                    {
                        _local.Add(key, value, expiry);
                        return value;
                    }
                    return default(T);
                }

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
            public async Task<T> GetOrAddAsync<T>(string key, Func<Task<T>> valFactory, TimeSpan expiry)
            {
                var success = _local.InnerGet(key, null, null, null, out T value);
                if (!success)
                {
                    var res = await _remote.InnerGetAsync(key, valFactory, expiry);
                    if (res.Success)
                    {
                        _local.Add(key, res.Value, expiry);
                        return value;
                    }
                    return default(T);
                }

                return value;
            }

            /// <summary>
            /// 缓存一个值，并设定过期时间
            /// </summary>
            /// <typeparam name="T">类型参数T</typeparam>
            /// <param name="key">指定的键</param>
            /// <param name="value">要缓存的值</param>
            /// <param name="expiry">过期时间</param>
            /// <returns>true为成功，否则失败</returns>
            public bool Add<T>(string key, T value, TimeSpan? expiry)
            {
                return _remote.Add(key, value, expiry);
            }

            /// <summary>
            /// 异步缓存一个值，并设定过期时间
            /// </summary>
            /// <typeparam name="T">类型参数T</typeparam>
            /// <param name="key">指定的键</param>
            /// <param name="value">要缓存的值</param>
            /// <param name="expiry">过期时间</param>
            /// <returns>true为成功，否则失败</returns>
            public Task<bool> AddAsync<T>(string key, T value, TimeSpan? expiry)
            {
                return _remote.AddAsync(key, value, expiry);
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
                // local.defaultexpiry >? expiry
                var ret = new Dictionary<string, T>();
                foreach (var key in keys)
                {
                    var success = _local.InnerGet(key, null, null, null, out T value);
                    if (!success)
                    {
                        success = _remote.InnerGet(key, null, expiry, out value);
                        if (success)
                        {
                            _local.Add(key, value);
                            ret[key] = value;
                        }
                        else
                            ret[key] = default(T);
                    }
                }

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
                throw new NotImplementedException();
            }

            /// <summary>
            /// 批量缓存值，并设定过期时间
            /// </summary>
            /// <typeparam name="T">类型参数T</typeparam>
            /// <param name="items">要缓存的键值集合</param>
            /// <param name="expiry">过期时间</param>
            /// <returns>true为成功，否则失败</returns>
            public bool AddAll<T>(IDictionary<string, T> items, TimeSpan? expiry)
            {
                throw new NotImplementedException();
            }

            /// <summary>
            /// 异步批量缓存值，并设定过期时间
            /// </summary>
            /// <typeparam name="T">类型参数T</typeparam>
            /// <param name="items">要缓存的键值集合</param>
            /// <param name="expiry">过期时间</param>
            /// <returns>true为成功，否则失败</returns>
            public async Task<bool> AddAllAsync<T>(IDictionary<string, T> items, TimeSpan? expiry)
            {
                throw new NotImplementedException();
            }
        }
    }
}
