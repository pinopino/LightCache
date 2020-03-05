using System;
using System.Collections.Generic;
using System.Linq;

namespace LightCache
{
    public class CacheService
    {
        protected CacheService()
        { }

        protected void EnsureKey(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException("缓存键不能为空");
        }

        protected void EnsureNotNull<T>(string name, IEnumerable<T> values)
        {
            if (values == null || !values.Any())
                throw new InvalidOperationException("提供的集合为空或未包含项");
        }

        protected void EnsureNotNull(string name, object value)
        {
            if (value == null)
                throw new ArgumentNullException($"{name}不允许为空");
        }
    }
}
