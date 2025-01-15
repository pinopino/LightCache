using Microsoft.Extensions.Caching.Memory;

namespace LightCache.Test
{
    public class LocalTests
    {
        [Fact]
        public void Get_缓存键不存在情况下返回给定的默认值()
        {
            var cache = new LocalCache();

            var ret1 = cache.Get("testKey", 10);
            Assert.Equal(10, ret1);

            var ret2 = cache.Exists("testKey");
            Assert.False(ret2);
        }

        [Fact]
        public void GetOrAdd_在没有缓存且valFactory为空情况下抛出异常()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                var cache = new LocalCache();
                cache.GetOrAdd<int>("testKey", null);
            });
        }

        [Fact]
        public void GetOrAdd_缓存键不存在情况下返回valFactory生成的值()
        {
            var cache = new LocalCache();

            cache.GetOrAdd("testKey", () => 10);

            var ret1 = cache.Exists("testKey");
            Assert.True(ret1);

            var ret2 = cache.Get("testKey", defaultVal: 11);
            Assert.Equal(10, ret2);
        }

        [Fact]
        public void GetOrAdd_valFactory生成的值可以绝对过期()
        {
            var cache = new LocalCache();

            var ret1 = cache.GetOrAdd("testKey", () => 10, absExp: DateTimeOffset.Now.AddSeconds(3));
            Assert.Equal(10, ret1);

            Thread.Sleep(2 * 1000);
            var ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(3 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.False(ret2);
        }

        [Fact]
        public void GetOrAdd_valFactory生成的值可以滑动过期()
        {
            var cache = new LocalCache();

            var ret1 = cache.GetOrAdd("testKey", () => 10, slidingExp: TimeSpan.FromSeconds(3));
            Assert.Equal(10, ret1);

            Thread.Sleep(2 * 1000);
            var ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(2 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(4 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.False(ret2);
        }

        [Fact]
        public void GetOrAdd_并发调用情况下valFactory只会被执行一次()
        {
            var cache = new LocalCache();

            var item = new Wrapper();
            Parallel.For(0, 5, (index) =>
            {
                cache.GetOrAdd("testKey", () =>
                {
                    item.Counter++;
                    return item;
                });
            });

            Assert.Equal(1, item.Counter);
        }

        [Fact]
        public async Task GetOrAddAsync_valFactory生成的值可以绝对过期()
        {
            var cache = new LocalCache();

            var ret1 = await cache.GetOrAddAsync("testKey",
                async () =>
                {
                    await Task.Delay(1000);
                    return 10;
                },
                absExp: DateTimeOffset.Now.AddSeconds(5));
            Assert.Equal(10, ret1);

            Thread.Sleep(1 * 1000);
            var ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(2 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(2 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.False(ret2);
        }

        [Fact]
        public async Task GetOrAddAsync_并发调用情况下valFactory只会被执行一次()
        {
            var cache = new LocalCache();

            var item = new Wrapper();
            var random = new Random();
            var allTask = Enumerable.Range(0, 10).Select(p =>
            {
                return Task.Run(async () =>
                {
                    await cache.GetOrAddAsync("testKey", async () =>
                    {
                        await Task.Delay(random.Next(4) * 1000);
                        item.Counter++;
                        return item;
                    });
                });
            });
            await Task.WhenAll(allTask);

            Assert.Equal(1, cache.GetLockCache().Count);
            Assert.Equal(1, item.Counter);
        }

        [Fact]
        public async Task GetOrAddAsync_并发调用情况下内部锁被正确释放()
        {
            var cache = new LocalCache();

            cache.SetLockAbsoluteExpiration(TimeSpan.FromSeconds(10));
            var item = new Wrapper();
            var random = new Random();
            var allTask = Enumerable.Range(0, 1).Select(p =>
            {
                return Task.Run(async () =>
                {
                    await cache.GetOrAddAsync("testKey", async () =>
                    {
                        await Task.Delay(random.Next(4) * 1000);
                        item.Counter++;
                        return item;
                    });
                });
            });
            await Task.WhenAll(allTask);

            var lockCache = cache.GetLockCache();
            Assert.Equal(1, lockCache.Count);
            Assert.Equal(1, item.Counter);

            Thread.Sleep(1 * 1000);
            var ret = lockCache.TryGetValue("testKey", out SemaphoreSlim semaphore);
            Assert.True(ret);

            Thread.Sleep(10 * 1000);
            var ret2 = lockCache.TryGetValue("testKey", out _);
            Assert.False(ret2);
        }

        [Fact]
        public void CacheEntry_设置了滑动过期但仍然可以被兜底绝对过期清除()
        {
            var cache = new LocalCache(capacity: 1024, expiration: 3, absoluteExpiration: 5);

            var ret1 = cache.GetOrAdd("testKey", () => 10);
            Assert.Equal(10, ret1);

            Thread.Sleep(1 * 1000);
            var ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(1 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(1 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(1 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.True(ret2);

            Thread.Sleep(2 * 1000);
            ret2 = cache.Exists("testKey");
            Assert.False(ret2);
        }

        [Fact]
        public void 能正常移除缓存项()
        {
            var cache = new LocalCache();

            var key = "testKey";
            var items = new List<Wrapper>
            {
                new Wrapper{ Counter = 1 },
                new Wrapper{ Counter = 2 },
                new Wrapper{ Counter = 3 }
            };
            cache.Add(key, items);

            var cachedItems = cache.Get<List<Wrapper>>(key);
            Assert.Equal(items, cachedItems);
            cachedItems.RemoveAt(0);

            cachedItems = cache.Get<List<Wrapper>>(key);
            Assert.Equal(2, items.Count);
            Assert.Equal(2, cachedItems.Count);
            Assert.Equal(items[0], cachedItems[0]);
            Assert.Equal(items[1], cachedItems[1]);
        }
    }

    class Wrapper
    {
        public int Counter;
    }
}