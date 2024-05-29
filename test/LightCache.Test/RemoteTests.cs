using LightCache.Remote;
using System.Runtime.CompilerServices;

namespace LightCache.Test
{
    public class RemoteTests
    {
        private string _host = "127.0.0.1:6379,defaultDatabase=0";

        [Fact]
        public async Task RemoveAllAsync_移除给定的所有键返回计数统计不为零()
        {
            var cache = new RemoteCache(_host);
            cache.AddAll(new Dictionary<string, int>
            {
                { "a", 1 },
                { "b", 2 },
                { "c", 3 }
            });

            var ret = cache.Exists("a");
            Assert.True(ret);
            ret = cache.Exists("b");
            Assert.True(ret);
            ret = cache.Exists("c");
            Assert.True(ret);

            var ret1 = await cache.RemoveAllAsync(["a", "b", "d"]);
            Assert.Equal(2, ret1);

            ret = cache.Exists("a");
            Assert.False(ret);
            ret = cache.Exists("b");
            Assert.False(ret);
            ret = cache.Exists("c");
            Assert.True(ret);

            // clean up
            cache.Remove("c");
        }

        [Fact]
        public void Get_缓存键不存在情况下返回给定的默认值()
        {
            var cache = new RemoteCache(_host);

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
                var cache = new RemoteCache(_host);
                cache.GetOrAdd<int>("testKey", null);
            });
        }

        [Fact]
        public void GetOrAdd_缓存键不存在情况下返回valFactory生成的值1()
        {
            var cache = new RemoteCache(_host);

            var key = Guid.NewGuid().ToString();
            cache.GetOrAdd(key, () => 10);

            var ret1 = cache.Exists(key);
            Assert.True(ret1);

            var ret2 = cache.Get(key, defaultVal: 11);
            Assert.Equal(10, ret2);

            // clean up
            cache.Remove(key);
        }

        [Fact]
        public void GetOrAdd_缓存键不存在情况下返回valFactory生成的值2()
        {
            var cache = new RemoteCache(_host);

            var key = Guid.NewGuid().ToString();
            cache.GetOrAdd(key, () => new Book { Name = "a", Price = 10 });

            var ret1 = cache.Exists(key);
            Assert.True(ret1);

            var ret2 = cache.Get<Book>(key);
            Assert.Equal("a", ret2.Name);
            Assert.Equal(10, ret2.Price);

            // clean up
            cache.Remove(key);
        }

        [Fact]
        public void GetOrAdd_valFactory生成的值可以过期()
        {
            var cache = new RemoteCache(_host);

            var key = Guid.NewGuid().ToString();
            var ret1 = cache.GetOrAdd(key, () => 10, expiry: TimeSpan.FromSeconds(3));
            Assert.Equal(10, ret1);

            Thread.Sleep(1 * 1000);
            var ret2 = cache.Exists(key);
            Assert.True(ret2);

            Thread.Sleep(2 * 1000);
            ret2 = cache.Exists(key);
            Assert.False(ret2);

            // clean up
            cache.Remove(key);
        }

        [Fact]
        public void GetOrAdd_并发调用情况下valFactory只会被执行一次1()
        {
            var cache = new RemoteCache(_host);

            var key = Guid.NewGuid().ToString();
            var item = new Wrapper();
            Parallel.For(0, 5, (index) =>
            {
                cache.GetOrAdd(key, () =>
                {
                    item.Counter++;
                    return item;
                }, useLock: false);
            });

            Assert.Equal(1, item.Counter);

            // clean up
            cache.Remove(key);
        }

        [Fact]
        public void GetOrAdd_并发调用情况下valFactory只会被执行一次2()
        {
            var cache = new RemoteCache(_host);

            var key = Guid.NewGuid().ToString();
            var item = new Wrapper();
            Parallel.For(0, 5, (index) =>
            {
                cache.GetOrAdd(key, () =>
                {
                    item.Counter++;
                    return item;
                });
            });

            Assert.Equal(1, item.Counter);

            // clean up
            cache.Remove(key);
        }

        [Fact]
        public async Task GetOrAddAsync_并发调用情况下valFactory只会被执行一次1()
        {
            var cache = new RemoteCache(_host);

            var key = Guid.NewGuid().ToString();
            var item = new Wrapper();
            var random = new Random();
            var allTask = Enumerable.Range(0, 10).Select(p =>
            {
                return Task.Run(async () =>
                {
                    await cache.GetOrAddAsync(key, async () =>
                    {
                        await Task.Delay(random.Next(3));
                        item.Counter++;
                        return item;
                    }, useLock: false);
                });
            });
            await Task.WhenAll(allTask);

            Assert.Equal(1, cache.GetLockCache().Count);
            Assert.Equal(1, item.Counter);

            // clean up
            cache.Remove(key);
        }

        [Fact]
        public async Task GetOrAddAsync_并发调用情况下valFactory只会被执行一次2()
        {
            var cache = new RemoteCache(_host);

            var key = Guid.NewGuid().ToString();
            var item = new Wrapper();
            var random = new Random();
            var allTask = Enumerable.Range(0, 10).Select(p =>
            {
                return Task.Run(async () =>
                {
                    await cache.GetOrAddAsync(key, async () =>
                    {
                        await Task.Delay(random.Next(3));
                        item.Counter++;
                        return item;
                    });
                });
            });
            await Task.WhenAll(allTask);

            Assert.Equal(0, cache.GetLockCache().Count);
            Assert.Equal(1, item.Counter);

            // clean up
            cache.Remove(key);
        }

        [Fact]
        public async Task GetAllAsync_能正常批量插入并且批量读取()
        {
            var cache = new RemoteCache(_host);

            var dict = new Dictionary<string, Wrapper>
            {
                { "A", new Wrapper{ Counter = 1 } },
                { "B", new Wrapper{ Counter = 2 } },
                { "C", new Wrapper{ Counter = 3 } }
            };
            await cache.AddAllAsync(dict);

            var ret = await cache.GetAllAsync<Wrapper>(["A", "B", "C"]);

            Assert.Equal(dict.Count, ret.Count);
            Assert.True(ret.ContainsKey("A"));
            Assert.True(ret.ContainsKey("B"));
            Assert.True(ret.ContainsKey("C"));

            Assert.Equal(dict["A"].Counter, ret["A"].Counter);
            Assert.Equal(dict["B"].Counter, ret["B"].Counter);
            Assert.Equal(dict["C"].Counter, ret["C"].Counter);

            // clean up
            cache.RemoveAll(["A", "B", "C"]);
        }
    }

    class Book
    {
        public string Name { set; get; }
        public int Price { set; get; }
    }
}