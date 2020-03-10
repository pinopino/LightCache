# LightCache
简单易用的缓存组件。

没有庞杂的无用的抽象层级，没有复杂的难以理解的接口封装，很多决策设计之初就已经定死：
- in-memory缓存就使用微软的`Microsoft.Extensions.Caching.Memory`
- remote缓存就使用redis，driver采用`StackExchange.Redis`

### 简单使用
- 本地
  ```csharp
  LightCacher cache = new LightCacher(1024);
  cache.Add(obj);
  ```

- remote
  ```csharp
  LightCacher cache = new LightCacher(1024);
  cache.InitRemote();
  cache.Remote.Add(obj);
  ```

- mutil-cache（二级缓存）
  在api的设计上有考虑过像上面`remote`那样的方式，比如：
  ```csharp
  LightCacher cache = new LightCacher(1024);
  cache.InitRemote(); // 初始化remote之后，相当于也就激活了多级缓存
  cache.Mutil.Add(obj);
  ```
  但是发现这会造成使用上的困扰，因为很显然这样子一来进程内缓存、远程缓存以及多级缓存将会同时存在。技术上是允许的，但是你肯定不希望在使用上有任何混淆，所以调整为：
  ```csharp
  MultiCache cache = new MultiCache(host, 1024);
  cache.Add(obj);
  ```

### 多级缓存
##### 使用场景
对于多级缓存的使用场景我做了一个假设，即应用程序是分布式部署的（常见的如`web-farm`）。通常在这种量级的环境中才有使用多级缓存的价值，简单的单机情况前面两种缓存足够应付了。

##### 缓存更新
直接上表更直观一点：

|        | 单机     | 多机                                         |
| ------ | -------- | -------------------------------------------- |
| In-Mem | 正常更新 | redis pub/sub                                |
| Remote | 正常更新 | 正常更新                                     |
| Multi  | 正常更新 | self -> 正常更新 <br> other -> redis pub/sub |

对于第三种，框架提供了通知方法，你可以在完成一次数据库修改动作之后调用：
```csharp
cache.NotifyChangeFor(key);
```
注意这里面多个事件的发生顺序：
1. self更新：删除remote -> 删除in-mem
2. 发送通知
3. other更新：删除remot -> 删除in-mem


