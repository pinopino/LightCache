# LightCache
简单易用的缓存组件。

没有庞杂的无用的抽象层级，没有复杂的难以理解的接口封装，很多决策设计之初就已经定死：
- in-memory缓存就使用微软的`Microsoft.Extensions.Caching.Memory`
- remote缓存就使用redis，driver采用`StackExchange.Redis`

### 简单的使用
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
  ```csharp
  LightCacher cache = new LightCacher(1024);
  cache.InitRemote();
  cache.Mutil.Add(obj);
  ```


