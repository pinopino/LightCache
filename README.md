# LightCache
简单易用的缓存组件。

没有庞杂的无用的抽象层级，没有复杂的难以理解的接口封装，很多决策设计之初就已经定死：
- in-memory缓存就使用微软的`Microsoft.Extensions.Caching.Memory`
- remote缓存就使用redis，driver采用`StackExchange.Redis`

### 简单使用
库在设计上考虑了两种使用场景，一种是常见的单机单应用。此种情况下你既可以使用本地缓存也可以使用远程缓存（比如你就是很喜欢redis）：
- 本地
  ```csharp
  LightCacher cache = new LightCacher(1024);
  cache.Add(obj);
  ```

- remote
  ```csharp
  LightCacher cache = new LightCacher(1024);
  cache.Remote.Add(obj);
  ```

### 多级缓存
#### 场景
对于多级缓存的使用场景我做了一个假设，即应用程序是分布式部署的（常见的如`web-farm`）。通常在这种量级的环境中才有使用多级缓存的价值，简单的单机情况上面提到的两种缓存足够应付了。

因此很多在本地缓存和远程缓存的设计中没有考虑的东西（就让我们无脑的简单使用吧），在多级缓存中都需要慎重考虑：
- 更新（下面会描述）
- 缓存穿透，两种处理方式：
  - 直接用null或者其它某个特殊值表达
  - 查询缓存前对key做过滤（bitmap，布隆过滤器）
- 缓存击穿（针对热点数据）
  主要是一个并发控制。目的就一个防止键miss的时候大量请求打到数据上，这在多级缓存的情况下要考虑的就是main cache（redis）的并发控制，有两种方式：
  - stackexchange.redis提供的`LockTake`/`LockRelease`方法组
  - 如果redis使用的集群配置，那么可以使用redlock，.net 这边有现成的实现
  > 你可能想要使用redis提供的`WATCH`搞一个乐观锁，很遗憾这是并不行的。原因就在于此种方式下乐观锁体中一部分请求逻辑还是会被所有并发客户端执行的，并不能起到保护数据库的作用。
- 缓存雪崩
  多级缓存一定程度上能够缓解这个问题；另外，过期时间随机一个范围可能会好点（应用层去决定这个时间要好点，库只需要提供支持方法即可）

#### 更新
直接上表更直观一点（`---`，如前所述库本身对使用场景是做了假设的，虚线的就表示你最好不要在此种情况下使用对应类型的缓存）：
|        | 单机     | 多机                                         |
| ------ | -------- | -------------------------------------------- |
| In-Mem | 正常更新 | --- |
| Remote | 正常更新 | --- |
| Hybird  | --- | self -> 正常更新 <br> other -> redis pub/sub |

对于第三种情况，框架提供了通知方法，你可以在完成一次数据库修改动作之后调用：
```csharp
cache.NotifyChangeFor(key);
```
注意这里面多个事件的发生顺序：
1. self更新：删除remote -> 删除in-mem
2. 发送通知
3. other更新：删除remot -> 删除in-mem

#### 使用
在api的设计上有考虑过像上面`remote`那样的方式，比如：
```csharp
LightCacher cache = new LightCacher(1024);
cache.InitRemote(); // 初始化remote之后，相当于也就激活了多级缓存
cache.Mutil.Add(obj);
```
但是发现这会造成使用上的困扰，因为很显然这样子一来进程内缓存、远程缓存以及多级缓存将会同时存在。技术上是允许的，但是你肯定不希望在使用上有任何混淆，所以调整为：
```csharp
HybirdCache cache = new HybirdCache(host, 1024);
cache.Add(obj);
```

### 关于StackExchange.Redis
最后，库内部远程缓存使用的正是`StackExchange.Redis`，这里记录一些其重要特性以备查阅用。

#### 核心对象ConnectionMultiplexer
使用该对象来连接redis服务端（redis集群亦可）。该对象上所有方法都是线程安全的，因其本身就是被设计来全局使用的：
> ConnectionMultiplexer is designed to be shared and reused between callers; you can have a ConnectionMultiplexer instance and  stored it away for re-use.

```csharp
var redis = ConnectionMultiplexer.Connect("localhost");
IDatabase db = redis.GetDatabase(databaseNumber, asyncState);
```
如上面代码展示的那样，通过`ConnectionMultiplexer`拿到了`IDatabase`，这之后的事情就非常简单了。我们正是通过这个接口来发送各种各样redis命令到服务端的。

#### 调用方式
所有的redis命令也就是IDatabase接口的所有方法都支持三种模式的调用：
- Synchronous
- Asynchronous
- Fire-and-Forget

最后一种单独说一下，此类调用是通过方法参数`CommandFlags`来实现的（所有方法都可以指定该参数）；这种是有比较明确的使用场景限制的，比如你确定了对于方法执行的结果不关心（正误不影响逻辑流）那么就可以采用fire-and-forget方式。

#### 数据类型
键的类型是`RedisKey`。因为redis支持text键或者binary键，所以接口方法上传入string或者是byte[]是可以的，很显然这里提供了这些类型到rediskey的隐式转换。

值的类型是`RedisValue`，类似也是提供了很多方便代码编写的隐式转换。

#### 事务
这里先说说redis原生的事务机制，像redis一样，其事务模型也是非常简单易懂的。

一组命令`MULTI`、`EXEC`、`DISCARD`以及`WATCH`。redis服务端解析发送过来的命令，遇到MULTI事务即开始；遇到EXEC事务即提交，遇到DISCARD事务即取消。

是不是很简单？一些额外的重要的规则：
- MULTI之后所有命令都不会被执行，而是被入队（注意，这同时也意味着你没法在redis事务中获取xxx数据执行某个逻辑判断，你做不了逻辑，你只能是执行a命令然后执行b命令这样）：
  ```csharp
  > MULTI
  OK
  > INCR foo
  QUEUED -- 返回queued
  > INCR bar
  QUEUED -- 返回queued
  > EXEC -- 执行所有入队的命令
  1) (integer) 1
  2) (integer) 1
  ```
- 出错处理，两种类型的错误（按发生时机）
  - EXEC执行之前QUEUED的某一条或某几条命令就已经出错，比如语法解析都过不了之类的。此时，大多数redis客户端实现都会直接终止该事务的继续执行。
  ```csharp
  > MULTI
  OK
  > INCR a b c
  -ERR wrong number of arguments for 'incr' command
  ```
  - EXEC执行出错。针对这种情况redis并没有做特殊的处理，而是出错的让它出错，没有出错的继续执行。
  ```csharp
  > MULTI
  OK
  > SET a abc
  QUEUED
  > LPOP a
  QUEUED
  > EXEC
  OK
  -ERR Operation against a key holding the wrong kind of value
  ```
  > It's important to note that even when a command fails, all the other commands in the queue are processed – Redis will not stop the processing of commands.

基本情况就是这样。上面一组命令中还剩最后一个`WATCH`没有说；这货是用来实现`CAS`控制的。我们可以用它来实现一个乐观锁，比如一个get-and-update操作，正常情况下是这样：
```csharp
val = GET mykey
val = val + 1
SET mykey $val
```
在只有一个redis客户端的情况这样的操作是没有问题的。但是一旦有多个客户端都执行这段逻辑的话就会出现典型的race condition，比如A和B两个client都get到原始值10，大家一起+1并回存，此时值将会是11而不是正确的12。用WATCH来解决这个问题：
```csharp
WATCH mykey -- 告诉redis帮我们监视mykey
val = GET mykey
val = val + 1
MULTI
SET mykey $val
EXEC -- 如果mykey没有被其它客户端并发操作，那么执行事务；否什么都不会发生
```

最后，redis中要执行事务操作的另外一种完全不同的办法就是使用脚本：
> A Redis script is transactional by definition, so everything you can do with a Redis transaction, you can also do with a script, and usually the script will be both simpler and faster.

说完了redis原生的事务，现在可以说说StackExchange.Redis中的事务处理了。因其自身设计上的原因，要想直接支持redis原生这一组事务命令不太可能，所以StackExchange.Redis自己做了一些抽象和封装（核心抽象就是`constraints`类型），整体使用下来效果是一样的并且并不会觉得太过别扭。

最后，在一些非常常见而又简单的需要事务支持的场景中，redis是直接提供了原子命令可以直接操作的，StackExchange.Redis也跟进做了很方便的封装，就是这里的`When`：
```csharp
var newId = CreateNewId();
bool wasSet = db.HashSet(custKey, "UniqueID", newId, When.NotExists);
```