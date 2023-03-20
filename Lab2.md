# Lab2 

### 评测结果

```
(base) liuziyang@liuziyangdeMacBook-Air raft % go test -run=2 
Test (2A): initial election ...
  ... Passed --   3.1  3   60   16214    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  152   29034    0
Test (2A): multiple elections ...
  ... Passed --   5.3  7  586  117264    0
Test (2B): basic agreement ...
  ... Passed --   0.9  3   16    4320    3
Test (2B): RPC byte count ...
  ... Passed --   2.5  3   46  113124   11
Test (2B): agreement after follower reconnects ...
  ... Passed --   6.2  3  142   36682    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.6  5  238   46077    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.8  3   14    3836    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.5  3  221   53941    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  26.8  5 6602 5978261  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.3  3   42   11924   12
Test (2C): basic persistence ...
  ... Passed --   4.2  3   87   21215    6
Test (2C): more persistence ...
  ... Passed --  17.1  5 1158  241210   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   2.1  3   38    9424    4
Test (2C): Figure 8 ...
  ... Passed --  30.6  5  982  205362   30
Test (2C): unreliable agreement ...
  ... Passed --   5.3  5  251   83182  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  33.7  5 10000 15330250  377
Test (2C): churn ...
  ... Passed --  16.2  5 1121  578257  117
Test (2C): unreliable churn ...
  ... Passed --  16.5  5  866  390341   79
Test (2D): snapshots basic ...
  ... Passed --   6.9  3  134   46590  215
Test (2D): install snapshots (disconnect) ...
  ... Passed --  79.5  3 1983  695470  313
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  70.1  3 2199  775320  353
Test (2D): install snapshots (crash) ...
  ... Passed --  37.6  3  741  352809  328
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  39.3  3  900  408002  343
Test (2D): crash and restart all servers ...
  ... Passed --  12.0  3  232   65346   52
PASS
ok      6.824/raft      434.123s
```

### 分析笔记

服务器上的共识模块从客户端接收命令并将其添加到日志中。它与其他服务器上的共识模块进行通信，以确保每个日志最终都以相同的顺序包含相同的请求，即使某些服务器出现故障。一旦命令被正确复制，每个服务器的状态机都会按照日志顺序处理它们，输出将返回给客户端。



Raft通过首先选举一名不同的leader，然后让leader完全负责管理复制的日志来实现共识。leader接受来自client的日志条目，在其他服务器上复制它们，并告诉服务器何时将日志条目应用到其状态机是安全的。



- **服务器状态：** *leader*, *follower*, or *candidate*.

  <img src="/Users/liuziyang/Library/Application Support/typora-user-images/image-20221115152948827.png" alt="image-20221115152948827" style="zoom:50%;" />

- **term检查：**Current terms are exchanged **whenever** servers communicate; 

  if one server’s current term is smaller than the other’s, then it **updates its current term to the larger value.** 

  If a **candidate or leader** discovers that its term is out of date, it immediately **reverts to follower state.** 

  If a server receives a request with a stale term number, it **rejects the request.**

- **follower的请求重定向：** The leader handles all client requests .

   (if a client contacts a follower, the follower **redirects** it to the leader). 

-  **RPCs：**

  **RequestVote RPCs** are initiated by candidates during elections , 

  **AppendEntries RPCs** are initiated by leaders to replicate log entries and to provide a form of heartbeat.

  **Transferring snapshots** between servers. 

  Servers **retry** RPCs if they do not receive a response in a timely manner, 

  issue RPCs in **parallel** for best performance.

- **选举：**

  begin： a follower **increments its current term** and transitions to **candidate state**.

  then： **votes for itself **and issues **RequestVote RPCs** in parallel to each of the other servers in the cluster. 

  **投票规则：**Each server will vote for **at most one candidate** in a given term, on a **first-come-first-served **basis.（因此需要记录每个term对应的投票情况，投过直接不同意）

  **选举结果：**

  (a) **自己竞选成功：**

  It then **sends heartbeat messages** （应该是**AppendEntries RPCs**）to all of the other servers to establish its authority and prevent new elections.

  (b) **收到了其他server的AppendEntries RPCs**

  如果收到的**AppendEntries RPCs**且term号$\ge$自己的term，则该server竞选成功，自己变为follower。（随后的term和log怎么变？）

  如果收到的**AppendEntries RPCs**且term号<自己的term，则继续保持candidate 状态，等待**time out **（时间随机） and start a new election by **incrementing its term** and initiating another round of **RequestVote RPCs.**

  (c) a period of time goes by with **no winner**. 等待timeout同上

- **竞选成功之后：**

  响应client请求。

  (1) The leader **appends** the command to its log as a new entry.（先把command作为一个新的条目加到自己的log中）

  (2) then **issues AppendEntries RPCs** in **parallel** to each of the other servers **to replicate the entry.**（向所有follower并行广播这一条目，让他们复制该条目）

  (3) When the entry has been **safely replicated **（需要被**多数**follower复制好）, the leader **applies the entry to its state machine** and **returns the result** of that execution to the client. （这里严格保证顺序，不可以并行）

  (4) If **followers crash** or run slowly, or if network packets are lost, the leader retries AppendEntries RPCs **indefinitely**（直到不是leader？） (even after it has responded to the client) until all followers eventually store all log entries.（如果leader换人了，新的leader也要继续广播直到所有人都填好，所以要有一个list记录谁没填好？）

- **log**

  <img src="/Users/liuziyang/Library/Application Support/typora-user-images/image-20221115163416329.png" alt="image-20221115163416329" style="zoom:50%;" />

  **log组成：**每个log有一个**term**号，一个**command**操作，一个**log index**.

  **提交原则：**一旦创建log entry的leader在大多数follower上复制了该条目，就会commit the log entry.

  （==？？？==暂时没理解为什么要记录这个）The leader **keeps track** of the **highest index** it knows to be committed, and it i**ncludes that index in future** AppendEntries RPCs (including heartbeats) so that the other servers eventually find out. Once a follower learns that a log entry is committed, it applies the entry to its local state machine (in log order). （follower会慢一个log的更新速度）

  **log必须满足的性质**：如果term和index都一样，那么该index以及之前index的操作都一样

  • If two entries in different logs have the same index and term, then they store the same command.
  • If two entries in different logs have the same index and term, then the logs are identical in all preceding entries.

  **强制将follower日志改为与leader一样：**To bring a follower’s log into consistency with its own, the leader must **find the latest log entry** where the two logs agree, **delete** any entries in the follower’s log after that point, and **send the follower all** of the leader’s entries after that point. 

  **触发机制：**All of these actions happen in response to the consistency check performed by **AppendEntries RPCs. **

  **具体实现方法：**

  (1) The leader maintains a **nextIndex for each follower**, 初始值为自己的下一个index号.

  (2) 如果不一致，在AppendEntries rpc的**consistency check**中将会失败，follower拒绝复制 。

  (3) After a rejection, the leader **decrements nextIndex** and **retries** the AppendEntries RPC. 直到不再拒绝。（**优化：**如果rejection可以包括**the term of the conflicting entry and the first index it stores for that term**，则不需要递减，可以一步到位）

  (4) When this happens, AppendEntries will succeed, which removes any conflicting entries in the follower’s log and **appends entries** from the leader’s log (if any). （把确实的条目全部复制）Once AppendEntries succeeds, the follower’s log is consistent with the leader’s, and it will remain that way for the rest of the term.

- **安全限制**

  **比较两个log谁更up-to-date：**term不同选大的，term相同选长的。

  ==？？？==5.4.2 figure8 term的变化不懂



### 实现细节

- 如果现有entry与新entry conflict（相同的index但不同的term），请删除现有entry及其后面的所有entry。

  **这里的if至关重要。**如果追随者拥有领导者发送的所有条目，则追随者**不得截断其日志。**

- **重置election timer的触发事件：**

   a) you get an `AppendEntries` RPC from the *current* leader (i.e., if the term in the `AppendEntries` arguments is outdated, you should **not** reset your timer); 

  b) you are starting an election; （if you are a candidate (i.e., you are currently running an election), but the election timer fires, you should start *another* election. This is important to avoid the system stalling due to delayed or dropped RPCs.）竞选时间过期了要重置时间重新竞选

  c) you *grant* a vote to another peer.（保证more up-to-date的可以当选）

- **term改变机制：**

  - If RPC request or response contains term `T > currentTerm`: set `currentTerm = T`, convert to follower.

  如果已经投票了，但遇到了更大的term要怎么回复？要更新term并投票。

  > For example, if you have already voted in the current term, and an incoming `RequestVote` RPC has a higher term that you, you should *first* step down and adopt their term (thereby resetting `votedFor`), and *then* handle the RPC, which will result in you granting the vote! 

  - election begin： a follower **increments its current term** and transitions to **candidate state**.

- **RPC处理**

  - If you get an `AppendEntries` RPC with a `prevLogIndex` that points beyond the end of your log, you should handle it the same as if you did have that entry but the term did not match (i.e., **reply false**).没有的按不匹配处理
  - 对于无entry的heartbeat的处理，也要进行**consistency check**

- **对于old reply的处理**

  From experience, we have found that by far the simplest thing to do is to first record the term in the reply (it may be higher than your current term), and then to compare the current term with the term you sent in your original RPC. If the two are different, drop the reply and return. *Only* if the two terms are the same should you continue processing the reply.

  update `matchIndex` to be `prevLogIndex + len(entries[])` from the arguments you sent in the RPC originally.




### 我的实现

- heartbeat 100ms
- 选举计时在1s之内
- start函数append logEntry随后发送appendEntries到所有人

## Debug

### Debug1：

Leader 2获得1,4的选票，并且成功在1,4上复制了1-12号index的log entry，由于在多数节点复制了log，于是将这些entry apply到自己的主机上，此时crash.（1,4并未获得提交的通知）term:22

此时3重新连接，当选leader，term:24，然后直接crash

此时2重新连接，当选leader，term:27，然后直接crash

此时0重新连接，当选leader，term:29，commitIndex:0,logsLen:12

发送appendEntries，与1,4 MisMatch

0调整nextindex值，重新向1,4发送appendEntries，成功覆盖了2在1,4上复制的log，完成复制后提交，此时发现2与0提交的entry不同

##### 解决：leader只能在本任期提交过log才能提交之前任期的log

### Debug2

follower只有投出赞同票才reset timer，而不是在发现有更大的任期的rpc就reset timer。

如果不遵守这条原则，会出现这样的情况：follower拥有更小的term和更新的log，由于一直被candidate重置时钟，一直无法参与竞选，同时candidate由于日志不够新又无法得到足够多的选票。这样不但会消耗大量的rpc资源，甚至会导致一直无法选出正确leader的超时现象。
