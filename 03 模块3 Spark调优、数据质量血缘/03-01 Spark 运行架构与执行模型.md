---
title: 03-01 Spark 运行架构与执行模型
module: 模块3 Spark 调优、数据质量与血缘
difficulty: ⭐⭐⭐
created: 2026-10-25
status: 已完成
tags:
- Spark
- 调优
---
# 03-01 Spark 运行架构与执行模型

> [!abstract] 一句话结论
> 一条 SQL 提交后，Spark 先把它翻译成 DAG，再用 **shuffle 切 Stage、用分区数定 Task 数**，最后把 task 塞进 executor 的 core 里跑。
> 记住两条铁律：**Stage 的边界由 shuffle 决定，Task 的数量由分区数决定**——两者都跟「数据量」没有直接关系。
> 所以调优的第一步永远是核对「并行度 × 单 task 数据量」是否匹配，而不是先改参数。

---

## 一、心智模型：Spark 是一家装修公司

把一次作业想成一次全屋装修：

| 角色 | Spark 概念 | 装修里的对应 | 关键特征 |
| --- | --- | --- | --- |
| **项目经理** | Driver | 出图纸、排工序、派活、验收 | 只有一个。它死了，整个工地停工 |
| **施工队** | Executor | 一支自带仓库（cache）和工具的队伍 | 有多支。一支队累倒了，活可以转给别人 |
| **工人** | Core（vcore） | 队里能同时干活的人数 | 决定「同时能干几件事」 |
| **一道工序** | Stage | 水电 → 泥瓦 → 油漆 | 换工序前必须等上一道**全部**完工 |
| **一个工位** | Task | 一个工人负责的一个房间 | 最小不可再分单元 |
| **材料中转站** | Shuffle | 水电阶段剩下的管件要集中分拣再发给泥瓦工 | 要落盘、要走网络、是性能分水岭 |

**两条从类比里直接长出来的结论**：

1. **为什么换工序要等**：泥瓦工不可能在水电只做完一半时开工，因为「哪个房间走哪根管」还没定。对应到 Spark，reduce 端要按 key 聚合，必须等**所有** map 端把同一个 key 的数据都吐出来 → 这就是 shuffle 成为 stage 边界的原因。
2. **为什么工人数 ≠ 效率**：给一个 50 平米的房子派 100 个工人，大部分人在等工具。对应到 Spark，**task 数远超并行度就是纯调度开销**，task 数远小于并行度就是集群空转。

**执行链路的分层视图**（从提交到跑起来）：

```text
① 提交层   spark-submit / spark-sql / thrift server
             ↓  Driver 进程启动，建 SparkSession
② 编译层   Catalyst：parse → analyze → optimize → physical plan
             ↓  得到一棵带 Exchange 节点的物理计划
③ 切分层   DAGScheduler：从后往前推，遇 ShuffleDependency 切一刀
             ↓  Job → N 个 Stage（ShuffleMapStage ... + ResultStage）
④ 调度层   TaskScheduler：把每个 Stage 拆成 partition 数个 Task
             ↓  按本地性级别（PROCESS→NODE→RACK→ANY）配对 executor 的 core
⑤ 执行层   Executor 里 TaskRunner 线程跑 task，读写 BlockManager
             ↓  窄依赖 pipeline 成一条链，宽依赖落盘 + 拉取
⑥ 汇总层   ResultStage 把结果回传 Driver（count/collect 走这里）
```

> [!important] 核心认知
> **Spark 的并行度是「声明式」的，不是「自动」的**。你写 `group by`，它不会自己算出「这个任务该用 800 个 task」——它只会用 `spark.sql.shuffle.partitions`（3.5 默认 200）或者 AQE 合并后的结果。所以「为什么我的任务只有 20 个 task 跑在 200 核集群上」这类问题，答案永远在分区数上，不在数据量上。

---

## 二、核心知识点

### 2.1 四个角色：谁在干什么

| 角色 | 进程/位置 | 职责 | 生命周期 | 挂了会怎样 |
| --- | --- | --- | --- | --- |
| **Driver** | 独立 JVM（client 模式在提交机，cluster 模式在集群 container） | 建 SparkSession、解析 SQL、生成 DAG、划分 stage、调度 task、汇总结果 | 整个 application | **整个作业失败**，已跑的 stage 全部作废 |
| **Executor** | 集群节点上的 JVM 进程 | 跑 task、缓存 block、提供 shuffle 服务 | 整个 application（可动态伸缩） | 上面的 task 重跑；它产出的 shuffle 文件丢失 → 下游 `FetchFailedException` |
| **Cluster Manager** | YARN RM / K8s API / Standalone Master | 只负责分配 container，不懂 Spark | 常驻 | 新 executor 申请不到，任务卡在 `WAITING` |
| **Task** | Executor 里的一个线程 | 处理**一个 partition** 的一次计算 | 秒~小时 | 重试，默认 `spark.task.maxFailures=4` |

**关键参数**：

```text
spark.driver.memory              默认 1g（3.5 默认，需实测）→ 生产 4~8g
spark.driver.maxResultSize       默认 1g → collect 超过直接报错
spark.executor.memory            默认 1g → 生产按 2.5 节推导
spark.executor.cores             默认 1 → 生产 4~5（见 2.5 节推导）
spark.executor.memoryOverhead    默认 max(384MB, 0.1 × executor.memory)
spark.deploy.mode                client / cluster
```

**为什么这样设计**：把「决策」和「执行」彻底分开。Driver 只做轻量调度（内存里维护 DAG 和 task 状态，不做数据计算），Executor 除了 cache 和 shuffle 文件之外**无状态**。于是 executor 挂掉只是「重跑一批 task」，而 driver 挂了没有任何东西能接管——这就是为什么**driver 必须稳**。

**生产上什么时候踩坑**：

| 场景 | 后果 | 正确做法 |
| --- | --- | --- |
| client 模式 + 提交机是个人笔记本/跳板机 | 笔记本休眠、SSH 断开 → driver 死 → 任务失败 | 生产一律 `--deploy-mode cluster`，或提交到 Gateway 机器并 `nohup` |
| `df.collect()` / `toPandas()` 拉回几千万行 | `Total size of serialized results of N tasks is bigger than spark.driver.maxResultSize` | 用 `write` 落盘，或先 `groupBy` 聚合再 collect |
| 广播一个大变量（如 200MB 维表 map） | driver 端序列化 + executor 端反序列化，双双 OOM | 广播前先 `filter`/`select` 瘦身，或改成 broadcast join 让 Spark 自己管 |
| driver 在 A 机房、executor 在 B 机房 | 每个 task 的状态上报 + 结果回传都跨机房，`Scheduling Delay` 极高 | driver 与 executor 同机房；结果用 `write` 而不是 `collect` |

### 2.2 Job 的边界：一个 action 一个 Job

Spark 是**惰性**的：`select`/`where`/`join`/`groupBy` 都只是往 DAG 上挂节点，不触发计算。只有 **action** 才会提交 Job。

| 算子 | 是 action 吗 | 触发 Job 数 | 结果去哪 |
| --- | --- | --- | --- |
| `select` / `filter` / `join` / `groupBy` / `withColumn` | ❌ transformation | 0 | DAG 上挂一个节点 |
| `count()` | ✅ | 1 | 一个数字回 driver |
| `show(n)` / `take(n)` / `first()` | ✅ | 1（内部带 limit） | 最多 n 行回 driver |
| `collect()` | ✅ | 1 | **全部**结果回 driver |
| `write.saveAsTable` / `insert overwrite` | ✅ | 1 | 落 HDFS |
| `foreach` / `foreachPartition` | ✅ | 1 | 各自定义 |
| `explain()` | ❌ | 0 | 只打印计划 |

> [!warning] `spark-sql` 交互式的一个陷阱
> 在 `spark-sql` shell 里敲一条 `select ...` 看起来「直接出结果」，其实是 shell 帮你调了 `show(20)`——**它已经是 action 了**。所以「我先 select 看看，再 insert」这种操作，在 shell 里是跑了两遍。

**为什么这样设计**：惰性求值让 Catalyst 能看到**整条链**再优化。如果每步都立刻执行，就无法做谓词下推、列裁剪、算子合并——`filter` 和 `select` 分开跑意味着读两遍数据。

**生产上什么时候踩坑**：迭代式逻辑（如反复修正的补数脚本）里，对同一个 DataFrame 连做 3 个 action 却不 `cache`：

```python
df = spark.table("dwd.fact_order").filter("dt='2026-10-01'")
print(df.count())                                  # Job 1：全量扫一遍
df.groupBy("user_id").sum("amount").show()          # Job 2：再扫一遍
df.select("amount").describe().show()               # Job 3：再扫一遍
```

SparkUI 的 Jobs 页会看到 3 个 job、3 次相同的 `input size`。**正确做法见实验 3.5**。

### 2.3 Stage 的边界：shuffle

这是整节最重要的一张表。

| 维度 | 窄依赖（NarrowDependency） | 宽依赖（ShuffleDependency） |
| --- | --- | --- |
| 定义 | 父 RDD 的**一个** partition 只被**一个**子 partition 使用 | 父 RDD 的**一个** partition 被**多个**子 partition 使用 |
| 典型算子 | `map` / `filter` / `select` / `union` / `coalesce` / `mapPartitions` | `groupByKey` / `reduceByKey` / `join`（非 broadcast）/ `distinct` / `repartition` / `sortBy` |
| 数据是否跨节点 | 否 | **是**，按 key 的 hash 重分布 |
| 是否落盘 | 否，pipeline 在内存里流式处理 | **是**，map 端写本地磁盘 + index 文件 |
| 失败恢复代价 | 只重算丢失的那个 partition | 要重算**整个上游 stage 的所有 map 输出** |
| 是否切 Stage | 否 | **是** |

**DAG 划分规则**：从最后一个 action 倒着往前推，**遇到 ShuffleDependency 就切一刀**。

```text
        [读 parquet]  →  [filter]  →  [select]        ← 全是窄依赖，pipeline 成 1 个 stage
                                    ↓ Exchange(hashpartitioning(user_id, 200))   ← 切一刀
                              [ShuffleMapStage]
                                    ↓
                        [HashAggregate] → [结果]        ← ResultStage
```

于是：**Stage 数 = Exchange 节点数 + 1**（单 job 内）。这个公式可以直接从 `explain` 里数出来。

```sql
-- 数 Exchange 的方法
set spark.sql.adaptive.enabled = false;   -- 关掉 AQE，否则计划被 AdaptiveSparkPlan 包住不好数
explain extended
select user_id, sum(amount) from tmp.fact_order where dt='2026-10-01' group by user_id;
-- 输出里搜 "Exchange"：出现 1 次 → 2 个 stage
```

**为什么这样设计**：shuffle 是「天然屏障」——reduce 端要按 key 聚合，必须等**所有** map 端把同一个 key 吐完。既然必须等，不如把这个等待点显式化成 stage 边界，让调度器能在这里做检查点（记录 map output 位置）、做失败重试、做资源回收。

**生产上什么时候踩坑**：

| 场景 | 现象 | 应对 |
| --- | --- | --- |
| 一个 stage 里塞了 5 次 `union all` 后再 `group by` | 上游 5 个分支要并行读，读放大 5 倍，但只有 1 次 shuffle | 可接受，但要注意 `union` 分支数 × 分区数 = task 数暴涨 |
| `repartition(1000)` 后又 `groupBy` | 2 次 shuffle，多一个 stage，多一轮落盘 | 删掉 `repartition`，直接让 `groupBy` 自己 shuffle |
| `distinct` 写在子查询里又外层 `count` | `distinct` = `groupBy` + shuffle，白白多一刀 | 用 `count(distinct x)` 让 Catalyst 合并成一次 shuffle |
| 大量 `withColumn` 链式调用 | 不会多 stage（窄依赖），但会生成巨大表达式树，编译慢 | 表达式过深时拆成多个 `select`，或 `checkpoint` 截断血缘 |

### 2.4 Task 的数量：分区数，不是数据量

**三条规则，按 stage 类型区分**：

| Stage 类型 | Task 数由什么决定 | 3.5 相关参数 |
| --- | --- | --- |
| **读文件的 stage**（第一个 stage） | 文件 split 数 ≈ `总大小 / maxPartitionBytes`，且**至少等于文件数** | `spark.sql.files.maxPartitionBytes`（默认 128MB）、`spark.sql.files.openCostInBytes`（默认 4MB）、`spark.sql.files.minPartitionNum` |
| **shuffle 后的 stage** | `spark.sql.shuffle.partitions`，AQE 开启时会被 `coalesce` 合并 | `spark.sql.shuffle.partitions`（默认 200）、`spark.sql.adaptive.advisoryPartitionSizeInBytes`（默认 64MB） |
| **`coalesce(n)` 之后** | 强制 n（窄依赖，不 shuffle） | — |

**并行度推导（这才是「怎么算出来的」）**：

```text
总并行度 P = executor 数 × 每个 executor 的 cores
单 task 目标数据量 D ≈ 128MB（对齐 HDFS block，避免一次读半个 block）
理想 task 数 N ≈ 输入数据量 / D，且 N 取 P 的 2~3 倍（留出长尾和调度弹性）

所以：spark.sql.shuffle.partitions ≈ max(2P, 输入量 / 128MB)
```

**举例**：200 核集群、单日 shuffle 数据 2TB。

```text
P = 200
输入量 / 128MB = 2TB / 128MB ≈ 16000
→ shuffle.partitions ≈ max(400, 16000) = 16000
```

反过来，如果 shuffle 数据只有 5GB：`5000MB / 128MB ≈ 39`，那就设 `max(400, 39) = 400`——**不能设 16000，否则每个 task 只有 0.3MB，调度开销远大于计算**。

> [!danger] 反模式：`repartition(1)` 或 `coalesce(1)`
> 很多人为了让「输出只有一个文件」加 `coalesce(1)`。后果：**最后一个 stage 只有 1 个 task，跑在 1 个 core 上**，前面 199 个核全部空转，最后 10 分钟就是单线程。
> 正确做法：控制文件数用 `repartition(n)` 配合目标文件大小，或写入后跑独立的 compact 任务（见 [03-05 小文件治理](03-05%20小文件治理.md)）。

**生产上什么时候踩坑**：

| 现象 | 根因 | 量化 |
| --- | --- | --- |
| 200 核集群，stage 只有 20 个 task | 上游文件只有 20 个（小文件反而少），或 `coalesce(20)` | 利用率 10% |
| stage 有 20000 个 task，每个跑 2 秒 | `shuffle.partitions` 设太大 / 分区表有 20000 个小文件 | 调度开销 ≈ task 数 × 数十 ms，光调度就几分钟 |
| 单个 task 处理 8GB | 分区数远小于数据量，或严重数据倾斜 | 必然 OOM 或超长尾，见 [03-04 数据倾斜的 6 种打法](03-04%20数据倾斜的%206%20种打法.md) |
| AQE 开着但没合并分区 | `advisoryPartitionSizeInBytes` 太小，或分区本来就不碎 | 看 SQL 页 `AdaptiveSparkPlan` 里 `Exchange` 的实际 partition 数 |

### 2.5 内存：一个 executor 的内存怎么分

这是最容易背错的地方。**从 YARN 申请到 JVM 堆内，一共过四道手**：

```text
① YARN container 申请量
   = spark.executor.memory + spark.executor.memoryOverhead
   （overhead 默认 max(384MB, 0.1 × executor.memory)）

② JVM 堆 = spark.executor.memory
   堆内先扣掉 Reserved Memory = 300MB（硬编码，给 Spark 内部对象用）

③ 可用内存 = (heap - 300MB) × spark.memory.fraction
   （memory.fraction 默认 0.6，剩下 0.4 留给用户数据结构和元数据）

④ 这 60% 是 Unified Memory，Execution 和 Storage 共用
   初始按 spark.memory.storageFraction（默认 0.5）对半分，但可以互相借用：
   - Storage 占着的内存，Execution 需要时可以「驱逐」它（丢 cache）
   - Execution 占着的内存，Storage **不能**抢回来
```

**单 task 可用内存推导（面试高频计算题）**：

```text
单 task 可用 execution memory ≈ (executor.memory - 300MB) × 0.6 / executor.cores
```

| executor 配置 | 单 task 可用内存 | 评价 |
| --- | --- | --- |
| 16G / 1 core | 9.42G | 一个 task 独占 9.4G，**其余 core 全闲**，浪费 |
| 16G / 4 core | 2.36G | 平衡点，宽表聚合/大 join 基本够 |
| 16G / 8 core | 1.18G | 单 task 吃紧，宽表 + 排序容易 spill 甚至 OOM |
| 4G / 4 core | 0.56G | **必炸**。一个 500MB 的 hash map 就撑爆 |

**为什么 cores 建议 4~5 而不是越大越好**：三个约束同时起作用——
① 单 task 内存被摊薄（上面的公式）；② 单 executor 的 core 数超过 5 后，HDFS 客户端吞吐反而下降（经验值，与 DataNode 连接数相关）；③ 一个 executor 挂掉会损失 `cores` 个 task 的进度，cores 越大单点故障影响越大。

**生产上什么时候踩坑**：

| 现象 | 根因 | 排查手段 |
| --- | --- | --- |
| `Container killed by YARN for exceeding memory limits. 5.2 GB of 5 GB physical memory used` | 堆外（off-heap / netty / Python worker）超了 overhead | YARN RM 的 container 日志；SparkUI Executors 页看 `Off Heap Storage Memory`；Python 任务要把 overhead 提到 0.2~0.3 |
| task 频繁 spill，`Spill (Memory)` 很大 | 单 task execution memory 不够（cores 太多或内存太小） | Stages 页 → 某 stage → `Spill (Memory)` / `Spill (Disk)` 列 |
| GC time 占 task time > 10% | 老年代被大对象占满，频繁 Full GC | Executors 页 `GC Time` 列 ÷ `Total Duration` |
| cache 了 500GB 数据但 executor 只有 200GB 内存 | Storage 内存不足，block 被驱逐，下次用又要重算 | SQL 页看 `In-memory table` 的 size；Stages 页看 `input size` 是否反而变大 |

### 2.6 数据在内存里长什么样：Catalyst 与 Whole-Stage Codegen

一条 SQL 到真正执行，中间过四关：

| 阶段 | 做什么 | 你能看到的东西 |
| --- | --- | --- |
| **Analysis** | 解析 SQL，绑定 catalog（表、列、类型） | 报错 `Table or view not found` 在这层 |
| **Logical Optimization** | 谓词下推、列裁剪、常量折叠、外连接消除、`count(distinct)` 改写 | `explain extended` 的 `Optimized Logical Plan` |
| **Physical Planning** | 选 join 策略（broadcast/sort-merge/shuffle-hash）、插入 `Exchange` | `explain` 的 `Physical Plan` |
| **Codegen（Tungsten）** | 把整个 stage 的算子**编译成一个 Java 方法**，消除虚函数调用；数据用 `UnsafeRow` 二进制格式存放 | `explain` 里带 `*(n)` 前缀的节点就是被 codegen 的 |

**为什么这样设计**：解释执行模式下，每行数据过每个算子都要一次虚函数调用 + 装箱拆箱。Whole-stage codegen 把 `filter + project + aggregate` 熔成一个循环，直接在二进制内存上跑——这是 Spark 2.0 之后比 1.x 快 2~5 倍的主要原因。

> [!warning] 什么会打断 codegen
> 1. **Scala/Java UDF**：会退化成「解释执行 + 逐行函数调用」；
> 2. **Python UDF**：更糟，除了打断 codegen，还要把每行数据序列化到独立的 Python worker 进程（3.x 的 Arrow 优化只对 `pandas_udf` 生效）；
> 3. **某些复杂类型 / 超长表达式**：超过 `spark.sql.codegen.maxFields` / 方法字节码上限（64KB）时自动回退。
>
> **所以生产上应该怎么写**：能用内置函数就别写 UDF；必须写时，优先 Scala/Java UDF 或 `pandas_udf`，绝不用普通 `udf(lambda ...)` 处理上亿行。

### 2.7 调度：task 怎么落到 executor 上

两层调度，各管一段：

| 层 | 组件 | 管什么 | 关键行为 |
| --- | --- | --- | --- |
| Stage 级 | `DAGScheduler` | 把 DAG 切成 stage，按依赖顺序提交 | 一个 stage 的**所有** task 完成后才提交下游 stage |
| Task 级 | `TaskScheduler`（YARN 下是 `YarnScheduler`） | 把 task 塞进 executor 的空闲 core | 按本地性级别逐级降级 |

**本地性级别（从好到差）**：

| 级别 | 含义 | 触发条件 |
| --- | --- | --- |
| `PROCESS_LOCAL` | 数据和计算在**同一个 JVM** | 数据已在 executor 的 cache 里 |
| `NODE_LOCAL` | 同一台机器，不同进程 | HDFS 副本在本节点 / shuffle 文件在本节点 |
| `NO_PREF` | 无所谓 | 数据来自 Kafka 等无位置概念的数据源 |
| `RACK_LOCAL` | 同一机架 | 本机没有副本 |
| `ANY` | 跨机架 | 等不及了，随便给一个 |

调度器会**先等 `spark.locality.wait`（默认 3s），等不到就降一级**，再等 3s 再降。所以 `locality.wait` 调太大（比如 60s）会让 task 排队等本地性，集群空转；调成 0 会让 task 全变 `ANY`，网络 IO 暴涨。

**其他调度开关**：

```text
spark.scheduler.mode              FIFO（默认）/ FAIR
spark.speculation                 默认 false
spark.task.maxFailures            默认 4
spark.stage.maxConsecutiveAttempts 默认 4
spark.dynamicAllocation.enabled   默认 false（YARN 上需配 ExternalShuffleService）
```

**生产上什么时候踩坑**：

| 场景 | 现象 | 应对 |
| --- | --- | --- |
| 多团队共用一个 YARN 队列，FIFO 模式 | 大任务把队列占满，小任务排队几小时 | 开 FAIR 并配 `spark.scheduler.pool`，或按队列隔离资源 |
| 长尾 task（3 个 task 跑 40 分钟，其余 2 分钟） | 关着推测执行，长尾拖住整个 stage | 开 `spark.speculation=true` + `spark.speculation.multiplier=1.5`；但**根治要解决倾斜** |
| 动态资源分配 + 没有 ExternalShuffleService | executor 被回收后 shuffle 文件丢失，下游 `FetchFailedException` | YARN 上必须部署 `spark-<version>-yarn-shuffle.jar` 并开 `yarn.nodemanager.aux-services` |
| 推测执行在写外部系统的任务里开启 | 同一批数据被写两遍（重复数据） | 写 Hudi/Iceberg/MySQL 的任务**关掉**推测执行，靠幂等 + 重跑 |

### 2.8 容错与重算：血缘的代价

Spark 的容错不靠副本，靠**血缘（lineage）重算**。

| 丢失的东西 | 恢复方式 | 代价 |
| --- | --- | --- |
| 某个 task 失败（代码异常/节点抖动） | 只重跑这个 task | 小 |
| 窄依赖的上游 partition 丢失 | 重算那一条链上的 partition | 小（只影响 1 个分区） |
| **宽依赖的上游 map 输出丢失**（executor 挂 / shuffle 文件被清理） | 重跑**整个上游 stage 的所有 map task** | **极大**，可能是几十分钟 |
| driver 丢失 | 无恢复 | 整个 application 失败 |

> [!tip] 所以生产上应该怎么写
> - 超长血缘（20+ 次 join、反复 union）的链路，在中途 `persist()` 或 `checkpoint()` 截断，避免一次失败引发雪崩式重算。
> - 关键宽依赖的上游 stage，若下游还要多次使用，**宁可多花磁盘也不要重算**：`df.persist(StorageLevel.MEMORY_AND_DISK)`。
> - 看到 `FetchFailedException` 不要只加重试次数，要查**为什么** map 输出丢了（executor 被 kill？磁盘满？动态资源分配回收了 executor？）。

---

## 三、动手实验

> 环境：Spark 3.5（local[4] 即可，`spark-sql` 或 `pyspark` 都行）。所有实验都在 `tmp` 库，可随时 drop。
> 建议边跑边开 SparkUI（`http://localhost:4040`），本节所有结论都能在 UI 上直接看到。

### 3.1 建表 + 造数

```sql
create database if not exists tmp;
drop table if exists tmp.fact_order;

create table tmp.fact_order (
    order_id bigint,
    user_id  bigint,
    amount   double,
    dt       string
) using parquet partitioned by (dt);

-- 造 3 天数据，每天 100 万行；user_id 只有 1 万个值（故意的，方便看倾斜）
insert overwrite table tmp.fact_order partition (dt='2026-10-01')
select id as order_id, id % 10000 as user_id, (id % 1000) * 0.01 as amount, '2026-10-01' as dt
from range(1000000);

insert overwrite table tmp.fact_order partition (dt='2026-10-02')
select id, id % 10000, (id % 1000) * 0.01, '2026-10-02' from range(1000000);

insert overwrite table tmp.fact_order partition (dt='2026-10-03')
select id, id % 10000, (id % 1000) * 0.01, '2026-10-03' from range(1000000);

select dt, count(*) from tmp.fact_order group by dt;
-- 预期：三天各 1000000 行
```

### 3.2 实验一：验证「一个 action = 一个 Job，shuffle 切 Stage」

**验证假设**：`select` 不产生 job；`group by` 引入 1 个 `Exchange` → 2 个 stage；stage 内 task 数 = 分区数。

```sql
-- ① 纯窄依赖：应该只有 1 个 stage
set spark.sql.adaptive.enabled = false;
explain extended
select count(*) from tmp.fact_order where dt='2026-10-01' and amount > 100;
```

**预期输出**（节选，重点看 `Exchange` 出现次数）：

```text
== Physical Plan ==
*(2) HashAggregate(keys=[], functions=[count(1)])
+- Exchange SinglePartition              ← 全局 count 需要汇总到 1 个分区，这是 1 次 shuffle
   +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
      +- *(1) Project
         +- *(1) Filter ((amount > 100.0) AND (dt = 2026-10-01))
            +- *(1) ColumnarToRow
               +- FileScan parquet tmp.fact_order[amount#..,dt#..]
                  PartitionFilters: [isnotnull(dt#..), (dt#.. = 2026-10-01)]
                  PushedFilters: [IsNotNull(amount), GreaterThan(amount,100.0)]
```

```sql
-- ② 带 group by：还是 1 个 Exchange，但分区数变成 shuffle.partitions
set spark.sql.shuffle.partitions = 8;
explain extended
select user_id, sum(amount) from tmp.fact_order where dt='2026-10-01' group by user_id;
-- 看到 Exchange hashpartitioning(user_id#.., 8) → 8 个 task 的 ResultStage
```

**观察要点**：

| 看什么 | 在哪看 | 期望 |
| --- | --- | --- |
| Job 数 | SparkUI → Jobs | 每执行一条 action 多 1 个 |
| Stage 数 | SparkUI → Stages，展开某个 Job | `Exchange` 数 + 1 |
| Stage 的 task 数 | Stages 页 → 点进 stage → Summary Metrics 的 `Total Tasks` | 读文件 stage ≈ 文件数；shuffle 后 stage = 8 |
| 分区裁剪是否生效 | `explain` 的 `PartitionFilters` | 有值 |

**结论**：**Stage 的划分只跟 shuffle 有关，跟数据量、跟 SQL 复杂度都无关**。所以「怎么减少 stage」= 「怎么减少 shuffle」，而不是「怎么把 SQL 写短」。

### 3.3 实验二：窄依赖 pipeline —— 加算子不加 stage

**验证假设**：连续多个窄依赖算子会被熔进同一个 stage，task 数不变，不会多一次落盘。

```sql
set spark.sql.adaptive.enabled = false;
set spark.sql.shuffle.partitions = 8;

-- A：只读
explain extended select order_id, amount from tmp.fact_order where dt='2026-10-01';

-- B：套 5 层窄依赖
explain extended
select order_id, round(amount * 2, 2) as a2
from (
  select order_id, amount * 2 as amount
  from (
    select order_id, amount from tmp.fact_order
    where dt='2026-10-01' and amount > 10
  ) t1
  where amount < 100000
) t2
where order_id > 0;
```

**预期输出**：两段 `explain` 的 `Exchange` 数量**都是 0**（都在同一个 stage 里，Plan 只有 `*(1)` 编号）。B 的计划里 `Filter`/`Project` 会全部合并到 `FileScan` 之后的同一个 codegen 段。

**结论**：**窄依赖算子是「免费的」**（除了 CPU），代价只在 `*(1)` 那个循环里。所以优化时不要怕多写几层子查询——真正要怕的是多一次 shuffle。

### 3.4 实验三：shuffle 分区数如何决定 task 数（4 vs 200）

**验证假设**：task 数与数据量无关，只与分区数有关；分区数过小会浪费核，过大会被调度开销吃掉。

```sql
set spark.sql.adaptive.enabled = false;   -- 关掉 AQE，否则会被自动合并，看不到原始效果

set spark.sql.shuffle.partitions = 4;
select count(distinct user_id) from tmp.fact_order where dt='2026-10-01';

set spark.sql.shuffle.partitions = 200;
select count(distinct user_id) from tmp.fact_order where dt='2026-10-01';
```

**预期输出**（Stages 页对比）：

| 分区数 | ResultStage 的 Total Tasks | 单 task 处理量 | 现象 |
| --- | --- | --- | --- |
| 4 | 4 | ≈ 25 万行 / 约 5MB | 只有 4 个核在跑，local[4] 刚好，集群上就是浪费 |
| 200 | 200 | ≈ 5000 行 / 约 100KB | 每个 task 几十毫秒，`Scheduler Delay` 占比超过 50% |

再打开 AQE 跑一次：

```sql
set spark.sql.adaptive.enabled = true;
set spark.sql.shuffle.partitions = 200;
select count(distinct user_id) from tmp.fact_order where dt='2026-10-01';
-- Stages 页会看到 ResultStage 实际 task 数远小于 200（被 coalesce 合并）
-- SQL 页的 AdaptiveSparkPlan 里能看到 "number of partitions: N"
```

**结论**：`shuffle.partitions` 是**上限**不是**实际值**（AQE 开启时）。所以 AQE 时代可以把它设大一点（比如 `2~3 × 总核数` 的 2 倍），让 AQE 去合并；但**不能指望 AQE 帮你把小分区变大到合理大小**——它只会合并，不会拆分。

### 3.5 实验四：错误写法 vs 正确写法 —— 未 cache 的重复 action

**验证假设**：同一个 DataFrame 上多次 action，如果不 cache，每次都会**从头重算整条血缘**。

**错误写法**：

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("lab4").getOrCreate()

df = spark.table("tmp.fact_order").filter("dt='2026-10-01'")

print("count:", df.count())                              # Job 1
df.groupBy("user_id").sum("amount").show(5)              # Job 2
df.select("amount").describe().show()                    # Job 3
```

**正确写法**：

```python
df = spark.table("tmp.fact_order").filter("dt='2026-10-01'")
df.cache()                    # 或 persist(StorageLevel.MEMORY_AND_DISK)
df.count()                    # 第一次 action 触发物化，把数据装进 Storage

print("count:", df.count())                              # 命中 cache
df.groupBy("user_id").sum("amount").show(5)              # 命中 cache
df.select("amount").describe().show()                    # 命中 cache
```

纯 SQL 版本（`spark-sql` 里也能用）：

```sql
cache table tmp.cached_order as
select order_id, user_id, amount from tmp.fact_order where dt='2026-10-01';

select count(*) from tmp.cached_order;
select user_id, sum(amount) from tmp.cached_order group by user_id;
select * from tmp.cached_order limit 10;

uncache table tmp.cached_order;
```

**预期输出对比**（SparkUI → Jobs 页）：

| 指标 | 错误写法 | 正确写法（cache） |
| --- | --- | --- |
| Job 数 | 3 | 4（多一个物化 job） |
| 三次查询的 `Input Size` 之和 | ≈ 3 × 单分区大小 | ≈ 1 × 单分区大小（后三次走 memory） |
| Stages 页的 `FileScan` 次数 | 3 次 | 1 次 |
| Executors 页 `Storage Memory` | 0 | > 0 |

**结论**：**cache 的本质是「用内存买血缘」**——把重算变成读内存。判断要不要 cache 的三条标准：
1. 同一份数据被 **2 次以上** action 使用；
2. 血缘**很长或很贵**（多次 join、读大表）；
3. 数据量**放得下**（cache 的总量 < 集群 Storage 内存 × 0.5，否则会驱逐，等于白 cache）。

> [!danger] 反向坑：乱 cache
> 一次性用的 DataFrame 也 cache，会挤占 execution 内存 → 频繁 spill → 反而更慢。**看到 Executors 页 `Storage Memory` 占满且 `Spilled` 大增，就是 cache 过量的信号。**

### 3.6 实验五：`collect()` vs `write` —— driver OOM 的现场

**验证假设**：`collect()` 把所有结果拉回 driver 内存；超过 `spark.driver.maxResultSize` 直接失败。

**错误写法**：

```python
df = spark.table("tmp.fact_order").filter("dt='2026-10-01'")
rows = df.collect()          # 100 万行 × 4 列全部进 driver 堆
print(len(rows))
```

**正确写法**：

```python
# ① 需要落盘 → 用 write，结果不经过 driver
df.write.mode("overwrite").parquet("/tmp/out/fact_order_dt=2026-10-01")

# ② 需要看样本 → 用 limit + show
df.limit(20).show()

# ③ 需要统计 → 先聚合再 collect（结果只有几十行）
df.groupBy("user_id").sum("amount").collect()
```

**预期输出**：

| 写法 | 结果 | 说明 |
| --- | --- | --- |
| `collect()` 100 万行 | 可能成功（100 万行 ≈ 几十 MB），但数据量到 1 亿行就会报 `Total size of serialized results ... is bigger than spark.driver.maxResultSize (1024.0 MB)` | 单机内存上限 |
| `write` | 成功，driver 只收 task 的元信息 | 结果不经过 driver |
| `limit(20).show()` | 成功，只回传 20 行 | 内部会先做 `LocalLimit` 再收 |

**结论**：**driver 只该拿「小结果」**——一个数字、几十行、或者一份 task 状态。凡是「行数 × 列数」可能很大的，一律走 `write`。这条规则同时解释了为什么 `count()` 比 `show()` 便宜（前者只回传一个 long，后者要回传 n 行）。

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| 任务 99% 卡在 `200/201 tasks`，一卡 40 分钟 | 数据倾斜 + 推测执行关闭，长尾 task 拖住整个 stage | SparkUI → Stages 页 → 该 stage 的 `Summary Metrics`，对比 `Max` 与 `75th percentile` 的 Duration / Shuffle Read Size；点 `Max` 那一行看是不是某个 key 特别大 | 先开 `spark.speculation=true` 止血；根治见 [03-04 数据倾斜的 6 种打法](03-04%20数据倾斜的%206%20种打法.md) |
| `Container killed by YARN for exceeding memory limits` | 堆外内存（netty / Python worker / off-heap）超出 `memoryOverhead` | YARN RM 日志搜 container id 看 `physical memory used`；SparkUI → Executors 页看 `Off Heap Storage Memory` 与 `Peak JVM Memory` | Python 任务把 `spark.executor.memoryOverheadFactor` 提到 0.2~0.3；`spark.executor.memoryOverhead` 直接写绝对值 |
| `Total size of serialized results of N tasks is bigger than spark.driver.maxResultSize` | 用了 `collect()` / `toPandas()` 把大结果拉回 driver | driver 日志里的这行报错；Jobs 页看该 job 的 `Output Size` | 改 `write` 落盘；必须取回时先 `limit` 或先聚合 |
| 一个 SQL 触发 5 个 Job，跑得比预期慢一倍 | 多个 action（`count` + `show` + `write`）复用同一 DataFrame 但没 cache | Jobs 页数 Job 数，对比每个 job 的 `Input Size` 是否重复 | `persist(MEMORY_AND_DISK)` 后统一 action，用完 `unpersist` |
| stage 有 3 万个 task，光调度就 5 分钟 | `shuffle.partitions` 太大 / 分区表小文件太多 | Stages 页 `Total Tasks`；SQL 页看 `Exchange` 的 partition 数；`dfs -count` 看文件数 | 降 `shuffle.partitions`；开启 AQE 让它合并；先做小文件 compact |
| 200 核集群，stage 只有 8 个 task | 上游文件只有 8 个 / 写了 `coalesce(8)` / `shuffle.partitions` 太小 | Executors 页 `Active Tasks` 远小于 `Total Cores`；Stages 页 `Total Tasks` | 调大 `shuffle.partitions`；读文件阶段用 `repartition(n)` 主动拆分 |
| `FetchFailedException: Failed to connect to host X` 反复重试 | 上游 executor 被 kill（OOM / 动态资源回收），它写的 shuffle 文件丢了 | Executors 页看是否有 `Dead` executor；YARN 日志搜 `Container killed`；Stages 页看是否在重跑上游 stage | 部署 ExternalShuffleService；关掉激进的 dynamic allocation；修复上游 OOM |
| task 的 `GC Time / Duration` 超过 15% | execution 内存不足或大对象（大 broadcast、大数组） | Executors 页 `GC Time` 列 ÷ `Total Duration`；开启 `-XX:+PrintGCDetails` 看老年代占用 | 降 `executor.cores`（给单 task 更多内存）；减 `memory.fraction` 之外的浪费；改用 `MEMORY_AND_DISK` |
| Stages 页 `Locality Level` 全是 `ANY` | 数据与计算不在一处；或 `locality.wait=0` | Stages 页 `Locality Level` 分布；对比 `Input Size` 与网络流量 | 提高副本数让计算靠近数据；`locality.wait` 回到 3s 左右；避免跨机房 |
| 写出的表只有 1 个文件，最后阶段单线程 | 结尾写了 `coalesce(1)` / `repartition(1)` | Stages 页最后一个 stage 的 `Total Tasks = 1` | 改成 `repartition(N)`（N ≈ 数据量 / 256MB）；或用独立 compact 任务 |
| client 模式下 SSH 断开会话，任务直接 FAILED | driver 跑在提交机上，进程被杀 | YARN 上 application 状态 `FAILED`，日志里 `Connection refused` / driver 日志中断 | 一律 `--deploy-mode cluster`；或 `nohup ... &` + `setsid` |
| 同样的 SQL，第一次 3 分钟，重跑 8 分钟 | 第一次命中了 OS page cache / 第一次 cache 了；重跑时资源被别的任务挤占 | SparkUI 对比两次的 `Scheduler Delay` 与 `Shuffle Read Blocked Time` | 用 `EXPLAIN` + UI 数据判断，不要靠「感觉变慢」下结论 |

---

## 五、面试追问

> [!question] 1. 一条 `select ... group by ...` 会生成几个 Job、几个 Stage？
> **一句话答法**：1 个 Job（只有 1 个 action）；Stage 数 = 物理计划里 `Exchange` 节点数 + 1，普通 `group by` 是 1 个 `Exchange` → 2 个 Stage（1 个 ShuffleMapStage + 1 个 ResultStage）。
> **追问陷阱**：「如果 SQL 里有 `union all` 呢」——`union all` 是窄依赖，不加 Stage；但如果有 `union`（去重），会额外引入一次 shuffle，多一个 Stage。

> [!question] 2. Stage 是按什么切的？为什么不能把整个 DAG 当一个 Stage？
> **一句话答法**：按 `ShuffleDependency` 切。因为 shuffle 的 reduce 端必须等**所有** map 端输出就绪，这是一个天然的同步点，必须显式化成 Stage 边界才能做检查点、失败重试和资源回收。
> **追问陷阱**：「Stage 越少越好吗」——不一定。Stage 少意味着 shuffle 少，但如果是靠 `repartition` 硬凑的「假合并」，反而多一次落盘。

> [!question] 3. 宽依赖和窄依赖的区别？为什么窄依赖能 pipeline？
> **一句话答法**：窄依赖是「父分区到子分区一对一/多对一」，数据不需要跨节点搬，所以可以像流水线一样一个分区算完直接喂给下一个算子；宽依赖要按 key 重分布，必须落盘 + 走网络。
> **追问陷阱**：「`coalesce` 是宽依赖吗」——不是，窄依赖（只减少分区数，不重分布）；「`repartition` 呢」——是宽依赖，因为它要均匀重分布。

> [!question] 4. 并行度由什么决定？task 越多越好吗？
> **一句话答法**：读文件的 stage 由文件 split 数决定，shuffle 后的 stage 由 `spark.sql.shuffle.partitions`（AQE 会合并）决定。task 太多会带来调度开销（每个 task 数十毫秒的启动 + 序列化），太少则核闲着。
> **追问陷阱**：「怎么定这个数」——目标单 task 128MB 左右，且 task 总数是总核数的 2~3 倍，取两者较大值。

> [!question] 5. 一个 executor 配 4 core 16G，跑 8 个 task，每个 task 能用多少内存？
> **一句话答法**：只有 4 个 core 所以最多同时跑 4 个 task；单 task 可用 execution memory ≈ (16G − 300MB) × 0.6 ÷ 4 ≈ **2.36G**。对 YARN 申请量还要加 overhead = max(384MB, 1.6G) ≈ 17.6G。
> **追问陷阱**：「为什么是 0.6 和 300MB」——`spark.memory.fraction` 默认 0.6（剩 0.4 给用户对象和元数据），`Reserved Memory` 300MB 是硬编码的；「cores 能不能设 8」——单 task 只剩 1.18G，宽表聚合会 spill 甚至 OOM。

> [!question] 6. client 模式和 cluster 模式的区别？生产用哪个？
> **一句话答法**：区别只在 **driver 跑在哪**——client 模式 driver 在提交机上，cluster 模式 driver 在集群的 container 里。生产一律 cluster，因为提交机断网/重启会直接杀死 driver，整个作业作废。
> **追问陷阱**：「那 client 模式还有什么用」——调试方便（driver 日志直接在终端、SparkUI 直连），以及 `spark-shell` / `pyspark` 交互式只能 client。

> [!question] 7. 为什么同一个 DataFrame 第二次 action 还要重算？
> **一句话答法**：因为 Spark 的容错模型是「血缘重算」而不是「结果保留」。不 `cache` 的话，每个 action 都会从头遍历血缘重新计算。
> **追问陷阱**：「那 `cache` 了就一定快吗」——不一定。如果数据量超过 Storage 内存，block 会被驱逐，下次用还是要重算，而且驱逐过程还会挤占 execution 内存导致 spill。

> [!question] 8. Spark 3 的 AQE 在哪个阶段介入？和 Catalyst 什么关系？
> **一句话答法**：AQE 在**物理计划执行过程中**介入，每完成一个 shuffle stage 就拿到真实的统计信息（分区大小、是否倾斜），然后**重新优化剩余的计划**（合并小分区、切换 join 策略、拆分倾斜分区）。Catalyst 是编译期的静态优化，AQE 是运行期的动态优化。
> **追问陷阱**：「AQE 能替代手工调优吗」——不能。它只能在 shuffle 之后调整，管不了 shuffle 之前的读放大、管不了业务级倾斜（比如某个 key 天生占 80%），也管不了写入端的小文件。

> [!question] 9. 什么是推测执行？为什么生产上经常关掉？
> **一句话答法**：某个 task 跑得明显比别人慢时（超过 `spark.speculation.multiplier` 倍中位数），在另一个 executor 上启动一份副本，谁先完成用谁的结果。生产上关掉是因为**写外部系统的任务会写两遍**（重复数据），以及会额外消耗资源。
> **追问陷阱**：「那长尾怎么办」——推测执行只是止血，根因是倾斜，要按 [03-04](03-04%20数据倾斜的%206%20种打法.md) 的方法处理。

> [!question] 10. Executor 挂了，正在跑的 task 会怎样？Stage 会重跑吗？
> **一句话答法**：该 executor 上的 task 标记失败并按 `spark.task.maxFailures` 重试（换到别的 executor）；如果它写的 shuffle 文件丢失，下游会抛 `FetchFailedException`，此时**上游整个 stage 要重跑**（因为要重新生成 map 输出）。
> **追问陷阱**：「重跑上游的代价怎么估」——等于上游 stage 的完整执行时间，这也是宽依赖比窄依赖「贵」的核心原因。

> [!question] 11. Driver 具体做了什么？为什么说 driver 是单点？
> **一句话答法**：driver 负责建 Session、跑 Catalyst、用 DAGScheduler 切 stage、用 TaskScheduler 派 task、收集结果和 task 状态。它是唯一持有全局 DAG 和调度状态的进程，没有备份，所以挂了作业就失败。
> **追问陷阱**：「driver 内存为什么也会 OOM」——`collect` 大结果、广播大变量、或者 task 数极多时 driver 端要维护几万个 task 的状态对象。

> [!question] 12. 从 SparkUI 上，你怎么一眼判断「是并行度不够」还是「是数据倾斜」？
> **一句话答法**：看 Stages 页某个 stage 的 `Summary Metrics`——如果 `Max` 和 `75th percentile` 的 Duration **差不多**，说明所有 task 都慢 → 并行度/单 task 数据量问题；如果 `Max` 是 `75th` 的 5 倍以上，且 `Shuffle Read Size` 的 `Max` 也异常大 → 倾斜。
> **追问陷阱**：「为什么不能只看总耗时」——总耗时被最慢的 task 决定（木桶效应），必须看分布。

---

## 六、自测题

> [!question] 1.（概念辨析）下面关于 Job / Stage / Task 的说法，哪几项正确？
> A. 一个 SQL 语句一定只生成一个 Job
> B. 一个 Stage 内的 task 数等于该 Stage 最后一个 RDD 的分区数
> C. Stage 的数量由数据量决定，数据量越大 Stage 越多
> D. `union all` 会新增一个 Stage
> E. 窄依赖算子会被 pipeline 进同一个 Stage

> [!question] 2.（概念辨析）判断并说明理由：**「`repartition(200)` 和 `coalesce(200)` 效果一样，都能把并行度提到 200。」**

> [!question] 3.（计算推导）集群有 10 个 executor，每个 4 core / 16G，YARN 队列可用内存 200G。当前要对 1.2TB 的 shuffle 数据做 `group by`。请计算：① 总并行度；② 单 task 可用 execution memory；③ 建议的 `spark.sql.shuffle.partitions`；④ 这个配置能起几个 executor。

> [!question] 4.（场景设计）一个日更任务跑了 50 分钟。SparkUI 显示：Jobs 页有 2 个 job；Job 0 有 3 个 stage，其中 Stage 1 有 200 个 task，`Max Duration` 32 分钟、`75th percentile` 2.1 分钟，`Max Shuffle Read Size` 是 18GB（其余约 200MB）。请给出：① 根因判断依据；② 10 分钟内要确认的三件事；③ 止血方案和根治方案。

> [!question] 5.（场景设计）你要做一个「订单事实表 left join 商品维表（200 万行，300MB）」的日更宽表，数据量 500GB。请从执行模型角度设计：① 怎么让这个 join 不产生 shuffle；② 整个链路预期有几个 stage；③ 如果维表涨到 3GB 该怎么办。

> [!success]- 答案与解析（点击展开）
> **1. B、E**
> - A ❌：一个 SQL 里如果有多个 action（比如 shell 里先 `select` 看结果再 `insert`），就是多个 Job；另外写多个 `insert` 分支（`from t insert ... insert ...`）也是一个 action 一个 Job。
> - B ✅：Stage 内 task 数 = 该 stage 输出 RDD 的 partition 数（读文件 stage 由 split 数决定，shuffle 后 stage 由 shuffle 分区数决定）。
> - C ❌：**Stage 数只跟 shuffle 次数有关**。100GB 和 100TB 的同一条 SQL，只要 shuffle 次数一样，Stage 数就一样。
> - D ❌：`union all` 是窄依赖（`RangeDependency`），不切 Stage、不 shuffle。只有 `union`（去重）才会。
> - E ✅：窄依赖可以 pipeline，多个算子熔进同一个 codegen 段。
>
> **错因分析**：选 C 的人是把「数据量大 → 慢」错误归因为「Stage 多」。真正变多的是**单个 task 处理的数据量**和 **task 数**，不是 Stage 数。
>
> ---
>
> **2. 错误**
> - `repartition(200)`：**宽依赖**，做一次完整 shuffle，把数据均匀打散到 200 个分区。**能从少变多**。
> - `coalesce(200)`：**窄依赖**，只是把相邻分区合并，**不能从少变多**。如果当前只有 50 个分区，`coalesce(200)` 结果还是 50 个分区（实际生效为 min(当前分区数, 200)），根本不会提升并行度。
> - 正确用法：**要变多必须用 `repartition`（有 shuffle 代价）；要变少用 `coalesce`（无 shuffle，但可能不均匀）**。
>
> **错因分析**：把 `coalesce` 当成 `repartition` 的「省 shuffle 版本」是常见误解。`coalesce` 的语义是「减少分区」，不是「设置分区」。
>
> ---
>
> **3. 计算**
>
> ① **总并行度** = 10 × 4 = **40 个 task slot**。
>
> ② **单 task 可用 execution memory**
> ```text
> 可用内存 = (16G − 300MB) × 0.6 = 15.7 × 0.6 ≈ 9.42G（Unified Pool，execution + storage 共用）
> 单 task ≈ 9.42G / 4 = 2.36G
> ```
>
> ③ **`spark.sql.shuffle.partitions`**
> ```text
> 按核数：2~3 × 40 = 80~120
> 按数据量：1.2TB / 128MB ≈ 9600
> 取较大值 → 建议 9600 左右（AQE 开启时会自动合并到 64MB/分区，即约 19000 → 实际合并到 ~9600）
> ```
> 注意：这里**不能用 120**。1.2TB / 120 = 10GB/task，必然 OOM。这条是本题的核心考点——**分区数要同时满足「并行度够」和「单 task 数据量合理」，取较大值**。
>
> ④ **能起几个 executor**
> ```text
> 单 executor 申请量 = 16G + max(384MB, 1.6G) = 17.6G
> 200G / 17.6G ≈ 11.4 → 能起 11 个
> ```
> 所以实际并行度是 11 × 4 = 44（比设计的 40 略高，无害）。若要严格按 10 个 executor 规划，队列至少要 176G。
>
> **错因分析**：最常见的错误是 ③ 只按核数算（得出 120），忽略单 task 数据量。记住公式是 **`max(2~3 × 核数, 数据量 / 128MB)`**，不是二者取一。
>
> ---
>
> **4. 场景分析**
>
> **① 根因判断依据**
> - `Max 32 分钟` vs `75th 2.1 分钟` → 相差 **15 倍**，远超 5 倍阈值 → **数据倾斜**，不是并行度不足。
> - `Max Shuffle Read Size = 18GB` vs 其余 200MB → 相差 **90 倍** → 确认是**某个 key 的数据量爆炸**（典型：`user_id = null` 或默认值、大商家 ID）。
> - Job 数 2 个：说明有 2 个 action，第二个 job 可能是 `count` 校验或 `insert`。Stage 1 是中间某个 shuffle stage。
>
> **② 10 分钟内要确认的三件事**
> 1. **Stages 页 → Stage 1 → 点 `Max` 那一行**：看 `Shuffle Read Size / Records`，确认是单分区数据量爆炸而不是单分区计算慢（GC 高、spill 大）。
> 2. **Executors 页**：看该 executor 的 `GC Time` 和 `Spill (Disk)`。如果 spill 很大 → 单 task 内存不够，是「倾斜 + 内存不足」复合问题。
> 3. **SQL 页 → 该 stage 对应的算子**：确认是 `HashAggregate` 还是 `SortMergeJoin` 的 shuffle，定位是哪个 key（用 `select key, count(*) from ... group by key order by 2 desc limit 20` 在旁路验证）。
>
> **③ 止血方案**（10 分钟内可执行）
> - 开 `spark.speculation=true` + `spark.speculation.multiplier=1.5`：让慢 task 起副本，能把 32 分钟压到 5 分钟内。
> - 开 AQE 的倾斜 join：`spark.sql.adaptive.skewJoin.enabled=true`（3.5 默认已开），确认 `skewedPartitionFactor=5.0` / `skewedPartitionThresholdInBytes=256MB` 是否适配。
> - 临时把 `spark.executor.cores` 从 4 降到 2，给单 task 更多内存，避免 spill。
>
> **根治方案**
> - 若是 `null` / 默认值热点：`where key is not null` 提前过滤，或把 null 单独打散（加随机后缀）。
> - 若是真实大 key：两阶段聚合（加盐 → 局部聚合 → 去盐 → 全局聚合），见 [03-04 数据倾斜的 6 种打法](03-04%20数据倾斜的%206%20种打法.md)。
> - 若是 join 导致的倾斜且小表能广播：改成 broadcast join，直接消灭这次 shuffle。
>
> **错因分析**：很多人一看到「慢」就去调 `shuffle.partitions`。但本例中 199 个 task 都是 2 分钟，说明分区数已经合理；调大分区数对那个 18GB 的 task 毫无帮助（**倾斜是 hash 决定的，加分区数只会让「一个巨型分区 + 更多空分区」**）。
>
> ---
>
> **5. 宽表设计**
>
> **① 让 join 不产生 shuffle**
> 维表 300MB，默认 `spark.sql.autoBroadcastJoinThreshold = 10MB`（3.5 默认）**不会**自动广播。两个办法：
> ```sql
> -- 办法一：显式 hint（推荐，意图清晰）
> select /*+ BROADCAST(dim) */ f.order_id, f.amount, d.category
> from dwd.fact_order f
> left join dim.product d on f.product_id = d.product_id
> where f.dt = '${bizdate}';
>
> -- 办法二：调阈值（影响全局，慎用）
> set spark.sql.autoBroadcastJoinThreshold = 314572800;  -- 300MB
> ```
> 广播后，维表被分发到每个 executor 的内存里，事实表**原地**做 map-side join，**没有 shuffle、没有 Exchange**。
>
> **② 预期 stage 数**
> ```text
> 读事实表（窄） → filter/select（窄） → BroadcastHashJoin（窄！） → 写出
> → 只有 1 个 Stage，0 次 shuffle
> ```
> 对比不广播的情况：`SortMergeJoin` 需要两侧都 `Exchange`（2 次 shuffle）→ **3 个 Stage**。
> **这就是「广播 join 把一个 3-stage 作业压成 1-stage」的具体含义**——省掉的不只是网络传输，还有两轮落盘和两轮调度。
>
> **③ 维表涨到 3GB 怎么办**
> | 方案 | 适用条件 | 代价 |
> | --- | --- | --- |
> | 继续广播 | executor 内存足够（单 executor 至少要能放下 3GB × 副本数），且 executor 数不多 | 3GB × 100 executor = 300GB 网络传输，driver 端序列化压力大；**不推荐** |
> | 维表瘦身后再广播 | 只 `select` 用到的列，通常能从 3GB 降到 300MB | 最优先尝试，几乎无成本 |
> | 分桶表 bucket join | 两表按 `product_id` 同列分桶且桶数成倍数 | 建表成本高，Spark 对 Hive 分桶支持有限 |
> | 接受 shuffle，用 SortMergeJoin | 通用方案 | 3 个 stage，1 次 shuffle（维表侧） |
> | 维表下沉到 KV 存储（HBase/Redis） | 点查场景，不需要全量 join | 引入外部依赖，网络 RTT 高，不适合批量 join |
>
> **错因分析**：这题最容易错在「以为广播阈值调大就万事大吉」。实际上广播的成本是 **O(维表大小 × executor 数)** 的网络传输 + 每 executor 一份内存副本，维表一大就得不偿失。正确顺序是：**先瘦身 → 再广播 → 不行才 shuffle**。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 直接下游 | [03-02 Shuffle 原理与调优](03-02%20Shuffle%20原理与调优.md) | 本节讲了「shuffle 是 stage 边界」，下一节讲「这个边界上到底发生了什么、为什么慢」——Write/Read 流程、分区数、spill、序列化 |
| 直接下游 | [03-03 Join 策略选择](03-03%20Join%20策略选择.md) | 本节说 broadcast join 能把 3 个 stage 压成 1 个，下一节讲三种 join 策略的阈值和强制手段 |
| 直接下游 | [03-06 AQE 自适应执行](03-06%20AQE%20自适应执行.md) | 本节反复提到「AQE 会合并分区」，下一节讲它到底在哪个时点、依据什么统计信息做决策 |
| 直接下游 | [03-09 慢任务定位方法论](03-09%20慢任务定位方法论.md) | 本节的实验都是「用 SparkUI 验证假设」的雏形，那一节把它系统化成六个必看面板的排查流程 |
| 上游 | [01-09 排序：order、sort、distribute、cluster by](../01%20模块1%20Hive%26Spark%20SQL/01-09%20排序：order、sort、distribute、cluster%20by.md) | `distribute by` / `cluster by` 直接决定 shuffle 的分区方式，是本节「分区数决定 task 数」在 SQL 层的入口 |
| 上游 | [01-03 SQL 执行顺序与常见误区](../01%20模块1%20Hive%26Spark%20SQL/01-03%20SQL%20执行顺序与常见误区.md) | 逻辑执行顺序决定了 Catalyst 能下推什么，进而决定物理计划里 Exchange 的位置 |
| 容易混淆 | [03-07 内存管理与序列化](03-07%20内存管理与序列化.md) | 本节的 2.5 节只讲了「内存怎么分」，那一节讲「分到之后怎么用、OOM 的三种堆栈怎么区分」 |
| 延伸 | [03-08 资源规划与参数模板](03-08%20资源规划与参数模板.md) | 本节的并行度公式（`P = executor × cores`）是资源规划那一节的输入 |

> [!tip] 学习建议
> 这一节必须建立两个「肌肉记忆」：
> ① **看到慢，先数 task 数**（Stages 页 `Total Tasks` 和 `Max vs 75th` 的分布），而不是先改参数；
> ② **看到 stage 多，先找 Exchange**（`explain` 里搜 `Exchange`），而不是先加资源。
> 建议把实验 3.2 和 3.4 在本地 `local[4]` 上各跑一遍，亲眼看到「分区数从 4 改到 200，task 数跟着变、数据量一点没变」——这个画面比任何文字都更能帮你记住「Task 数由分区数决定」。
