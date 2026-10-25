---
title: 01-10 Hive 与 Spark SQL 的差异
module: 模块1 Hive & Spark SQL
difficulty: ⭐⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - Hive
  - SparkSQL
  - 迁移
  - 兼容性
---

# 01-10 Hive 与 Spark SQL 的差异

> [!abstract] 一句话结论
> 「Hive 能跑，Spark 报错」或「两边结果不一样」，绝大多数落在**四类差异**上：**类型严格度**（Spark 更严格，隐式转换会直接报错）、**NULL 排序**（Hive 与 Spark 默认一致，但和 PostgreSQL/MySQL 不同）、**函数行为**（日期、字符串函数在边界上不同）、**写入行为**（动态分区覆盖、文件布局）。
> 这一节不是要你背差异表，而是建立一套**迁移检查清单**：拿到一份 Hive SQL 迁 Spark，按清单逐项过一遍。

---

## 一、心智模型：两种引擎的「性格」

| 维度 | Hive（3.x） | Spark SQL（3.5） |
| --- | --- | --- |
| 定位 | SQL on Hadoop，翻译成 MR/Tez | 内存计算引擎，Catalyst 优化器 |
| 类型严格度 | **宽松**（隐式转换多，很多情况返回 NULL 不报错） | **严格**（遵循 ANSI 模式，很多情况直接报错） |
| 执行模型 | 多阶段 MR/Tez DAG，落盘多 | DAG + 内存缓存，shuffle 为主 |
| 优化器 | Calcite（CBO，需统计信息） | Catalyst（规则优化 + CBO + AQE） |
| 默认模式 | 非 ANSI（`''` 不是 NULL，除零返回 NULL） | Spark 3.x 可开 ANSI（`spark.sql.ansi.enabled`） |
| 容错 | task 失败重试 | lineage 重算 + task 重试 |

> [!important] 核心差异的根源
> **Hive 的设计哲学是「尽量不报错」**（脏数据返回 NULL，任务继续跑）；**Spark 的设计哲学是「尽早暴露问题」**（类型不对直接抛异常）。
> 这导致：Hive 上的任务经常「静默出错」，Spark 上的任务经常「直接失败」。**两者都不是坏事，但迁移时必须有心理准备。**

---

## 二、核心知识点

### 2.1 类型与隐式转换：Spark 更严格

**差异一：`string` 与数值比较**

```sql
-- Hive：允许，把 string cast 成 double
select * from t where dt = 20261001;        -- 不报错，返回空

-- Spark 3.x（ANSI 关闭时）：多数情况也允许，但行为可能不同
-- Spark 3.x（ANSI 开启时）：可能报错
set spark.sql.ansi.enabled = true;
select * from t where dt = 20261001;        -- 可能抛 Cannot cast
```

**差异二：`int` 溢出**

```sql
-- Hive：溢出返回 NULL
select 2147483647 + 1;                      -- NULL

-- Spark（ANSI 关闭）：溢出返回 NULL
-- Spark（ANSI 开启）：抛 ArithmeticException
```

**差异三：除零**

```sql
-- Hive：返回 NULL
select 1 / 0;                               -- NULL

-- Spark（ANSI 关闭）：返回 NULL
-- Spark（ANSI 开启）：抛 DivisionByZero
```

**差异四：非法字符串转数值**

```sql
-- Hive：cast('abc' as int) → NULL（不报错）
-- Spark（ANSI 关闭）：NULL
-- Spark（ANSI 开启）：抛异常
```

> [!danger] 迁移时最容易翻车的地方
> 如果 Spark 集群开了 `spark.sql.ansi.enabled = true`（有些平台默认开），大量 Hive 上「能跑但结果是 NULL」的 SQL 会直接报错。
> **排查方法**：
> ```sql
> set spark.sql.ansi.enabled;   -- 看当前值
> ```
> 迁移前先关掉 ANSI（`set spark.sql.ansi.enabled = false;`）跑一遍，确认逻辑正确后再考虑开启。

### 2.2 函数差异对照表（高频）

| 功能 | Hive 3.x | Spark 3.5 | 差异说明 |
| --- | --- | --- | --- |
| 日期截断 | `trunc(d, 'MM')` 返回 **string** | `trunc(d, 'MM')` 返回 **date**；`date_trunc` 返回 timestamp | **返回类型不同，后续比较会出错** |
| 日期加月 | `add_months(d, n)` | `add_months(d, n)` | 一致 |
| 日期加减 interval | 不支持 / 部分支持 | `d + interval 1 day` | Spark 原生支持 |
| 周起点 | `next_day(d, 'MO')` | `date_trunc('WEEK', d)` 默认周一 | 语义一致，写法不同 |
| 字符串转日期 | `to_date(s)` | `to_date(s)` / `to_date(s, fmt)` | Spark 支持指定格式 |
| 时间戳格式化 | `date_format(ts, fmt)` | `date_format(ts, fmt)` | 一致（注意 `MM` vs `mm`） |
| 当前时间 | `current_timestamp()` / `now()` | `current_timestamp()` / `now()` | 一致，但时区依赖配置 |
| JSON 解析 | `get_json_object` | `get_json_object` / `from_json` | Spark 的 `from_json` 更强（可指定 schema） |
| 数组聚合 | `collect_list` / `collect_set` | 同名 | `collect_list` 对 NULL 的处理可能不同 |
| 字符串拼接 | `concat` / `concat_ws` | 同名 | 一致 |
| 正则 | `regexp_extract` / `regexp_replace` | 同名 | 一致（但 `regexp_extract` 无匹配返回空串） |
| 哈希 | `hash()` | `xxhash64()` / `hash()` | **算法不同，结果不同** |
| 随机数 | `rand()` | `rand()` | 一致 |
| 最大/最小对应值 | 需用 `max(struct(...))` 技巧 | `max_by(x, y)` / `min_by(x, y)` | Spark 有原生函数 |
| 分位数 | `percentile` / `percentile_approx` | 同名 | 一致 |
| 数组包含 | `array_contains` | `array_contains` | 一致 |
| 展开 map | `explode(map)` | `explode(map)` | 一致 |

> [!warning] `trunc` 返回类型不同是个大坑
> ```sql
> -- Hive：trunc 返回 string
> select trunc('2026-10-25', 'MM');        -- '2026-10-01'（string）
>
> -- Spark：trunc 返回 date
> select trunc('2026-10-25', 'MM');        -- 2026-10-01（date）
> ```
> 如果后续用 `concat(trunc(...), ' 00:00:00')`，Hive 能跑，Spark 会因为 date 不能隐式转 string 而报错或行为异常。
> **修复**：显式 `cast(trunc(...) as string)`。

> [!danger] `hash()` 算法不同
> Hive 的 `hash()` 和 Spark 的 `hash()` **算法不一样**，同一个值算出的结果不同。
> 如果用它做分桶、分片、去重键，迁移后数据会错乱。
> **修复**：改用 `md5()` 或 `xxhash64()`（Spark），或者在 SQL 里显式指定算法。

### 2.3 NULL 相关差异

| 场景 | Hive | Spark |
| --- | --- | --- |
| `''` 是否等于 NULL | **不等于**（空串是独立值） | **不等于** |
| `ORDER BY` NULL 位置 | 升序最前、降序最后 | 同 Hive |
| `GROUP BY` NULL | 归为一组 | 同 |
| `count(distinct)` 对 NULL | 忽略 | 同 |
| `nvl('', 'x')` | 返回 `''` | 返回 `''` |
| `concat` 遇 NULL | 返回 NULL | 返回 NULL |
| `collect_list` 对 NULL | Hive 3.x 保留 NULL | Spark 默认**保留**，可用 `spark.sql.legacy...` 调整 |

> [!note] 和 MySQL/Oracle 的差异更大
> 迁移到 Hive/Spark 时，`''` vs NULL 是最大的语义分歧（见 01-01）。Hive 和 Spark 在这点上是一致的，都是「空串不是 NULL」。

### 2.4 写入行为差异（最容易造成数据事故）

| 场景 | Hive | Spark |
| --- | --- | --- |
| `insert overwrite ... partition(dt)`（动态） | **删除所有分区**再写 | 默认（`static` 模式）也删所有分区；`dynamic` 模式只覆盖涉及的分区 |
| 小文件合并 | `hive.merge.*` 参数 | 无自动合并，需 `repartition` / `coalesce` |
| 写入文件数 | 由 reducer 数决定 | 由 shuffle 分区数决定 |
| 文件命名 | `part-00000` | `part-00000-xxxx.parquet` |
| 事务表 | 支持 ACID（ORC） | **不支持写 Hive ACID 表**（3.x 有限支持） |
| 分桶表写入 | 需 `hive.enforce.bucketing=true` | 写入时可能不按 Hive 分桶规则 |

> [!danger] Spark 写 Hive 动态分区的两种模式
> ```sql
> -- 模式1：static（默认）—— 覆盖所有分区
> set spark.sql.sources.partitionOverwriteMode = static;
>
> -- 模式2：dynamic —— 只覆盖本次写入涉及的分区
> set spark.sql.sources.partitionOverwriteMode = dynamic;
> ```
> **生产建议：开 `dynamic`**，否则一次误写会清空整表。
> 但要注意：`dynamic` 模式下如果 SELECT 结果为空，**不会删除任何分区**（而 static 模式会清空）——这又会导致「想清空某分区但清不掉」。

### 2.5 执行与优化差异

| 维度 | Hive | Spark |
| --- | --- | --- |
| 默认并行度 | reducer 数由数据量估算 | `spark.sql.shuffle.partitions`（默认 200） |
| Join 策略 | MapJoin（小表）、Common Join | BHJ / SMJ / SHJ / BNLJ |
| 广播阈值 | `hive.auto.convert.join.noconditionaltask.size`（默认 25MB） | `spark.sql.autoBroadcastJoinThreshold`（默认 10MB） |
| 倾斜处理 | `hive.optimize.skewjoin` | AQE 自动处理 |
| 中间结果 | 落盘（MR）/ 内存+落盘（Tez） | 内存优先，spill 到磁盘 |
| 统计信息 | `analyze table ... compute statistics` | `analyze table ... compute statistics`（Catalyst CBO 依赖） |

> [!tip] Spark 3.x 的 AQE 是「免费的性能提升」
> ```sql
> set spark.sql.adaptive.enabled = true;                        -- 3.2+ 默认开
> set spark.sql.adaptive.coalescePartitions.enabled = true;      -- 合并小分区
> set spark.sql.adaptive.skewJoin.enabled = true;                -- 自动处理 join 倾斜
> set spark.sql.adaptive.localShuffleReader.enabled = true;      -- 本地读 shuffle
> ```
> AQE 能在运行时根据实际数据量调整分区数、join 策略，**对迁移过来的 Hive SQL 特别友好**（因为很多 Hive SQL 的分区数设置是拍脑袋的）。

### 2.6 迁移检查清单（实操）

拿到一份 Hive SQL 迁 Spark，按这个顺序检查：

| # | 检查项 | 检查方法 | 常见问题 |
| --- | --- | --- | --- |
| 1 | ANSI 模式 | `set spark.sql.ansi.enabled` | 开启后会报类型错误 |
| 2 | 类型隐式转换 | 找 `where 数值列 = '字符串'` 这类写法 | 报错或结果不同 |
| 3 | `trunc` / `date_format` 返回类型 | 找日期函数的使用点 | 返回类型不同导致后续报错 |
| 4 | `hash()` 的使用 | 搜 `hash(` | 算法不同导致分片错乱 |
| 5 | 动态分区写入 | 找 `insert overwrite ... partition(动态列)` | 数据被清空 |
| 6 | `collect_list` 顺序 | 找 `collect_list` | 顺序可能不同 |
| 7 | 分桶表 | 找 `clustered by` | Spark 可能不识别 |
| 8 | UDF | 找 `create temporary function` | 需要重新注册 |
| 9 | 事务表 / ACID | 找 `transactional=true` | Spark 写不了 |
| 10 | 小文件 | 看输出文件数 | Spark 默认分区数可能不合适 |
| 11 | 时区 | 找 `current_date` / `unix_timestamp` | 集群配置不同导致差一天 |
| 12 | 排序稳定性 | 找 `row_number` 无唯一排序键 | 结果不确定 |

> [!success] 迁移的标准流程
> 1. **先关 ANSI 跑通**（保证不因类型报错中断）
> 2. **对比结果**：同一份输入，两边跑，逐表对比行数和关键指标（金额合计、去重数）
> 3. **定位差异**：按上面的清单逐项排查
> 4. **再开 ANSI**（如果需要），修掉所有类型问题
> 5. **压测性能**：调整分区数、广播阈值、AQE

---

## 三、动手实验

### 3.1 类型严格度对比

```sql
-- 在 Hive 和 Spark 分别执行，记录结果
select 1 / 0;                          -- Hive: NULL
select cast('abc' as int);             -- Hive: NULL
select 2147483647 + 1;                 -- Hive: NULL
select '2026-10-01' = 20261001;        -- Hive: false（cast 成 double 失败 → NULL → false）

-- Spark 开启 ANSI 后
set spark.sql.ansi.enabled = true;
select 1 / 0;                          -- 抛 DivisionByZero
set spark.sql.ansi.enabled = false;
```

### 3.2 `trunc` 返回类型对比

```sql
-- Hive
select trunc('2026-10-25', 'MM') as v, typeof(trunc('2026-10-25', 'MM')) as t;
-- v = '2026-10-01', t = 'string'

-- Spark
select trunc('2026-10-25', 'MM') as v, typeof(trunc('2026-10-25', 'MM')) as t;
-- v = 2026-10-01, t = 'date'

-- 验证差异带来的问题
select concat(trunc('2026-10-25','MM'), ' 00:00:00');
-- Hive: '2026-10-01 00:00:00'
-- Spark: 可能报错或返回 '2026-10-01 00:00:00'（取决于隐式转换）
-- 安全写法：concat(cast(trunc('2026-10-25','MM') as string), ' 00:00:00')
```

### 3.3 `hash()` 算法对比

```sql
select hash('abc'), hash(123), hash('2026-10-01');
-- Hive 和 Spark 的结果不同，请分别执行记录
```

### 3.4 动态分区写入对比（危险，用测试表）

```sql
-- 准备：一个有 3 个分区的表
-- Hive 执行：
insert overwrite table tmp.part_test partition(dt)
select id, dt from src where dt = '2026-10-01';
show partitions tmp.part_test;   -- 只剩 1 个分区

-- Spark 执行（dynamic 模式）：
set spark.sql.sources.partitionOverwriteMode = dynamic;
insert overwrite table tmp.part_test partition(dt)
select id, dt from src where dt = '2026-10-01';
show partitions tmp.part_test;   -- 其他分区保留
```

### 3.5 执行计划对比

```sql
-- 同一条 SQL，分别看执行计划
explain extended
select city, count(*) from tmp.order_lab group by city;

-- Hive：Stage 依赖、Map/Reduce 算子
-- Spark：Exchange / HashAggregate / FileScan
```

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| Hive 能跑，Spark 报类型错误 | ANSI 模式严格 | `set spark.sql.ansi.enabled` | 关 ANSI 或修正类型 |
| 两边结果行数不同 | 隐式转换行为不同 / NULL 处理不同 | 逐层对比中间结果 | 显式 cast，显式处理 NULL |
| 日期字段拼接报错 | `trunc` 返回类型不同 | `typeof()` 验证 | 显式 `cast(... as string)` |
| 分片/分桶数据错乱 | `hash()` 算法不同 | 对比 hash 值 | 改用 `md5` 或统一算法 |
| 迁移后历史数据被清空 | Spark 动态分区覆盖模式不同 | `show partitions` | 开 `dynamic` 模式；或改静态分区 |
| 迁移后小文件暴增 | Spark 默认分区数不合适 | `dfs -ls` 数文件 | 调 `shuffle.partitions`；`repartition` |
| UDF 找不到 | Spark 需要重新注册 | 报错 `Undefined function` | 重新 `create temporary function` |
| 时间差 8 小时 | 集群时区配置不同 | 对比 `current_timestamp()` | 统一 UTC 存储；显式时区转换 |
| 分桶表 join 没加速 | Spark 不识别 Hive 分桶 | `explain` 看 join 类型 | 改广播 join |
| 任务变慢 | 并行度/广播阈值不同 | 看 stage 数和 task 数 | 调参；开 AQE |

---

## 五、面试追问

> [!question] 1. Hive 和 Spark SQL 最大的区别是什么？
> **一句话答法**：执行引擎不同（Hive 是 SQL on Hadoop 翻译成 MR/Tez，Spark 是内存计算 + Catalyst 优化器），加上类型严格度不同（Hive 宽松返回 NULL，Spark 严格报错）。
> **追问陷阱**：「性能差多少」——Spark 通常快 3~10 倍（避免中间落盘），但取决于数据量和 SQL 写法，不是绝对的。

> [!question] 2. 同一份 SQL 在 Hive 能跑、Spark 报错，最可能是哪几类原因？
> **一句话答法**：① ANSI 模式下类型不匹配报错；② 日期函数返回类型不同（`trunc`）；③ UDF 未注册；④ 分桶表/事务表不支持。
> **追问陷阱**：「怎么快速定位」——先关 ANSI 跑一遍，如果通了就是类型问题；还不通就看报错信息里的函数名。

> [!question] 3. `hash()` 在两边结果一样吗？
> **一句话答法**：不一样，算法不同。用它做分片/分桶/去重键会导致迁移后数据错乱。
> **追问陷阱**：「替代方案」——`md5()`（两边一致），或 Spark 的 `xxhash64()`（更快但和 Hive 也不一致）。

> [!question] 4. Spark 写 Hive 动态分区有什么坑？
> **一句话答法**：`partitionOverwriteMode` 有 `static` 和 `dynamic` 两种。`static` 会覆盖所有分区（危险），`dynamic` 只覆盖涉及的分区。
> **追问陷阱**：「`dynamic` 模式下 SELECT 为空会怎样」——不删除任何分区，想清空分区要用 `static` 或先 `drop partition`。

> [!question] 5. 为什么 Spark 需要 `spark.sql.shuffle.partitions` 而 Hive 不用？
> **一句话答法**：Hive 的 reducer 数由数据量自动估算，Spark 默认固定 200 个分区，数据量大时分区太少会 OOM/慢，数据量小时分区太多会产生小文件。
> **追问陷阱**：「怎么定」——按 128~256MB/分区反推；或者开 AQE 让它自动调整。

> [!question] 6. 迁移时怎么保证数据一致？
> **一句话答法**：同一份输入两边跑，逐层对比中间结果的行数、金额合计、去重数；差异定位到具体算子后逐个修。
> **追问陷阱**：「只对比最终结果够吗」——不够，最终结果一致可能是巧合（两处错误抵消），必须对比中间层。

> [!question] 7. Spark 的 AQE 是什么？对迁移有什么帮助？
> **一句话答法**：Adaptive Query Execution，运行时根据实际 shuffle 数据量调整分区数、join 策略、处理倾斜。对迁移过来的 Hive SQL 特别友好，因为很多 Hive SQL 的分区数是拍脑袋设的。
> **追问陷阱**：「AQE 有什么代价」——需要额外收集统计信息，小任务上开销不明显但存在；另外 AQE 只在 shuffle 后生效，map 端问题救不了。

> [!question] 8. Hive 的 MapJoin 和 Spark 的 Broadcast Join 有什么区别？
> **一句话答法**：思路一样（小表广播到每个节点），但阈值和实现不同：Hive 默认 25MB，Spark 默认 10MB；Spark 的广播是真正的 broadcast variable，Hive 是分布式缓存。
> **追问陷阱**：「广播表太大会怎样」——OOM。要评估 executor 内存。

> [!question] 9. 两边对 NULL 的处理一致吗？
> **一句话答法**：Hive 和 Spark 在 `''` vs NULL、`ORDER BY` NULL 位置、`count(distinct)` 忽略 NULL 上基本一致；但和 MySQL/Oracle 差异很大（Oracle 里 `''` 就是 NULL）。
> **追问陷阱**：「`collect_list` 对 NULL 呢」——版本相关，Hive 3.x 保留，Spark 默认保留，但历史版本可能过滤，**不要依赖**。

> [!question] 10. Spark 为什么不支持 Hive 事务表？
> **一句话答法**：Hive ACID 依赖 ORC + 特定的 delta 文件机制，Spark 的写入模型（直接写 parquet/orc 文件）不兼容这套机制。
> **追问陷阱**：「那怎么做更新」——用 `merge into`（Spark 3.x 支持 Delta/Iceberg 表），或分区覆盖。

---

## 六、自测题

> [!question] 1.（单选）`select trunc('2026-10-25', 'MM')` 在 Hive 和 Spark 中返回类型分别是？
> A. string / string
> B. date / date
> C. string / date
> D. date / string

> [!question] 2.（单选）Spark 中 `spark.sql.sources.partitionOverwriteMode = static` 时，`insert overwrite table t partition(dt) select ... where dt='2026-10-01'` 会？
> A. 只覆盖 2026-10-01 分区
> B. 覆盖所有分区
> C. 报错
> D. 追加数据

> [!question] 3.（判断）Hive 的 `hash('abc')` 和 Spark 的 `hash('abc')` 返回值相同。

> [!question] 4.（场景设计）你有一份 200 行的 Hive SQL 要迁到 Spark，已知涉及日期函数、`hash()` 分片、动态分区写入、两个 Hive UDF。请给出完整的迁移步骤和每步的验证方法。

> [!question] 5.（场景设计）迁移后发现 Spark 任务比 Hive 慢 3 倍，且输出小文件从 50 个变成 2000 个。请分析可能原因并给出调优方案。

> [!success]- 答案与解析（点击展开）
> **1. C**
> Hive 的 `trunc` 返回 string，Spark 的 `trunc` 返回 date。这是迁移时的经典坑。
>
> **2. B**
> `static` 模式会先删除表的所有分区，再写入新数据。这是默认行为，也是迁移事故的高发点。
>
> **3. 错误**
> 两者算法不同，结果不同。用 `hash()` 做分片/分桶键时迁移会导致数据错乱。
>
> **4. 迁移步骤**
> | 步骤 | 操作 | 验证方法 |
> | --- | --- | --- |
> | ① 环境准备 | 确认 `spark.sql.ansi.enabled` 的值；先设为 false | `set spark.sql.ansi.enabled` |
> | ② UDF 迁移 | 把 Hive UDF jar 重新注册为 Spark UDF（`create temporary function ... as 'com.xxx.Udf' using jar '...'`） | 单独 `select my_udf('test')` 验证 |
> | ③ 日期函数修正 | 所有 `trunc` 结果显式 `cast(... as string)`；检查 `date_format` 的格式串 | 对比两边 `typeof()` 和值 |
> | ④ hash 替换 | 把 `hash(x)` 改成 `md5(x)` 或统一的分片算法 | 对比分片后各桶行数分布 |
> | ⑤ 动态分区 | 设 `partitionOverwriteMode=dynamic`；或改成静态分区逐个写 | 跑一次后 `show partitions` 对比 |
> | ⑥ 结果对比 | 同一份输入两边跑，逐层对比行数/金额/去重数 | 写对比 SQL，差异应为 0 |
> | ⑦ 性能调优 | 调 `shuffle.partitions`、开 AQE、调广播阈值 | 对比 stage 耗时 |
> | ⑧ 开 ANSI（可选） | 修完所有类型问题后再开 | 全量回归 |
>
> **关键提醒**：第 ⑥ 步「逐层对比」最重要。只对比最终结果会漏掉「两处错误抵消」的情况。
>
> **5. 可能原因与调优**
> **变慢的原因**
> ① **分区数不足**：Spark 默认 `shuffle.partitions=200`，如果数据量大（如 1TB），200 个分区每个 5GB，单 task 处理太慢甚至 spill。Hive 会根据数据量自动算 reducer 数。
> ② **广播阈值更低**：Spark 默认 10MB，Hive 默认 25MB。原来能 MapJoin 的小表在 Spark 上变成了 SMJ（多一次 shuffle）。
> ③ **没有 AQE**：Hive 的 Tez 有动态优化，Spark 3.0 之前没有 AQE。
> ④ **UDF 效率**：Hive UDF 在 Spark 里通过兼容层调用，可能有额外开销。
>
> **小文件暴增的原因**
> 输出文件数 = shuffle 分区数（200）× 分区数。如果写了 10 个 dt 分区，就是 2000 个文件。Hive 的 reducer 数是按数据量算的，可能只有 5 个。
>
> **调优方案**
> ```sql
> -- 1. 分区数按数据量重算
> set spark.sql.shuffle.partitions = 800;   -- 例：200GB / 256MB ≈ 800
>
> -- 2. 开 AQE 自动合并
> set spark.sql.adaptive.enabled = true;
> set spark.sql.adaptive.coalescePartitions.enabled = true;
> set spark.sql.adaptive.advisoryPartitionSizeInBytes = 256MB;
>
> -- 3. 提高广播阈值
> set spark.sql.autoBroadcastJoinThreshold = 104857600;   -- 100MB
>
> -- 4. 写入前重分区（控制文件数）
> insert overwrite table t partition(dt)
> select /*+ REPARTITION(50) */ ... ;
> -- 或
> select ... distribute by cast(rand() * 50 as int);
>
> -- 5. 倾斜处理
> set spark.sql.adaptive.skewJoin.enabled = true;
> ```
> **验证**：调完后对比 ① 总耗时；② 每个 stage 的 task 时长分布（是否均匀）；③ 输出文件数和平均文件大小。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 上游 | [01-01 数据类型、NULL、空字符串、隐式转换坑](01-01%20%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B%E3%80%81NULL%E3%80%81%E7%A9%BA%E5%AD%97%E7%AC%A6%E4%B8%B2%E3%80%81%E9%9A%90%E5%BC%8F%E8%BD%AC%E6%8D%A2%E5%9D%91.md) | 类型严格度差异的根源 |
| 上游 | [01-02 内置函数：日期、NVL、时区、币种转换](01-02%20%E5%86%85%E7%BD%AE%E5%87%BD%E6%95%B0%EF%BC%9A%E6%97%A5%E6%9C%9F%E3%80%81NVL%E3%80%81%E6%97%B6%E5%8C%BA%E3%80%81%E5%B8%81%E7%A7%8D%E8%BD%AC%E6%8D%A2.md) | 函数差异对照 |
| 上游 | [01-08 分区表设计与分区裁剪](01-08%20%E5%88%86%E5%8C%BA%E8%A1%A8%E8%AE%BE%E8%AE%A1%E4%B8%8E%E5%88%86%E5%8C%BA%E8%A3%81%E5%89%AA.md) | 动态分区写入行为差异 |
| 直接下游 | [01-13 小文件与写入参数](01-13%20%E5%B0%8F%E6%96%87%E4%BB%B6%E4%B8%8E%E5%86%99%E5%85%A5%E5%8F%82%E6%95%B0.md) | Spark 的文件数控制 |
| 延伸 | [00_模块3导航](../03%20%E6%A8%A1%E5%9D%973%20Spark%E8%B0%83%E4%BC%98%E3%80%81%E6%95%B0%E6%8D%AE%E8%B4%A8%E9%87%8F%E8%A1%80%E7%BC%98/00_%E6%A8%A1%E5%9D%973%E5%AF%BC%E8%88%AA.md) | Spark 调优的完整体系 |

> [!tip] 学习建议
> 这一节不需要背全表，**记住「四大差异」+「迁移检查清单」就够**：类型严格度、`trunc` 返回类型、`hash` 算法、动态分区覆盖。
> 真正迁移时，把 2.6 的 12 项清单当 checklist 过一遍，比临场回忆靠谱得多。
