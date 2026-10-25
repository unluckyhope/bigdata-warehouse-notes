---
title: 01-09 排序：order、sort、distribute、cluster by
module: Hive & Spark SQL
difficulty: ⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - Hive
  - SparkSQL
  - 排序
  - distribute by
---

# 01-09 排序：order、sort、distribute、cluster by

> [!abstract] 一句话结论
> Hive 的四种排序不是「写法不同」，而是**执行代价差几个数量级**：`ORDER BY` 是全局排序（**强制 1 个 reducer**，大数据量必挂）、`SORT BY` 是单 reducer 内排序、`DISTRIBUTE BY` 是控制分区（不排序）、`CLUSTER BY` 是两者的合体（分区+排序，但只能升序）。
> 生产上 90% 的场景**不需要全局有序**，需要的是「每个分区内部有序」+「控制输出文件数」——这正是 `DISTRIBUTE BY + SORT BY` 的用途。

---

## 一、心智模型：MapReduce 里的三种「有序」

```
输入数据 → Map → Shuffle（按 key 分区 + 排序）→ Reduce → 输出

DISTRIBUTE BY  →  决定"哪条数据去哪个 reducer"（= 分区函数）
SORT BY        →  决定"每个 reducer 内部怎么排"（= 局部有序）
ORDER BY       →  决定"全局怎么排"（= 强制 1 个 reducer，全局有序）
CLUSTER BY     →  DISTRIBUTE BY + SORT BY（同一列，只能升序）
```

**一张图看懂结果差异**（假设 2 个 reducer）：

| 写法 | Reducer 1 输出 | Reducer 2 输出 | 全局是否有序 |
| --- | --- | --- | --- |
| `ORDER BY x` | 1,2,3,4,5 | （无，只有 1 个 reducer） | ✅ 全局有序 |
| `SORT BY x` | 1,3,5 | 2,4,6 | ❌ 各自有序，合并后无序 |
| `DISTRIBUTE BY x` | 无序 | 无序 | ❌ 只是分到一起 |
| `DISTRIBUTE BY x SORT BY x` | 1,2,3 | 4,5,6 | ❌ 各自有序（且同一 x 在一起） |
| `CLUSTER BY x` | 1,2,3 | 4,5,6 | ❌ 同上，但等价于 `distribute by x sort by x` |

> [!important] 关键结论
> **只有 `ORDER BY` 能保证全局有序，代价是强制单 reducer。**
> 其他三种都只是「局部有序」，但它们能并行，所以在大数据量下是唯一可行的选择。

---

## 二、核心知识点

### 2.1 四种排序的完整对比

| 维度 | `ORDER BY` | `SORT BY` | `DISTRIBUTE BY` | `CLUSTER BY` |
| --- | --- | --- | --- | --- |
| 作用范围 | **全局** | 单 reducer 内 | 决定分区 | 分区 + 分区内排序 |
| reducer 数 | **强制 1** | 多个 | 多个 | 多个 |
| 是否有序 | ✅ 全局有序 | ⚠️ 局部有序 | ❌ 不排序 | ⚠️ 局部有序 |
| 能否多列 | ✅ | ✅ | ✅ | ❌ 只能一列 |
| 能否指定升降序 | ✅ `asc/desc` | ✅ | — | ❌ 只能升序 |
| 大数据量可用 | ❌ | ✅ | ✅ | ✅ |
| 典型用途 | 小结果集最终排序 | 分区内排序 | 控制输出文件数 | 简单分区排序 |

### 2.2 `ORDER BY`：为什么是「危险操作」

```sql
select * from dwd.order order by amount desc limit 100;
```

**执行过程**：所有数据 shuffle 到**一个 reducer**，在那里做全排序，然后取前 100。

**问题**：

| 数据量 | 结果 |
| --- | --- |
| 1 万行 | 秒级 |
| 100 万行 | 几十秒 |
| 1 亿行 | 单 reducer 内存爆 / 跑几小时 |
| 10 亿行 | 基本不可能完成 |

> [!danger] `ORDER BY` + `LIMIT` 并不会变快
> 很多人以为 `order by x limit 10` 会「只排前 10 个」，实际上**还是要对所有数据排序**（除非引擎做了 TopN 优化，Hive/Spark 的 TopN 优化只在一定条件下生效）。
> 真正的 TopN 应该用窗口函数或 `DISTRIBUTE BY + SORT BY + LIMIT` 的组合（见 2.5）。

**什么时候可以放心用 `ORDER BY`**：

- 结果集已经很小（聚合后的汇总表，几千行）
- 需要输出给下游做严格有序的文件（如某些对账文件）
- 配合 `limit` 且数据量不大

### 2.3 `SORT BY`：单 reducer 内排序

```sql
-- 每个 reducer 内部按 amount 降序，但 reducer 之间无序
select * from dwd.order sort by amount desc;
```

**注意**：`SORT BY` 不保证「相同 key 的数据到同一个 reducer」，所以它**不能**用来做「分组内排序」。

```sql
-- ❌ 错误认知：以为 sort by user_id 能让同一用户的数据在一起
select * from dwd.order sort by user_id;
-- 实际上 user_id=101 的数据可能分散在多个 reducer
```

> [!warning] `SORT BY` 单独用几乎没有意义
> 它只保证「每个输出文件内部有序」，但文件之间的顺序和内容都是随机的。
> **实际用途**：配合 `DISTRIBUTE BY` 使用（见 2.4）。

### 2.4 `DISTRIBUTE BY` + `SORT BY`：生产标准组合

```sql
-- 按 user_id 分区（同一用户的数据进同一个 reducer），分区内按 dt 排序
select user_id, dt, amount
from dwd.order
distribute by user_id
sort by user_id, dt desc;
```

**这个组合解决三个问题**：

1. **同一 key 的数据聚在一起** → 后续可以按 key 做有序处理
2. **每个 reducer 内部有序** → 输出文件内部有序
3. **控制输出文件数** → 文件数 ≈ reducer 数

**经典用法一：控制小文件**

```sql
-- 想输出 10 个文件（每个 256MB），而不是默认的 200 个
insert overwrite table dwd.order partition (dt = '2026-10-01')
select order_id, amount, dt
from ods.order
where dt = '2026-10-01'
distribute by cast(rand() * 10 as int);   -- 随机分成 10 份
```

> [!tip] 用 `distribute by` 控制文件数的技巧
> `distribute by cast(rand() * N as int)` 会把数据随机分到 N 个 reducer → 输出 N 个文件。
> 但要注意：**如果表有分区，`distribute by` 要放在分区列之后考虑**——实际上写入分区表时，引擎会按分区列自动分发，`distribute by` 控制的是「每个分区内的文件数」（Hive 行为，Spark 略有不同）。

**经典用法二：分组内取 TopN（不用窗口函数）**

```sql
-- 每个品类取金额最大的 3 笔订单（Hive 写法）
select category, order_id, amount
from (
    select category, order_id, amount
    from dwd.order
    distribute by category
    sort by category, amount desc
) t
-- 这里需要配合 limit 或 row_number，单靠 sort by 做不到
```

> [!warning] `DISTRIBUTE BY + SORT BY` 不能直接做 TopN
> 因为它没有「组内行号」的概念。**TopN 还是要用 `row_number`**（01-06）。
> `DISTRIBUTE BY + SORT BY` 的价值在于「让数据有序地落盘」，为后续处理（如 `map join`、有序 merge）做准备。

**经典用法三：配合 `CLUSTER BY` 做等值查询优化**

```sql
-- 如果数据按 user_id 有序落盘，后续按 user_id 查询可以更快（配合索引/分桶）
insert overwrite table dwd.order_sorted partition (dt='2026-10-01')
select order_id, user_id, amount, dt
from ods.order
where dt = '2026-10-01'
distribute by user_id
sort by user_id, order_id;
```

### 2.5 `CLUSTER BY`：简化的合体

```sql
-- 等价于 distribute by x sort by x
select * from dwd.order cluster by user_id;
```

**限制**：
- 只能按**一列**（或一个表达式）分区和排序
- **只能升序**，不能 `desc`

**什么时候用**：分区键和排序键恰好是同一列，且要升序。实际生产中不多，因为通常需要「按 A 分区、按 B 排序」。

### 2.6 TopN 的正确姿势

**场景**：10 亿行数据，取金额最大的 100 笔。

**方案一：窗口函数（推荐，可控）**

```sql
select * from (
    select order_id, amount,
           row_number() over (order by amount desc) as rn
    from dwd.order
) t where rn <= 100;
```

> [!warning] 这个写法在 10 亿行上仍然会全排序
> `row_number() over (order by amount desc)` 不带 `partition by`，等价于全局排序，代价和 `ORDER BY` 一样。
> **优化**：先在各分区内取 TopN，再全局取 TopN。

```sql
-- ✅ 两阶段 TopN
select * from (
    select order_id, amount,
           row_number() over (order by amount desc) as rn
    from (
        -- 阶段1：每个分区（如按 dt）内取 Top 1000
        select order_id, amount, dt,
               row_number() over (partition by dt order by amount desc) as rn_local
        from dwd.order
    ) t1
    where rn_local <= 1000
) t2
where rn <= 100;
```

**方案二：`DISTRIBUTE BY + SORT BY + LIMIT`（Hive 特化）**

```sql
-- 先分区内排序取 TopN，再合并
select * from (
    select order_id, amount
    from dwd.order
    distribute by cast(rand() * 10 as int)    -- 打散到 10 个 reducer
    sort by amount desc
    limit 100                                  -- ⚠️ 每个 reducer 取 100
) t
order by amount desc
limit 100;
```

> [!danger] `sort by ... limit` 的语义陷阱
> `limit` 在 `sort by` 之后，作用于**每个 reducer**，不是全局。
> 上面例子里，10 个 reducer 各取 100 条 → 中间结果 1000 条 → 再全局排序取 100。
> 这样确实减少了数据量，但**如果某个 reducer 里恰好没有真正的 Top100，结果就错了**（随机分区导致分布不均）。
> **正确做法**：`distribute by` 用有业务含义的键（如 `dt`），或者用两阶段窗口函数。

### 2.7 排序相关的性能参数

```sql
-- Hive
set hive.exec.reducers.bytes.per.reducer = 256000000;   -- 每个 reducer 处理的数据量
set hive.exec.reducers.max = 999;
set hive.merge.mapfiles = true;                          -- 合并小文件

-- Spark
set spark.sql.shuffle.partitions = 200;                  -- 默认 200，按数据量调
set spark.sql.adaptive.enabled = true;                   -- AQE 自动合并小分区
set spark.sql.adaptive.coalescePartitions.enabled = true;
```

> [!tip] reducer/分区数怎么定
> 经验公式：`分区数 = 数据量 / 每个分区的目标大小`，目标大小 128~256MB。
> 例：100GB 数据 → 100GB / 256MB ≈ 400 个分区。
> 分区太少 → 单 task 处理太慢、OOM；分区太多 → 调度开销大、小文件多。

### 2.8 排序与 NULL

| 引擎 | 升序时 NULL 位置 | 降序时 NULL 位置 | 控制方式 |
| --- | --- | --- | --- |
| Hive | 最前（最小） | 最后 | `NULLS FIRST/LAST`（Hive 2.1+ 支持） |
| Spark | 最前（最小） | 最后 | `NULLS FIRST/LAST` |
| MySQL | 最前 | 最后 | 不支持 `NULLS` 语法，用 `ISNULL()` |
| PostgreSQL | **最后**（默认 NULL 最大） | 最前 | 支持 `NULLS` |

```sql
-- 显式控制（跨引擎可移植）
select * from t order by amount desc nulls last;
```

> [!warning] 跨引擎迁移时排序结果不同
> PostgreSQL 和 Hive 对 NULL 的默认排序相反。如果下游依赖「第一行是最大的」这种假设，迁移后会出错。
> **规范：所有排序显式写 `NULLS FIRST/LAST`。**

---

## 三、动手实验

### 3.1 建表造数

```sql
drop table if exists tmp.sort_lab;
create table tmp.sort_lab (id int, category string, amount decimal(10,2));
insert into tmp.sort_lab values
(1,'A',100),(2,'A',300),(3,'A',200),
(4,'B',50),(5,'B',150),(6,'B',80),
(7,'C',900),(8,'C',700),(9,'C',800),
(10,'A',null);
```

### 3.2 实验一：四种排序的结果对比

```sql
-- A：ORDER BY（全局有序）
select id, category, amount from tmp.sort_lab order by amount desc;

-- B：SORT BY（每个 reducer 内有序，但这里 reducer 数可能为 1，看不出差异）
-- 强制 2 个 reducer
set mapreduce.job.reduces = 2;
select id, category, amount from tmp.sort_lab sort by amount desc;

-- C：DISTRIBUTE BY（只分区不排序）
select id, category, amount from tmp.sort_lab distribute by category;

-- D：DISTRIBUTE BY + SORT BY（分区内有序）
select id, category, amount from tmp.sort_lab
distribute by category sort by category, amount desc;

-- E：CLUSTER BY
select id, category, amount from tmp.sort_lab cluster by category;
```

**观察点**：
- A 的输出是一个文件，严格降序
- B 的输出是 2 个文件，每个文件内部降序，但文件 1 的最小值可能小于文件 2 的最大值
- C 的输出中同一 category 在一起，但内部无序
- D 的输出中同一 category 在一起且按 amount 降序

### 3.3 实验二：ORDER BY 的单 reducer 瓶颈

```sql
-- 造 100 万行
drop table if exists tmp.sort_big;
create table tmp.sort_big as
select a.id * 1000 + b.id as id, a.category, a.amount
from tmp.sort_lab a cross join (select id from tmp.sort_lab) b
cross join (select id from tmp.sort_lab) c;

select count(*) from tmp.sort_big;   -- 约 1000 行 * 10 = 10000，再放大

-- 对比执行时间
-- A：ORDER BY（1 个 reducer）
select * from tmp.sort_big order by amount desc limit 10;

-- B：DISTRIBUTE BY + SORT BY（多 reducer）
select * from (
    select * from tmp.sort_big distribute by id sort by amount desc
) t limit 10;
```

**看什么**：A 的 `explain` 里 reducer 数是 1；B 的 reducer 数等于分区数。

### 3.4 实验三：用 `distribute by` 控制文件数

```sql
-- 默认写入（文件数 = reducer 数，通常是 200 或数据量决定）
insert overwrite table tmp.sort_lab_out partition (dt='2026-10-01')
select id, category, amount from tmp.sort_big distribute by cast(rand() * 5 as int);

-- 看文件数
dfs -ls /user/hive/warehouse/tmp.db/sort_lab_out/dt=2026-10-01;
-- 应该约 5 个文件（每个 reducer 一个）
```

### 3.5 实验四：NULL 的排序位置

```sql
select id, amount from tmp.sort_lab order by amount asc;
-- Hive：NULL 在最前（id=10 排第一）

select id, amount from tmp.sort_lab order by amount desc;
-- Hive：NULL 在最后

-- 显式控制
select id, amount from tmp.sort_lab order by amount desc nulls last;
select id, amount from tmp.sort_lab order by amount asc nulls last;
```

### 3.6 实验五：TopN 的两阶段写法

```sql
-- 每个 category 取 Top2，再全局取 Top3
select * from (
    select id, category, amount,
           row_number() over (order by amount desc) as rn
    from (
        select id, category, amount,
               row_number() over (partition by category order by amount desc) as rn_local
        from tmp.sort_lab
        where amount is not null
    ) t1 where rn_local <= 2
) t2 where rn <= 3;
```

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| 任务只有一个 reducer，跑几小时 | 用了 `ORDER BY` | `explain` 看 reducer 数 | 改 `DISTRIBUTE BY + SORT BY`，或两阶段 TopN |
| 输出文件数 = 200，全是小文件 | 默认 shuffle 分区数 | `dfs -ls` 看文件数 | `distribute by` 控制；开小文件合并 |
| 同一用户的数据分散在多个文件 | 只用了 `SORT BY` | 检查是否漏了 `DISTRIBUTE BY` | 加 `distribute by user_id` |
| `cluster by` 报错不能 desc | `CLUSTER BY` 只能升序 | 报错信息 | 改 `distribute by + sort by` |
| TopN 结果不全 | `sort by ... limit` 是每个 reducer 的 limit | 检查中间结果行数 | 用两阶段窗口函数 |
| 排序结果与 MySQL 不一致 | NULL 默认位置不同 | 对比两端结果 | 显式 `NULLS FIRST/LAST` |
| 排序后数据量暴涨 | 排序触发 shuffle，中间结果膨胀 | 看 shuffle write size | 先过滤/聚合再排序 |
| `ORDER BY` OOM | 单 reducer 内存不足 | 看 executor 内存和 GC | 改用分布式排序 |
| `distribute by` 后数据倾斜 | 分区键分布不均 | 看各 reducer 的输入量 | 加盐或换分区键 |
| 有序落盘后查询没变快 | 没配合分桶/索引，顺序对查询无用 | — | 顺序只在特定场景有用（如有序 merge join） |

---

## 五、面试追问

> [!question] 1. `ORDER BY`、`SORT BY`、`DISTRIBUTE BY`、`CLUSTER BY` 的区别？
> **一句话答法**：`ORDER BY` 全局有序（强制 1 个 reducer）；`SORT BY` 单 reducer 内有序；`DISTRIBUTE BY` 只控制分区不排序；`CLUSTER BY` 是 `DISTRIBUTE BY + SORT BY`（同列、只能升序）。
> **追问陷阱**：「生产上用哪个最多」——`DISTRIBUTE BY + SORT BY`，因为既能并行又能控制输出。

> [!question] 2. 为什么 `ORDER BY` 在大数据量下不可用？
> **一句话答法**：它强制所有数据进一个 reducer 做全排序，单点瓶颈，数据量超过单机内存就 OOM。
> **追问陷阱**：「加 `limit` 会好点吗」——不一定，除非引擎做了 TopN 优化，否则还是要全排序。

> [!question] 3. 怎么实现全局有序又不爆内存？
> **一句话答法**：两阶段——先按范围分区（`range partition`）让各分区有序且范围不重叠，再各自排序；或者先分区内 TopN，再全局合并。
> **追问陷阱**：「Hive 有 range partition 吗」——有 `distribute by` + 自定义分区器，或 Spark 的 `rangePartitioner`（`sortWithinPartitions` + 全局采样）。

> [!question] 4. `SORT BY` 单独用有意义吗？
> **一句话答法**：几乎没有。它只保证每个输出文件内部有序，文件之间无序，也无法保证同一 key 在一起。
> **追问陷阱**：「那什么时候用」——配合 `DISTRIBUTE BY`，或者在只需要「输出文件内部有序」的场景（如给下游做流式读取）。

> [!question] 5. `CLUSTER BY` 和 `DISTRIBUTE BY + SORT BY` 什么关系？
> **一句话答法**：`CLUSTER BY x` 等价于 `DISTRIBUTE BY x SORT BY x`，但只能一列、只能升序。
> **追问陷阱**：「为什么要有 CLUSTER BY」——语法糖，简化写法；实际生产因为常需要多列/降序，用得少。

> [!question] 6. 怎么控制 Hive 输出文件数？
> **一句话答法**：三种方式——① `distribute by cast(rand()*N as int)` 控制 reducer 数；② 设置 `hive.exec.reducers.bytes.per.reducer`；③ 开小文件合并（`hive.merge.*`）。
> **追问陷阱**：「文件数应该多少」——按 128~256MB/文件反推。

> [!question] 7. `sort by ... limit` 的 limit 作用范围是什么？
> **一句话答法**：作用于**每个 reducer**，不是全局。所以多个 reducer 各取 N 条，结果可能不是真正的全局 TopN。
> **追问陷阱**：「那怎么写 TopN」——用 `row_number()` 窗口函数，或两阶段（分区内 TopN + 全局 TopN）。

> [!question] 8. 排序时 NULL 排在哪？
> **一句话答法**：Hive/Spark 默认升序 NULL 在前、降序 NULL 在后；PostgreSQL 相反。用 `NULLS FIRST/LAST` 显式控制。
> **追问陷阱**：「MySQL 支持 NULLS 语法吗」——不支持，要用 `ORDER BY ISNULL(col), col`。

> [!question] 9. 排序和分区的区别？
> **一句话答法**：分区决定「数据去哪个 reducer」，排序决定「reducer 内怎么排」。分区是前提，排序是结果。
> **追问陷阱**：「只分区不排序行吗」——行，`DISTRIBUTE BY` 就是。常用于控制文件数。

> [!question] 10. 为什么 `DISTRIBUTE BY` 能减少小文件？
> **一句话答法**：输出文件数 ≈ reducer 数，而 reducer 数由 `DISTRIBUTE BY` 的分区数决定。指定少量分区就得到少量文件。
> **追问陷阱**：「文件数少了会不会单文件太大」——会，所以要按数据量算：`分区数 = 总数据量 / 256MB`。

> [!question] 11. Spark 里对应 Hive 四种排序的是什么？
> **一句话答法**：Spark SQL 支持 `ORDER BY`（全局，用 `RangePartitioning` 实现，比 Hive 高效）、`SORT BY`、`DISTRIBUTE BY`、`CLUSTER BY`（语法兼容），底层是 `sort` / `repartition` / `sortWithinPartitions`。
> **追问陷阱**：「Spark 的 `ORDER BY` 也是单 reducer 吗」——不是，Spark 用范围分区做分布式全局排序，比 Hive 高效得多，但代价仍是全量 shuffle。

> [!question] 12. 什么场景需要「有序落盘」？
> **一句话答法**：① 下游用有序 merge join；② 生成对账文件要求严格有序；③ 配合分桶/索引做范围查询加速；④ 时序数据按时间有序便于增量读取。
> **追问陷阱**：「普通报表查询需要吗」——不需要。查询结果的顺序由最终 `ORDER BY` 决定，中间数据有序没有意义，反而浪费资源。

---

## 六、自测题

> [!question] 1.（单选）哪个子句会强制使用 1 个 reducer？
> A. `SORT BY`
> B. `DISTRIBUTE BY`
> C. `ORDER BY`
> D. `CLUSTER BY`

> [!question] 2.（单选）`SELECT * FROM t CLUSTER BY user_id` 等价于？
> A. `ORDER BY user_id`
> B. `DISTRIBUTE BY user_id SORT BY user_id`
> C. `SORT BY user_id`
> D. `DISTRIBUTE BY user_id`

> [!question] 3.（判断）`SELECT * FROM t SORT BY amount DESC LIMIT 10` 一定返回全局金额最大的 10 条。

> [!question] 4.（场景设计）一张订单表 500GB，要按 `province` 分组输出，每个省份一个文件，且文件内部按 `amount` 降序。请写出 SQL，并说明文件数会是多少、如何验证。

> [!question] 5.（场景设计）你需要从 10 亿行日志中取「每个 `event_type` 金额最大的 5 条」，同时最终结果按金额全局降序输出前 50 条。现有 SQL 是 `select * from log order by amount desc limit 50`，跑了 3 小时还没结束。请分析问题并给出优化方案。

> [!success]- 答案与解析（点击展开）
> **1. C**
> `ORDER BY` 需要全局有序，必须把所有数据集中到一个 reducer。其他三种都可以多 reducer 并行。
>
> **2. B**
> `CLUSTER BY x` 等价于 `DISTRIBUTE BY x SORT BY x`（升序，单列）。
>
> **3. 错误**
> `SORT BY` 只在每个 reducer 内部排序，`LIMIT 10` 是每个 reducer 取 10 条。如果有 200 个 reducer，会得到 2000 条（每个 reducer 的前 10），**不是全局 Top10**。
>
> **4. 参考 SQL**
> ```sql
> insert overwrite table dwd.order_by_prov partition (dt = '2026-10-01')
> select order_id, province, amount, dt
> from ods.order
> where dt = '2026-10-01'
> distribute by province
> sort by province, amount desc;
> ```
> **文件数**：约等于 `province` 的 distinct 值个数（每个 reducer 输出 1 个文件）。如果全国 34 个省级行政区，就是 34 个文件。
> **验证方法**：
> ① `dfs -ls /path/to/partition \| wc -l` 看文件数是否约等于省份数；
> ② 抽一个文件 `dfs -cat` 看内部是否按 amount 降序；
> ③ `explain` 看 reducer 数是否等于 province 基数。
> **注意**：如果某个省的订单量特别大（如广东），单文件可能几百 GB，需要考虑按省 + 二次打散。
>
> **5. 问题分析**
> `order by amount desc limit 50` 有两个问题：
> ① **`ORDER BY` 强制单 reducer**，10 亿行全部进一个 reducer，内存和耗时都不可接受；
> ② **`LIMIT` 不一定能减少排序量**，引擎通常还是要排完才能确定前 50。
>
> **优化方案（两阶段）**
> ```sql
> -- 阶段1：每个 event_type 内取 Top5（并行）
> -- 阶段2：从这些候选中全局取 Top50
> select * from (
>     select order_id, event_type, amount,
>            row_number() over (order by amount desc) as rn
>     from (
>         select order_id, event_type, amount,
>                row_number() over (partition by event_type order by amount desc) as rn_local
>         from dwd.log
>         where dt between '2026-10-01' and '2026-10-31'
>     ) t1
>     where rn_local <= 5
> ) t2
> where rn <= 50;
> ```
> **为什么快**：
> - 阶段1 的窗口 `partition by event_type` 可以并行（event_type 基数小，每个分区独立排序）
> - 阶段1 输出只有 `event_type 数 × 5` 行（假设 100 种事件 → 500 行）
> - 阶段2 在 500 行上做全局排序，几乎瞬间完成
> - 总耗时从「10 亿行全排序」降到「10 亿行分区内 TopN」+「500 行全排序」
>
> **额外优化**：如果 `dt` 分区很多，阶段1 里 `row_number` 会跨分区排序（因为没按 dt 分区），可以考虑 `partition by event_type, dt` 先取每天 Top5，再按 event_type 取 Top5，最后全局取 50（三层）。层数取决于数据分布，**先用两层试，慢了再加一层**。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 上游 | [[01-03 SQL 执行顺序与常见误区]] | `ORDER BY` 在最后执行 |
| 上游 | [[01-06 窗口函数与帧边界]] | 窗口内部也依赖排序 |
| 上游 | [[01-08 分区表设计与分区裁剪]] | 分区决定数据分布，排序决定文件内顺序 |
| 直接下游 | [[01-13 小文件与写入参数]] | `distribute by` 是控制文件数的核心手段 |
| 延伸 | [[00_模块3导航]] | shuffle 与排序的深度调优 |

> [!tip] 学习建议
> 这一节内容不多，但**「`ORDER BY` 是单 reducer」这一条必须条件反射式地记住**。
> 每次写 `ORDER BY` 前先问自己：数据量多大？如果超过 100 万行，就考虑改成两阶段 TopN 或 `DISTRIBUTE BY + SORT BY`。
