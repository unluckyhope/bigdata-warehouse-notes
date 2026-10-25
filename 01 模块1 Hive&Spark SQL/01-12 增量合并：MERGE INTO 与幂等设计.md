---
title: 01-12 增量合并：MERGE INTO 与幂等设计
module: 模块1 Hive & Spark SQL
difficulty: ⭐⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - Hive
  - SparkSQL
  - MERGE
  - 幂等
  - 增量
---

# 01-12 增量合并：MERGE INTO 与幂等设计

> [!abstract] 一句话结论
> 增量合并要解决的核心问题是：**「新数据来了，怎么和历史数据融合」**——新增插入、变更更新、删除标记。
> `MERGE INTO` 是标准答案，但它不是银弹：**Hive 3.x 只支持 ACID 表（ORC + 事务）**，Spark 需要 Delta/Iceberg/Hudi 才原生支持。没有这些条件时，**「分区覆盖 + 全量重算」反而是最稳的方案**。
> 这一节的真正重点是**幂等设计**：任务重跑 N 次，结果必须一致。

---

## 一、心智模型：增量合并的三种范式

```
范式一：全量覆盖（最简单，最稳）
  每天把「历史 + 增量」全量重算，覆盖整个分区
  优点：绝对幂等，逻辑简单
  缺点：数据量大时算不动

范式二：分区覆盖（最常用）
  只重算「受影响的那些分区」，覆盖写入
  优点：幂等，成本可控
  缺点：要求「增量只影响有限分区」

范式三：行级合并（MERGE INTO）
  按主键匹配，逐行 update/insert/delete
  优点：只动变化的行，最省
  缺点：依赖事务表（ACID/Delta/Iceberg），实现复杂
```

> [!important] 选型判断
> **90% 的数仓场景用「范式二：分区覆盖」就够了**，不需要 `MERGE INTO`。
> 只有当「增量数据会修改很久以前的分区」（比如订单状态在 30 天后才变成「已完成」），才需要行级合并。

---

## 二、核心知识点

### 2.1 幂等：重跑不产生副作用

**定义**：同一个任务用同一份输入跑 N 次，结果完全一致。

**为什么重要**：

- 任务失败重试是常态（网络抖动、资源不足、上游延迟）
- 数据修复时要「重跑历史某天」
- 如果任务不幂等，重跑会产生重复数据或错误结果

**三种写入方式的幂等性**：

| 写入方式 | 幂等？ | 说明 |
| --- | --- | --- |
| `insert into` | ❌ | 每次追加，重跑就重复 |
| `insert overwrite`（静态分区） | ✅ | 覆盖指定分区，重跑结果一致 |
| `insert overwrite`（动态分区） | ⚠️ | Hive 上会清空所有分区（危险但幂等）；Spark 上只覆盖涉及分区（幂等但可能漏删） |
| `merge into` | ✅ | 按主键合并，天然幂等 |

> [!danger] `insert into` 是最常见的「不幂等」源头
> ```sql
> -- ❌ 重跑一次，数据翻倍
> insert into dwd.order_daily
> select * from ods.order where dt = '${bizdate}';
>
> -- ✅ 改成 overwrite
> insert overwrite table dwd.order_daily partition (dt = '${bizdate}')
> select * from ods.order where dt = '${bizdate}';
> ```

### 2.2 `MERGE INTO` 标准语法

```sql
MERGE INTO target_table AS t
USING source_table AS s
ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET t.col1 = s.col1, t.col2 = s.col2
WHEN NOT MATCHED THEN INSERT (id, col1, col2) VALUES (s.id, s.col1, s.col2);
```

**关键约束**：

| 引擎 | 支持情况 | 前提条件 |
| --- | --- | --- |
| Hive 3.x | ✅ 支持 `MERGE` | **表必须是 ACID 事务表**（ORC + `transactional=true`） |
| Spark 3.x + Hive 表 | ❌ 不支持 | Spark 不能写 Hive ACID 表 |
| Spark 3.x + Delta Lake | ✅ 支持 | 表格式是 Delta |
| Spark 3.x + Iceberg | ✅ 支持 | 表格式是 Iceberg（Spark 3.2+） |
| Spark 3.x + Hudi | ✅ 支持 | 表格式是 Hudi |

> [!warning] Hive ACID 表的代价
> ```sql
> create table dwd.user_acid (
>     id bigint,
>     name string
> )
> clustered by (id) into 8 buckets
> stored as orc
> tblproperties ('transactional' = 'true');
> ```
> 代价：① **必须分桶**；② **只能 ORC**；③ 小文件问题更严重（delta 文件）；④ 查询性能比普通表差（要合并 delta）；⑤ 需要定期 `compaction`。
> **除非必须，不要用 ACID 表。**

### 2.3 没有 MERGE 时怎么做行级合并

**方案一：分区覆盖（推荐）**

```sql
-- 思路：把「受影响的分区」全量重算，覆盖写入
-- 前提：增量数据能确定影响哪些分区（如按 dt）

-- 步骤1：找出增量数据影响的分区
-- 步骤2：重算这些分区的全量数据（历史 + 增量合并后）
-- 步骤3：覆盖写入

insert overwrite table dwd.order partition (dt)
select order_id, user_id, status, amount, dt
from (
    -- 历史数据（排除受影响分区，避免重复）
    select order_id, user_id, status, amount, dt
    from dwd.order
    where dt not in (select distinct dt from ods.order_inc where dt = '${bizdate}')
    union all
    -- 增量数据（按主键去重，保留最新）
    select order_id, user_id, status, amount, dt
    from (
        select *, row_number() over (partition by order_id order by update_time desc) as rn
        from (
            select * from dwd.order where dt = '${bizdate}'
            union all
            select * from ods.order_inc where dt = '${bizdate}'
        ) t
    ) t2 where rn = 1
) t3;
```

> [!note] 这个写法看起来笨，但最稳
> 它的本质是「局部全量重算」。虽然没有 `MERGE` 优雅，但**不依赖任何特殊表格式**，任何引擎都能跑，且绝对幂等。
> 数据量大时，只重算受影响的分区，成本可控。

**方案二：`full outer join` 模拟 merge**

```sql
-- 用 full join 实现 update + insert
insert overwrite table dwd.user partition (dt = '${bizdate}')
select
    coalesce(s.id, t.id)         as id,
    coalesce(s.name, t.name)     as name,      -- 增量优先
    coalesce(s.phone, t.phone)   as phone,
    coalesce(s.update_time, t.update_time) as update_time
from dwd.user t
full outer join (
    -- 增量按主键去重
    select * from (
        select *, row_number() over (partition by id order by update_time desc) as rn
        from ods.user_inc where dt = '${bizdate}'
    ) x where rn = 1
) s on t.id = s.id
where t.dt = '${bizdate}';   -- 只处理当天的全量快照
```

**方案三：`left join` + `union all`（更清晰的拆解）**

```sql
insert overwrite table dwd.user partition (dt = '${bizdate}')
-- ① 增量中「已存在」的 → 更新
select s.id, s.name, s.phone, s.update_time
from dwd.user t
join ods.user_inc s on t.id = s.id
where t.dt = '${bizdate}' and s.dt = '${bizdate}'

union all

-- ② 增量中「不存在」的 → 插入
select s.id, s.name, s.phone, s.update_time
from ods.user_inc s
left anti join dwd.user t on t.id = s.id and t.dt = '${bizdate}'
where s.dt = '${bizdate}'

union all

-- ③ 历史中「增量没有」的 → 保留
select t.id, t.name, t.phone, t.update_time
from dwd.user t
left anti join ods.user_inc s on t.id = s.id and s.dt = '${bizdate}'
where t.dt = '${bizdate}';
```

> [!success] 三种方案怎么选
> | 方案 | 可读性 | 性能 | 适用 |
> | --- | --- | --- | --- |
> | 分区覆盖（全量重算） | 中 | 一般 | 数据量不大，或受影响分区少 |
> | `full outer join` | 中 | 好 | 单表 merge，字段不多 |
> | `left join` + `union all` | 好 | 一般（三次扫描） | 逻辑复杂，需要清晰拆解 |
> | 真 `MERGE INTO` | 最好 | 最好 | 有事务表（Delta/Iceberg/Hive ACID） |

### 2.4 删除怎么处理

**软删除 vs 硬删除**：

| 方式 | 做法 | 优点 | 缺点 |
| --- | --- | --- | --- |
| 软删除 | 加 `is_deleted` 标记 | 可追溯、可恢复 | 查询要带 `where is_deleted=0` |
| 硬删除 | 物理删除行 | 干净 | 不可恢复、下游可能出错 |

> [!important] 数仓推荐软删除
> ODS/DWD 层**永远不要物理删除**，用 `is_deleted` 标记。原因：
> 1. 下游任务可能依赖历史数据
> 2. 出问题能追溯
> 3. 物理删除在分区表上代价高（要重写整个分区）

**软删除的合并逻辑**：

```sql
-- 增量里有 op='D' 的记录 → 标记删除
select
    coalesce(s.id, t.id) as id,
    case when s.op = 'D' then 1 else coalesce(t.is_deleted, 0) end as is_deleted,
    ...
from dwd.user t
full outer join ods.user_inc s on t.id = s.id;
```

### 2.5 拉链表：另一种「增量合并」

**场景**：用户等级会变化，需要保留「每个等级从哪天到哪天」的完整历史。

**拉链表结构**：

| user_id | level | start_date | end_date |
| --- | --- | --- | --- |
| 101 | 普通 | 2026-01-01 | 2026-05-31 |
| 101 | VIP | 2026-06-01 | 9999-12-31 |

**每日更新逻辑**：

```sql
-- 步骤1：找出今天有变化的用户
-- 步骤2：把历史表中这些用户的「未关闭记录」关闭（end_date = 昨天）
-- 步骤3：插入新记录（start_date = 今天，end_date = 9999-12-31）

insert overwrite table dim.user_zip partition (dt = '${bizdate}')
-- ① 未变化的用户：原样保留
select user_id, level, start_date, end_date
from dim.user_zip
where dt = '${bizdate-1}'
  and user_id not in (select user_id from ods.user_change where dt = '${bizdate}')

union all

-- ② 变化的用户：关闭旧记录
select user_id, level, start_date,
       date_sub('${bizdate}', 1) as end_date
from dim.user_zip
where dt = '${bizdate-1}'
  and user_id in (select user_id from ods.user_change where dt = '${bizdate}')
  and end_date = '9999-12-31'

union all

-- ③ 变化的用户：插入新记录
select user_id, level, '${bizdate}' as start_date, '9999-12-31' as end_date
from ods.user_change
where dt = '${bizdate}';
```

> [!note] 拉链表的细节
> - 上面的写法是「每日全量快照」（每天存一份完整拉链表），存储成本高但查询简单
> - 另一种是「增量拉链表」（只追加变化），存储省但要合并查询
> - 模块2 会详细展开

### 2.6 幂等设计的四条铁律

| # | 铁律 | 说明 |
| --- | --- | --- |
| 1 | **写入用 `overwrite`，不用 `into`** | 除非明确要追加（如日志流水） |
| 2 | **分区键从调度参数来** | `partition(dt='${bizdate}')`，不要用 `current_date` |
| 3 | **去重时排序键必须唯一** | 加 `id desc` 兜底，否则重跑结果不同 |
| 4 | **所有中间结果可重算** | 不要依赖「上次运行的临时表」 |

> [!danger] 「伪幂等」的三种情况
> ① **`insert into` 追加**：重跑翻倍
> ② **`row_number` 排序键并列**：重跑结果可能不同（虽然行数一样）
> ③ **依赖 `current_timestamp()`**：每次跑写入的时间不同，重跑结果不同
> **验证幂等性的方法**：同一个分区连跑两次，对比 `count(*)`、`sum(金额)`、以及 `md5(所有行的有序拼接)`。

### 2.7 增量合并的常见模式对比

| 模式 | 输入 | 输出 | 适用 |
| --- | --- | --- | --- |
| 全量快照 | 每天全量 | 每天一份全量 | 小表、维表 |
| 增量追加 | 每天增量 | 只追加，不更新 | 日志、流水 |
| 增量合并（upsert） | 每天增量 | 更新+插入 | 订单、用户 |
| 拉链表 | 每天变化 | 保留历史版本 | 需要追溯变化的维表 |
| 快照+流水 | 全量+增量 | 两份都存 | 既要查最新又要查历史 |

---

## 三、动手实验

### 3.1 建表造数

```sql
-- 目标表（历史数据）
drop table if exists tmp.merge_target;
create table tmp.merge_target (id bigint, name string, amount decimal(10,2), update_time string)
partitioned by (dt string) stored as parquet;

insert overwrite table tmp.merge_target partition (dt='2026-10-01')
select 1, 'A', 100.00, '2026-10-01 10:00:00'
union all select 2, 'B', 200.00, '2026-10-01 10:00:00'
union all select 3, 'C', 300.00, '2026-10-01 10:00:00';

-- 增量表
drop table if exists tmp.merge_inc;
create table tmp.merge_inc (id bigint, name string, amount decimal(10,2), update_time string, op string);
insert into tmp.merge_inc values
(1, 'A2', 150.00, '2026-10-02 10:00:00', 'U'),   -- 更新
(4, 'D',  400.00, '2026-10-02 10:00:00', 'I'),   -- 新增
(2, null, null,  '2026-10-02 10:00:00', 'D');    -- 删除
```

### 3.2 实验一：`full outer join` 实现 upsert

```sql
insert overwrite table tmp.merge_target partition (dt='2026-10-02')
select
    coalesce(s.id, t.id)                                       as id,
    case when s.op = 'D' then null else coalesce(s.name, t.name) end as name,
    case when s.op = 'D' then 0    else coalesce(s.amount, t.amount) end as amount,
    coalesce(s.update_time, t.update_time)                     as update_time
from tmp.merge_target t
full outer join tmp.merge_inc s on t.id = s.id
where t.dt = '2026-10-01';

select * from tmp.merge_target where dt = '2026-10-02' order by id;
```

**预期结果**：

| id | name | amount | 说明 |
| --- | --- | --- | --- |
| 1 | A2 | 150.00 | 更新 |
| 2 | null | 0 | 删除（软删可以用 is_deleted） |
| 3 | C | 300.00 | 保留 |
| 4 | D | 400.00 | 新增 |

### 3.3 实验二：验证幂等性

```sql
-- 连跑两次，对比结果
-- 第一次
insert overwrite table tmp.merge_target partition (dt='2026-10-03') select * from tmp.merge_target where dt='2026-10-02';
-- 第二次（同样的 SQL）
insert overwrite table tmp.merge_target partition (dt='2026-10-03') select * from tmp.merge_target where dt='2026-10-02';

select count(*), sum(amount) from tmp.merge_target where dt = '2026-10-03';
-- 两次结果应完全一致
```

### 3.4 实验三：对比 `insert into` 的不幂等

```sql
drop table if exists tmp.not_idempotent;
create table tmp.not_idempotent (id int, amount decimal(10,2));

insert into tmp.not_idempotent select 1, 100.00;   -- 跑 1 次：1 行
insert into tmp.not_idempotent select 1, 100.00;   -- 再跑：2 行！
select count(*) from tmp.not_idempotent;            -- 2
```

### 3.5 实验四：去重排序键不唯一的「伪幂等」

```sql
-- 造两条 update_time 相同的数据
drop table if exists tmp.idem_lab;
create table tmp.idem_lab (id int, val string, update_time string);
insert into tmp.idem_lab values
(1, 'X', '2026-10-02 10:00:00'),
(1, 'Y', '2026-10-02 10:00:00');   -- 同时间

-- 只按 update_time 排序
select id, val from (
    select *, row_number() over (partition by id order by update_time desc) as rn
    from tmp.idem_lab
) t where rn = 1;
-- 结果可能是 X 也可能是 Y（不确定）

-- 加 id 兜底（假设有自增 id 列）
-- 或者用 val 兜底：order by update_time desc, val desc
```

### 3.6 实验五：Hive ACID 表的 MERGE（如果环境支持）

```sql
-- 建 ACID 表
create table tmp.acid_lab (id bigint, name string)
clustered by (id) into 4 buckets
stored as orc
tblproperties ('transactional'='true');

insert into tmp.acid_lab values (1,'A'),(2,'B');

-- MERGE
merge into tmp.acid_lab t
using (select 1 as id, 'A2' as name union all select 3, 'C') s
on t.id = s.id
when matched then update set name = s.name
when not matched then insert values (s.id, s.name);

select * from tmp.acid_lab;   -- 1:A2, 2:B, 3:C
```

> [!warning] ACID 表需要额外配置
> ```sql
> set hive.support.concurrency = true;
> set hive.enforce.bucketing = true;
> set hive.exec.dynamic.partition.mode = nonstrict;
> set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
> set hive.compactor.initiator.on = true;
> set hive.compactor.worker.threads = 1;
> ```

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| 重跑后数据翻倍 | 用了 `insert into` | 对比重跑前后 `count(*)` | 改 `overwrite` |
| 重跑后结果不同 | `row_number` 排序键并列 | 连跑两次对比 | 加唯一列兜底 |
| `MERGE` 报错不支持 | Hive 表不是 ACID / Spark 不支持 Hive ACID | 看报错信息 | 改分区覆盖；或换 Delta/Iceberg |
| ACID 表查询很慢 | 有大量 delta 文件未合并 | 看文件数 | 定期 `compact` |
| 合并后历史数据丢了 | 分区覆盖时漏了「历史保留」分支 | 对比合并前后行数 | 补 `left anti join` 保留分支 |
| 软删除标记没生效 | 增量 op 没处理 | 检查 merge 逻辑 | 补 `case when op='D'` |
| 拉链表有多条 `end_date=9999` | 关闭旧记录的逻辑漏了 | `group by user_id having count(*)>1` | 补关闭逻辑 |
| 增量表里有重复主键 | 上游重复推送 | `group by 主键 having count>1` | 合并前先去重 |
| 合并后金额对不上 | 增量数据的 `amount` 是 null 覆盖了历史 | 检查 `coalesce` | 用 `coalesce(s.x, t.x)` |
| 并发写同一分区冲突 | 两个任务同时 overwrite | 看调度依赖 | 加依赖；或用事务表 |

---

## 五、面试追问

> [!question] 1. 什么是幂等？为什么数仓任务必须幂等？
> **一句话答法**：同一份输入跑 N 次，结果完全一致。因为任务失败重试是常态，不幂等会导致数据重复或错误。
> **追问陷阱**：「怎么验证」——连跑两次，对比行数、金额合计、以及全部行的哈希值。

> [!question] 2. `insert into` 和 `insert overwrite` 的幂等性区别？
> **一句话答法**：`into` 追加，重跑翻倍，不幂等；`overwrite` 覆盖指定分区，重跑结果一致，幂等。
> **追问陷阱**：「什么时候必须用 `into`」——日志流水这类「追加即事实」的场景，但要靠上游保证不重推。

> [!question] 3. `MERGE INTO` 在 Hive 上能用吗？
> **一句话答法**：Hive 3.x 支持，但表必须是 ACID 事务表（ORC + 分桶 + `transactional=true`），代价是性能差、小文件多、要定期 compaction。
> **追问陷阱**：「Spark 能写 Hive ACID 表吗」——不能。Spark 要用 Delta/Iceberg/Hudi 才有原生 merge。

> [!question] 4. 没有 `MERGE INTO` 怎么做增量合并？
> **一句话答法**：三种方案——分区覆盖（局部全量重算）、`full outer join` 模拟 upsert、`left join + union all` 拆成更新/插入/保留三部分。
> **追问陷阱**：「哪个最稳」——分区覆盖，不依赖特殊表格式，绝对幂等。

> [!question] 5. 增量合并时删除怎么处理？
> **一句话答法**：数仓推荐软删除（`is_deleted` 标记），不物理删除。硬删除只在明确要求时做，且要评估下游影响。
> **追问陷阱**：「软删除后下游查询要注意什么」——所有查询都要带 `where is_deleted = 0`，建议封装成视图，避免漏写。

> [!question] 6. 拉链表怎么更新？
> **一句话答法**：三步——① 未变化的用户原样保留；② 变化的用户关闭旧记录（`end_date = 昨天`）；③ 变化用户插入新记录（`start_date = 今天`，`end_date = 9999-12-31`）。
> **追问陷阱**：「怎么验证拉链表正确」——每个用户只能有一条 `end_date = 9999-12-31` 的记录；且记录时间区间不能有重叠或空洞。

> [!question] 7. 分区覆盖和行级合并怎么选？
> **一句话答法**：看「增量影响的分区范围」。如果只影响最近几个分区（如订单状态 7 天内变），用分区覆盖；如果会改很久以前的数据（如 30 天后才完结），用行级合并。
> **追问陷阱**：「分区覆盖会不会很慢」——只重算受影响的分区，成本可控；如果影响范围大，才考虑行级合并。

> [!question] 8. 什么是「伪幂等」？
> **一句话答法**：行数一致但内容不一致。典型是 `row_number` 排序键并列导致每次取到不同的行，或者用了 `current_timestamp()`。
> **追问陷阱**：「怎么发现」——对比两次运行结果的哈希值（`md5(concat_ws(',', sort_array(collect_list(...))))`）。

> [!question] 9. 增量表里有重复主键怎么办？
> **一句话答法**：合并前先按主键去重，保留最新（`row_number` + 唯一排序键）。
> **追问陷阱**：「不去重会怎样」——merge 时同一主键匹配多行，结果不确定（Hive 会报错，Spark 可能产生多行）。

> [!question] 10. 怎么保证增量任务的可重跑性？
> **一句话答法**：四条铁律——用 `overwrite`、分区键来自调度参数、排序键唯一、中间结果可重算。
> **追问陷阱**：「如果上游数据变了呢」——上游变了重跑结果当然会变，这不算破坏幂等；幂等的前提是「同一份输入」。

---

## 六、自测题

> [!question] 1.（单选）下面哪种写入方式是幂等的？
> A. `insert into t select ...`
> B. `insert overwrite table t partition(dt='2026-10-01') select ...`
> C. `insert into t partition(dt='2026-10-01') select ...`
> D. 以上都不是

> [!question] 2.（单选）Hive 3.x 的 `MERGE INTO` 对目标表的要求是？
> A. 任意 Hive 表
> B. 必须是 Parquet 格式
> C. 必须是 ACID 事务表（ORC + 分桶 + transactional=true）
> D. 必须是外部表

> [!question] 3.（判断）Spark 3.x 可以直接对 Hive ACID 表执行 `MERGE INTO`。

> [!question] 4.（场景设计）有一张订单表 `dwd.order`（按 `dt` 分区，一行一单），订单状态会在下单后 30 天内变化（支付、发货、完成、退款）。现在每天有一份增量 `ods.order_inc`（含 `order_id`、`status`、`update_time`、`op`）。请设计增量合并方案，要求幂等，并说明为什么不能用「只覆盖当天分区」。

> [!question] 5.（场景设计）你发现一个每日任务重跑后数据翻倍了。请给出排查步骤和修复方案，并说明如何从设计上避免这类问题。

> [!success]- 答案与解析（点击展开）
> **1. B**
> `overwrite` + 静态分区，每次覆盖同一分区，结果一致。A 和 C 都是追加，重跑翻倍。
>
> **2. C**
> 必须是 ACID 事务表。这要求 ORC 格式、分桶、`transactional=true`，代价较高。
>
> **3. 错误**
> Spark 不能写 Hive ACID 表。Spark 要 merge 需要用 Delta Lake / Iceberg / Hudi。
>
> **4. 设计方案**
> **为什么不能只覆盖当天分区**：订单状态在 30 天内变化，意味着「今天的一条更新」可能修改「30 天前的分区」。只覆盖 `dt = 今天` 的分区，历史分区里的旧状态不会被更新，导致状态不一致。
>
> **方案：按 `update_time` 分区重算（推荐）**
> ```sql
> -- 步骤1：确定受影响的分区（增量数据的 update_time 落在哪些 dt 上）
> -- 假设分区键 dt 就是订单创建日期，那增量会影响多个历史分区
>
> -- 步骤2：重算受影响分区（局部全量）
> insert overwrite table dwd.order partition (dt)
> select order_id, user_id, status, amount, create_time, update_time, dt
> from (
>     -- 历史数据：排除受影响分区（这些分区会重算）
>     select order_id, user_id, status, amount, create_time, update_time, dt
>     from dwd.order
>     where dt not in (
>         select distinct date(create_time) from ods.order_inc where dt = '${bizdate}'
>     )
>
>     union all
>
>     -- 受影响分区：历史 + 增量合并后，按 order_id 去重保留最新
>     select order_id, user_id, status, amount, create_time, update_time, dt
>     from (
>         select *, row_number() over (
>             partition by order_id
>             order by update_time desc, op desc   -- 兜底
>         ) as rn
>         from (
>             select order_id, user_id, status, amount, create_time, update_time, dt
>             from dwd.order
>             where dt in (select distinct date(create_time) from ods.order_inc where dt='${bizdate}')
>             union all
>             select order_id, user_id, status, amount, create_time, update_time, dt
>             from ods.order_inc
>             where dt = '${bizdate}' and op != 'D'
>         ) t
>     ) t2 where rn = 1
> ) t3;
> ```
>
> **更优方案：拆成「订单主表 + 状态流水表」**
> - 主表 `dwd.order` 只存订单的**不可变属性**（下单时间、金额、用户），按 `dt` 分区，只追加不更新
> - 状态表 `dwd.order_status_log` 存所有状态变更（append-only），按 `dt` 分区
> - 需要「最新状态」时，用 `row_number` 从状态表取最新
> 这样**彻底避免更新**，天然幂等，也符合「不可变事实表 + 派生视图」的数仓最佳实践。
>
> **5. 排查步骤**
> ① **确认翻倍**：对比「今天的数据量」和「历史同期的数据量」，看是否正好 2 倍或 N 倍。
> ② **看 SQL 的写入方式**：搜 `insert into`。如果是 `into`，基本确定。
> ③ **看分区**：`show partitions` 看是否产生了重复分区（分区键有 NULL 或格式不一致）。
> ④ **看去重逻辑**：如果有 `row_number`，检查排序键是否唯一。
> ⑤ **看调度**：是否配置了失败重试 + 上游重推，导致跑了两遍。
>
> **修复方案**
> ```sql
> -- 步骤1：删除重复数据（保留一份）
> insert overwrite table dwd.order partition (dt='2026-10-01')
> select order_id, user_id, status, amount, update_time
> from (
>     select *, row_number() over (partition by order_id order by update_time desc) as rn
>     from dwd.order where dt = '2026-10-01'
> ) t where rn = 1;
>
> -- 步骤2：改 SQL，把 insert into 改成 insert overwrite
> ```
>
> **从设计上避免**
> | 措施 | 说明 |
> | --- | --- |
> | 编码规范：禁止 `insert into` | 除非明确是 append-only 流水表，且上游保证不重推 |
> | 上线前做幂等测试 | 连跑两次，对比行数和哈希 |
> | 加数据量监控 | 每日行数环比波动超过阈值告警 |
> | 调度加「失败重跑」保护 | 同一分区同一时间只能有一个任务实例 |
> | 用 `merge` 或分区覆盖 | 从机制上保证幂等 |

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 上游 | [01-06 窗口函数与帧边界](01-06%20%E7%AA%97%E5%8F%A3%E5%87%BD%E6%95%B0%E4%B8%8E%E5%B8%A7%E8%BE%B9%E7%95%8C.md) | `row_number` 去重是 merge 的核心 |
| 上游 | [01-07 CTE、UNION、子查询与去重方案](01-07%20CTE%E3%80%81UNION%E3%80%81%E5%AD%90%E6%9F%A5%E8%AF%A2%E4%B8%8E%E5%8E%BB%E9%87%8D%E6%96%B9%E6%A1%88.md) | 去重方案全景 |
| 上游 | [01-08 分区表设计与分区裁剪](01-08%20%E5%88%86%E5%8C%BA%E8%A1%A8%E8%AE%BE%E8%AE%A1%E4%B8%8E%E5%88%86%E5%8C%BA%E8%A3%81%E5%89%AA.md) | 分区覆盖是幂等的基础 |
| 直接下游 | [01-13 小文件与写入参数](01-13%20%E5%B0%8F%E6%96%87%E4%BB%B6%E4%B8%8E%E5%86%99%E5%85%A5%E5%8F%82%E6%95%B0.md) | 反复覆盖写入会产生小文件 |
| 延伸 | [00_模块2导航](../02%20%E6%A8%A1%E5%9D%972%20%E7%A6%BB%E7%BA%BF%E6%95%B0%E4%BB%93%E7%BB%B4%E5%BA%A6%E5%BB%BA%E6%A8%A1/00_%E6%A8%A1%E5%9D%972%E5%AF%BC%E8%88%AA.md) | 拉链表、SCD2 的完整设计 |
| 延伸 | [00_模块5导航](../05%20%E6%A8%A1%E5%9D%975%20%E6%B9%96%E4%BB%93%E4%B8%80%E4%BD%93Lakehouse/00_%E6%A8%A1%E5%9D%975%E5%AF%BC%E8%88%AA.md) | Delta/Iceberg 的 merge 能力 |

> [!tip] 学习建议
> 这一节的「分区覆盖 vs 行级合并」的选择判断最重要。实际工作中，**先把「不可变事实表 + 派生视图」的模式用起来**（订单主表 + 状态流水表），能避免 80% 的合并复杂度。
> 真需要行级合并时，优先考虑 Iceberg/Delta（如果有条件），而不是 Hive ACID 表。
