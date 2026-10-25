---
title: 01-07 CTE、UNION、子查询与去重方案
module: 模块1 Hive & Spark SQL
difficulty: ⭐⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - Hive
  - SparkSQL
  - CTE
  - 去重
  - 子查询
---

# 01-07 CTE、UNION、子查询与去重方案

> [!abstract] 一句话结论
> 这一节解决「**怎么把复杂 SQL 写得又对又快**」：`CTE` 让逻辑分层可读，`UNION ALL` 是拼装多来源的标准手段，子查询决定性能（`IN` vs `EXISTS` vs `JOIN`），而 `DISTINCT` 和「去重」是两回事——**`DISTINCT` 是语法，去重是业务问题**（按什么键去？保留哪一条？）。
> 学完这节，你应该能判断：什么时候 `CTE` 会被物化、什么时候 `IN` 子查询会拖垮任务、去重该用 `row_number` 还是 `group by`。

---

## 一、心智模型：SQL 的「分层」与「拼接」

复杂 SQL 无非两种组织方式：

```
① 分层（纵向）：把一段逻辑封装起来，给个名字，后面复用
   CTE (WITH ...)  /  子查询  /  临时表

② 拼接（横向）：把多个结果集按行堆起来
   UNION ALL  /  UNION
```

**关键认知**：`CTE` 和子查询在**语义上等价**，但在**执行上可能完全不同**：

| 写法 | 优化器怎么处理 | 会不会重复计算 |
| --- | --- | --- |
| CTE（Hive / Spark） | 默认**内联展开**（inline），不是物化 | 引用 N 次 → 可能计算 N 次 |
| CTE + `materialized` 提示 | 物化成临时表 | 只算一次 |
| 子查询 | 内联展开 | 同上 |
| 临时表（`create table as`） | 真正落盘 | 只算一次，但多一次 IO |

> [!danger] 最常见的性能误解
> 「用了 CTE 就不会重复计算」——**错**。在 Hive 和 Spark 里，CTE 默认只是语法糖，被引用几次就展开几次。
> 如果 CTE 里是一个重扫描的聚合，在 SQL 里被引用 3 次，就等于扫了 3 遍表。
> **解决**：要么用临时表落盘，要么确认优化器做了 `ReuseExchange`（Spark 对相同子查询有复用优化，但不总是生效）。

---

## 二、核心知识点

### 2.1 CTE：让 SQL 有「变量」

```sql
with base as (
    select user_id, dt, amount
    from dwd.order
    where dt between '2026-10-01' and '2026-10-31'
),
user_agg as (
    select user_id, sum(amount) as total, count(*) as cnt
    from base
    group by user_id
)
select * from user_agg where total > 1000;
```

**CTE 的三个好处**：

1. **可读性**：把「取数 → 清洗 → 聚合 → 输出」拆成有名字的步骤
2. **复用**：同一段逻辑在多处引用，改一处即可
3. **递归**：`with recursive` 支持递归查询（树形结构、层级展开）

**CTE 的两种形态**：

```sql
-- 普通 CTE（默认内联）
with a as (select ...) select ... from a;

-- 物化 CTE（Hive 3.x 支持，Spark 不支持该语法）
with a as materialized (select ...) select ... from a;
```

> [!warning] Hive 和 Spark 对 CTE 的处理不同
> - **Hive 3.x**：支持 `as materialized`，可显式物化；默认也是内联
> - **Spark 3.x**：CTE 一律内联展开，**没有 materialized 语法**；但有 `ReuseExchange` 优化，相同子查询可能被复用
> - **Presto/Trino**：CTE 默认内联，可用 `MATERIALIZED` 提示
>
> 跨引擎时不要依赖 CTE 的物化行为，**重逻辑用临时表**。

**递归 CTE**（处理层级数据）：

```sql
-- 组织架构：从某个节点往下展开所有子节点
with recursive org_tree as (
    -- 锚点：根节点
    select id, parent_id, name, 1 as lvl
    from dim.org
    where id = 1

    union all

    -- 递归：子节点 = 上一层的子节点
    select o.id, o.parent_id, o.name, t.lvl + 1
    from dim.org o
    join org_tree t on o.parent_id = t.id
)
select * from org_tree order by lvl, id;
```

> [!note] 递归 CTE 的限制
> - Hive 3.x 支持，但需要 `hive.exec.max.dynamic.partitions` 等相关配置
> - Spark 3.x 从 3.0 开始支持 `WITH RECURSIVE`（需 `spark.sql.legacy.ctePrecedencePolicy` 等配置）
> - **生产上更常用的是「迭代写临时表」或「预计算层级路径」**，递归 CTE 在大数据量下容易跑挂

### 2.2 `UNION` vs `UNION ALL`：默认用后者

| | `UNION ALL` | `UNION` |
| --- | --- | --- |
| 是否去重 | 否 | **是**（等价 `UNION ALL` + `DISTINCT`） |
| 性能 | 快（纯拼接） | 慢（要多一次全局去重 shuffle） |
| 适用 | 确定无重复，或重复有意义 | 必须去重时 |

```sql
-- 快：直接拼接
select user_id from src_a
union all
select user_id from src_b;

-- 慢：去重（多一次 shuffle）
select user_id from src_a
union
select user_id from src_b;

-- 等价于（但更可控）
select distinct user_id from (
    select user_id from src_a
    union all
    select user_id from src_b
) t;
```

> [!important] 生产规范
> **默认写 `UNION ALL`**。需要去重时，显式写 `select distinct` 或 `group by`，让去重意图明确，也方便优化（比如先去重再 union 可能更快）。

**`UNION` 的三条硬规则**：

1. **按位置对齐**，不是按列名
2. **列数必须相同**
3. **类型按「宽类型」提升**（`int` + `string` → `string`）

```sql
-- ❌ 危险：位置错位，类型恰好兼容 → 静默错误
select user_id, amount from t1
union all
select amount, user_id from t2;

-- ✅ 正确：显式列名 + 显式 cast
select user_id, cast(amount as decimal(18,4)) as amount from t1
union all
select user_id, cast(amount as decimal(18,4)) as amount from t2;
```

### 2.3 子查询：三种位置，三种性能

**位置一：`FROM` 子查询（派生表）**

```sql
select * from (
    select user_id, sum(amount) as total from dwd.order group by user_id
) t where total > 1000;
```

**位置二：`WHERE` 子查询（`IN` / `EXISTS` / 标量）**

```sql
-- IN
select * from dwd.order where user_id in (select user_id from dim.vip);

-- EXISTS
select * from dwd.order o where exists (select 1 from dim.vip v where v.user_id = o.user_id);

-- 标量（返回单值）
select * from dwd.order where amount > (select avg(amount) from dwd.order);
```

**位置三：`SELECT` 子查询（标量子查询，慎用）**

```sql
-- ⚠️ 每行执行一次子查询，性能极差
select order_id,
       (select name from dim.user u where u.user_id = o.user_id) as user_name
from dwd.order o;
-- 应改为 join
```

> [!danger] `SELECT` 里的标量子查询是性能杀手
> 如果外层有 1 亿行，这个子查询会被执行 1 亿次（即使有缓存也很慢）。
> **一律改成 `left join`**。

### 2.4 `IN` vs `EXISTS` vs `JOIN`：怎么选

| 写法 | 语义 | 性能 | 什么时候用 |
| --- | --- | --- | --- |
| `IN (子查询)` | 值在集合中 | 子查询结果小 → 快；结果大 → 慢 | 子查询结果集小（几千行内） |
| `EXISTS` | 存在性判断 | 短路求值，找到就停 | 子查询表大、外层表小 |
| `LEFT SEMI JOIN` | 存在性判断 | **最优**，可走 hash join | 生产首选 |
| `LEFT JOIN + WHERE IS NULL` | 不存在性 | 好 | anti 场景 |
| `NOT IN` | 不在集合中 | **有 NULL 陷阱** | 几乎不要用 |

```sql
-- 推荐：semi join（存在性）
select o.* from dwd.order o
left semi join dim.vip v on o.user_id = v.user_id;

-- 推荐：anti join（不存在性）
select o.* from dwd.order o
left anti join dim.blacklist b on o.user_id = b.user_id;

-- 避免：NOT IN
select * from dwd.order
where user_id not in (select user_id from dim.blacklist);   -- NULL 会让结果为空
```

> [!success] 记忆口诀
> **「要判断存在，用 semi/anti；不要用 IN/NOT IN，除非你确认子查询没有 NULL。」**

### 2.5 `DISTINCT` 的代价与替代方案

**`DISTINCT` 做了什么**：对**整行**做全局去重 → 需要按全部列 shuffle → 数据量大时极慢。

```sql
-- 对 (user_id, dt) 组合去重
select distinct user_id, dt from dwd.user_action;
```

**`DISTINCT` 的三个问题**：

1. **整行去重**：不是对单列去重，容易误用
2. **全局 shuffle**：所有列参与分区，网络开销大
3. **无法保留其他列**：想去重又要保留 `amount`，`distinct` 做不到

**替代方案对比**：

| 需求 | 方案 | 说明 |
| --- | --- | --- |
| 单列去重计数 | `count(distinct col)` | 大基数改 `approx_count_distinct` |
| 多列组合去重 | `group by a, b` | 与 `distinct` 等价，但可加聚合列 |
| 去重且保留其他列（保留最新） | `row_number() over (partition by 键 order by 时间 desc) = 1` | **生产最常用** |
| 去重且保留其他列（保留最大/最小） | `group by 键` + `max_by` / `min_by`（Spark）/ `first` | 看引擎支持 |
| 去重后还要聚合 | 先 `group by` 再聚合 | 避免 `distinct` 后再 join |

```sql
-- 场景：用户表有重复，要保留最新一条完整记录
-- ❌ distinct 做不到（会保留所有不同的组合，可能还是多行）
select distinct user_id, name, phone, update_time from ods.user;

-- ✅ row_number（推荐）
select * from (
    select *, row_number() over (partition by user_id order by update_time desc) as rn
    from ods.user
) t where rn = 1;

-- ✅ group by + max_by（Spark 3.x）
select user_id, max_by(name, update_time) as name, max_by(phone, update_time) as phone
from ods.user group by user_id;
```

> [!note] `max_by` 的可用性
> Spark 3.0+ 支持 `max_by(x, y)`（返回 y 最大时对应的 x）。
> Hive 没有 `max_by`，但有 `max(struct(update_time, name)).col` 的技巧（用 struct 排序）：
> ```sql
> select user_id, max(named_struct('t', update_time, 'n', name)).n as name from ods.user group by user_id;
> ```
> 可移植性差，**跨引擎时优先用 `row_number`**。

### 2.6 去重方案全景（面试高频）

「去重」在数仓里至少有 5 种场景，选错方案会出大问题：

| 场景 | 需求 | 推荐方案 | 为什么 |
| --- | --- | --- | --- |
| ① 统计去重数 | 有多少个不同的用户 | `count(distinct user_id)` | 简单直接 |
| ② 明细去重（无主次） | 完全相同的行只留一条 | `select distinct` / `group by` | 无「保留哪条」的问题 |
| ③ 明细去重（保留最新） | 同一 ID 多条，留最新 | `row_number` 取 `rn=1` | 能控制保留规则 |
| ④ 跨表去重 | 两表合并后去重 | `union` 或 `union all` + 去重 | 注意 `union` 的 shuffle 代价 |
| ⑤ 增量去重（幂等） | 重跑不产生重复 | `merge into` / 分区覆盖 | 见 01-12 |

**方案③的完整模板**（务必背下来）：

```sql
-- 通用去重模板：按业务主键去重，保留最新
insert overwrite table dwd.user_dedup partition (dt = '${bizdate}')
select
    user_id, name, phone, update_time
from (
    select
        user_id, name, phone, update_time,
        row_number() over (
            partition by user_id                    -- 去重键
            order by update_time desc,              -- 保留规则：最新
                     id desc                        -- 兜底：保证唯一
        ) as rn
    from ods.user
    where dt = '${bizdate}'
) t
where rn = 1;
```

> [!danger] 去重的三个必备检查
> 1. **去重键选对了吗**：`user_id` 还是 `(user_id, tenant_id)`？漏了租户维度会把不同租户的同 ID 用户合并
> 2. **排序键唯一吗**：`order by update_time desc` 如果有并列，加 `id desc` 兜底，否则每次跑结果不同
> 3. **去重后行数对得上吗**：`count(*)` vs `count(distinct 去重键)` 应该相等，不等说明还有问题

**去重后的校验 SQL**：

```sql
-- 校验：去重键是否真的唯一（应返回 0 行）
select user_id, count(*) as cnt
from dwd.user_dedup
where dt = '2026-10-01'
group by user_id
having count(*) > 1;
```

### 2.7 子查询展开与谓词下推

```sql
-- 子查询里的过滤条件，能不能下推到扫描层？
select * from (
    select * from dwd.order where dt = '2026-10-01'
) t where amount > 100;

-- 优化器会合并成：
-- select * from dwd.order where dt = '2026-10-01' and amount > 100
-- → 分区裁剪 + 谓词下推都生效 ✅
```

但有些情况**下推会失效**：

```sql
-- ❌ 窗口函数阻断了谓词下推
select * from (
    select *, row_number() over (partition by user_id order by dt) as rn
    from dwd.order where dt = '2026-10-01'
) t where rn = 1 and amount > 100;
-- amount > 100 不能下推到窗口之前（会改变 rn 的结果），只能在外层过滤
```

> [!tip] 判断下推是否生效
> 看 `explain` 里 `TableScan` 节点的 `PartitionFilters` 和 `PushedFilters`。
> 如果外层的过滤条件出现在了 scan 节点，说明下推成功。

### 2.8 SQL 组织的实战建议

**什么时候用 CTE，什么时候用临时表**：

| 判断依据 | CTE | 临时表 |
| --- | --- | --- |
| 逻辑简单、引用 1~2 次 | ✅ | 过度设计 |
| 引用 3 次以上、且逻辑重 | ⚠️ 可能重复扫描 | ✅ |
| 需要多次调试中间结果 | ❌ | ✅ |
| 跨任务复用 | ❌ | ✅（中间表） |
| 只是为了让 SQL 可读 | ✅ | ❌ |

**推荐的 SQL 分层结构**（可直接套用）：

```sql
-- 第 1 层：取数（只做过滤和列裁剪）
with src as (
    select user_id, order_id, amount, dt
    from dwd.order
    where dt between '${start_date}' and '${end_date}'
),
-- 第 2 层：清洗（处理 NULL、类型、异常值）
clean as (
    select
        user_id,
        order_id,
        coalesce(amount, 0) as amount,
        dt
    from src
    where user_id is not null
),
-- 第 3 层：聚合（业务逻辑）
agg as (
    select user_id, sum(amount) as total, count(*) as cnt
    from clean
    group by user_id
)
-- 第 4 层：输出
select * from agg where total > 0;
```

> [!success] 这个结构的价值
> 出问题时能快速定位是「取数错」「清洗错」还是「聚合错」。比一条 200 行的嵌套子查询好维护 10 倍。

---

## 三、动手实验

### 3.1 建表造数

```sql
-- 有重复的用户表
drop table if exists tmp.dup_user;
create table tmp.dup_user (id int, user_id bigint, name string, phone string, update_time string);
insert into tmp.dup_user values
(1, 101, '张三', '13800000001', '2026-10-01 10:00:00'),
(2, 101, '张三丰', '13800000001', '2026-10-03 10:00:00'),   -- 同 user_id，更新
(3, 102, '李四', '13800000002', '2026-10-02 10:00:00'),
(4, 103, '王五', null,          '2026-10-01 10:00:00'),
(5, 103, '王五五', '13800000003', '2026-10-02 10:00:00'),
(6, 101, '张三', '13800000009', '2026-10-03 10:00:00');   -- 同时间！需要 id 兜底

-- 两个来源，用于 union 实验
drop table if exists tmp.src_a;
drop table if exists tmp.src_b;
create table tmp.src_a (user_id bigint, amount decimal(10,2));
create table tmp.src_b (user_id bigint, amount decimal(10,2));
insert into tmp.src_a values (101, 100.00), (102, 200.00);
insert into tmp.src_b values (102, 200.00), (103, 300.00);
```

### 3.2 实验一：`UNION` vs `UNION ALL`

```sql
select 'union_all' as t, count(*) as cnt from (
    select user_id from tmp.src_a union all select user_id from tmp.src_b
) x
union all
select 'union', count(*) from (
    select user_id from tmp.src_a union select user_id from tmp.src_b
) y;
```

**预期**：`union_all` = 4，`union` = 3（102 重复被去掉）。

### 3.3 实验二：`UNION` 位置错位的静默错误

```sql
-- ❌ 列顺序不一致，但类型兼容 → 不报错
select user_id, amount from tmp.src_a
union all
select amount, user_id from tmp.src_b;

-- 结果第二列出现 103.00 这种"user_id 变成金额"的行
```

**验证点**：这类错误 SQL 不报错，只有看数据才能发现。

### 3.4 实验三：三种去重方案对比

```sql
-- 方案A：distinct（会保留 6 行中的不同组合，不能保证 user_id 唯一）
select distinct user_id, name, phone from tmp.dup_user;

-- 方案B：row_number（正确）
select user_id, name, phone, update_time
from (
    select *, row_number() over (partition by user_id order by update_time desc, id desc) as rn
    from tmp.dup_user
) t where rn = 1;

-- 方案C：group by + max(struct)（Hive 技巧）
select
    user_id,
    max(named_struct('t', update_time, 'id', id, 'n', name)).n as name,
    max(named_struct('t', update_time, 'id', id, 'p', phone)).p as phone
from tmp.dup_user
group by user_id;
```

**预期**：B 和 C 应该结果一致（3 行）；A 会返回 5~6 行（因为 phone/name 不同）。

> [!warning] 注意 id=6 那行
> `user_id=101` 有两条 `update_time` 相同的记录（id=2 和 id=6）。如果只按 `update_time desc` 排序，`row_number` 的结果**不确定**。加上 `id desc` 后才能稳定取到 id=6。这就是「排序键必须唯一」的实证。

### 3.5 实验四：`IN` vs `semi join` 的 NULL 差异

```sql
drop table if exists tmp.vip_lab;
create table tmp.vip_lab (user_id bigint);
insert into tmp.vip_lab values (101), (null);

-- A：IN（含 NULL 但只影响 NOT IN）
select count(*) as cnt_in from tmp.dup_user where user_id in (select user_id from tmp.vip_lab);

-- B：NOT IN（结果为空！）
select count(*) as cnt_not_in from tmp.dup_user where user_id not in (select user_id from tmp.vip_lab);

-- C：anti join（正确）
select count(*) as cnt_anti from tmp.dup_user u
left anti join tmp.vip_lab v on u.user_id = v.user_id;

-- D：NOT IN + 过滤 NULL（也正确）
select count(*) as cnt_not_in_fixed from tmp.dup_user
where user_id not in (select user_id from tmp.vip_lab where user_id is not null);
```

**预期**：`cnt_not_in` = 0，其余三个都正常。

### 3.6 实验五：CTE 是否被重复计算

```sql
-- 造一张稍大的表
drop table if exists tmp.big_cte;
create table tmp.big_cte as
select a.id * 1000 + b.id as id, a.amount, a.user_id
from tmp.src_a a cross join tmp.src_b b;

-- 引用 CTE 三次
with base as (
    select user_id, sum(amount) as total from tmp.big_cte group by user_id
)
select
    (select count(*) from base) as c1,
    (select sum(total) from base) as c2,
    (select max(total) from base) as c3;

-- 看执行计划
explain extended
with base as (
    select user_id, sum(amount) as total from tmp.big_cte group by user_id
)
select * from base a join base b on a.user_id = b.user_id;
```

**看什么**：计划里 `Aggregate` 出现几次。如果出现 2 次，说明 CTE 被展开了 2 次（重复扫描）。

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| CTE 写得很多但任务很慢 | CTE 被内联展开，重复扫描 | `explain` 看 scan/aggregate 节点数量 | 重逻辑落临时表 |
| `union` 后数据少了 | 用了 `UNION` 而非 `UNION ALL`，被去重 | 对比 `union all` 的行数 | 确认是否真需要去重 |
| `union` 后列错位 | 按位置对齐，列顺序不一致 | 抽样看数据 | 显式写列名，顺序一致 |
| `NOT IN` 返回空 | 子查询含 NULL | 检查子查询 `is null` | 改 anti join |
| 去重后仍有重复 | 去重键选错 / 排序键不唯一 | `group by 键 having count>1` | 补全去重键；排序键加唯一列 |
| 去重结果每次不同 | `row_number` 排序键并列 | 多次执行对比 | 加 `id desc` 兜底 |
| `select` 里子查询极慢 | 标量子查询逐行执行 | 看执行计划有 `Subquery` 节点 | 改 `left join` |
| 谓词没下推 | 子查询里有窗口/聚合阻断 | `explain` 看 `PushedFilters` | 把过滤条件提到内层 |
| `distinct` 内存溢出 | 高基数 + 多列，去重集太大 | 看去重后的行数 | 改 `group by` 分批；或近似去重 |
| 递归 CTE 跑挂 | 层级太深 / 数据量太大 | 看递归层数 | 改迭代写临时表 |

---

## 五、面试追问

> [!question] 1. CTE 会被物化吗？
> **一句话答法**：Hive 和 Spark 默认都**不物化**，CTE 只是语法糖，被引用几次就展开几次。Hive 3.x 可以用 `as materialized` 显式物化，Spark 不支持。
> **追问陷阱**：「那怎么避免重复计算」——重逻辑落临时表，或用 `cache table`（Spark）。

> [!question] 2. `UNION` 和 `UNION ALL` 有什么区别？性能差多少？
> **一句话答法**：`UNION` 去重（多一次全局 shuffle），`UNION ALL` 直接拼接。数据量大时性能差一个数量级。
> **追问陷阱**：「什么情况必须用 UNION」——确实需要去重且无法提前去重时。但更推荐 `union all` + 显式 `distinct`，意图更清楚。

> [!question] 3. `UNION` 的列是怎么对齐的？
> **一句话答法**：按**位置**对齐，不是按列名；列数必须相同；类型按宽类型提升。
> **追问陷阱**：「列顺序不一致会报错吗」——不会，只要类型兼容就会静默出错，这是很危险的一类 bug。

> [!question] 4. `IN`、`EXISTS`、`JOIN` 怎么选？
> **一句话答法**：存在性判断首选 `left semi join`；不存在性用 `left anti join`；`IN` 只在子查询结果集很小时用；`NOT IN` 几乎不要用（NULL 陷阱）。
> **追问陷阱**：「`EXISTS` 和 `IN` 性能差别」——老版本 MySQL 有差别，Hive/Spark 里优化器通常会把它们改写成 join，差别不大，但 `NOT IN` 的语义坑始终存在。

> [!question] 5. `NOT IN` 为什么会返回空？
> **一句话答法**：子查询结果含 NULL 时，`x NOT IN (1,2,NULL)` 等价于 `x<>1 AND x<>2 AND x<>NULL`，最后一项是 UNKNOWN，整体不成立。
> **追问陷阱**：「`NOT EXISTS` 有这个问题吗」——没有，`NOT EXISTS` 是按行判断，NULL 不影响。

> [!question] 6. 去重有哪几种方案？怎么选？
> **一句话答法**：① `distinct`（完全相同的行去重）；② `group by`（可加聚合）；③ `row_number`（保留最新/最大，能控制规则）；④ `union`（跨表去重）；⑤ `merge into`（增量幂等）。
> **追问陷阱**：「要保留最新一条完整记录用哪个」——`row_number`，因为 `distinct` 无法指定保留规则。

> [!question] 7. `row_number` 去重时排序键怎么选？
> **一句话答法**：先按业务规则（如 `update_time desc`），再加一个唯一列兜底（如 `id desc`），保证排序结果确定。
> **追问陷阱**：「不加兜底会怎样」——排序键并列时结果不确定，每次跑出来的数据可能不同，任务不可复现。

> [!question] 8. `distinct` 和 `group by` 性能有区别吗？
> **一句话答法**：现代优化器里两者执行计划几乎一致（都是 shuffle + 去重）。但 `group by` 更灵活（可以加聚合列），可读性也更好。
> **追问陷阱**：「`count(distinct a), count(distinct b)` 为什么慢」——每个 distinct 需要独立去重，会导致多次 shuffle 或数据膨胀。

> [!question] 9. 子查询什么时候会导致性能问题？
> **一句话答法**：三种情况——① `SELECT` 里的标量子查询（逐行执行）；② 相关子查询（引用外层列，逐行执行）；③ 子查询里有窗口/聚合，阻断谓词下推。
> **追问陷阱**：「怎么改」——标量子查询改 join；相关子查询改 join + 聚合；阻断下推的把过滤条件提到内层。

> [!question] 10. 什么是谓词下推？什么时候会失效？
> **一句话答法**：把过滤条件尽可能推到数据扫描层，减少参与计算的数据量。窗口函数、聚合、`limit`、非确定性函数会阻断下推。
> **追问陷阱**：「怎么验证」——`explain` 看 `TableScan` 节点的 `PushedFilters` 和 `PartitionFilters`。

> [!question] 11. 递归 CTE 能解决什么问题？有什么风险？
> **一句话答法**：解决层级/树形数据的展开（组织架构、类目树、依赖关系）。风险是层级深或数据量大时会跑挂（递归次数不可控、内存压力大）。
> **追问陷阱**：「生产上替代方案」——迭代写临时表（每次展开一层），或预计算层级路径（`path` 字段）。

> [!question] 12. 一条 200 行的嵌套 SQL，你会怎么重构？
> **一句话答法**：按「取数 → 清洗 → 聚合 → 输出」拆成多个 CTE，每层只做一件事；重逻辑落临时表；关键中间结果加校验（行数、金额合计）。
> **追问陷阱**：「拆成 CTE 会不会变慢」——可能会（CTE 内联导致重复扫描），所以「重逻辑」要落临时表而不是 CTE。

---

## 六、自测题

> [!question] 1.（单选）Hive 3.x 中，一个 CTE 在 SQL 里被引用 3 次，默认会执行几次？
> A. 1 次
> B. 3 次
> C. 取决于优化器，可能 1~3 次
> D. 报错

> [!question] 2.（单选）`select user_id from a union select user_id from b` 与 `union all` 相比，多了什么开销？
> A. 多一次扫描
> B. 多一次全局去重 shuffle
> C. 多一次排序
> D. 没有区别

> [!question] 3.（判断）`UNION` 合并两个查询时，如果列名不同但位置相同、类型兼容，会报错。

> [!question] 4.（场景设计）ODS 层有一张用户表，同一个 `user_id` 可能有多条记录（来自不同业务系统的同步），需要按 `update_time` 保留最新一条，且要保留所有字段。请写出完整 SQL，并说明如何验证去重结果的正确性。

> [!question] 5.（场景设计）你需要把三个来源的订单数据（`src_app`、`src_web`、`src_mini`）合并成一张全渠道订单表。三个来源的字段不完全一致，且存在同一订单在多个来源重复的情况（以 `order_id` 为准，保留 `update_time` 最新的）。请设计完整方案，说明每一步的作用和可能的坑。

> [!success]- 答案与解析（点击展开）
> **1. C**
> 默认是内联展开，理论上会执行 3 次。但 Spark 有 `ReuseExchange` 优化，如果子查询完全相同且结果被复用，可能只算 1 次。**不要依赖这个行为**，重逻辑要落临时表。
>
> **2. B**
> `UNION` 等价于 `UNION ALL` + `DISTINCT`，多了一次全局去重（需要 shuffle 全部列）。扫描次数不变。
>
> **3. 错误**
> 不会报错。`UNION` 按位置对齐，列名取第一个查询的，类型按宽类型提升。列名不同但位置/类型兼容时会**静默成功**，这正是危险之处。
>
> **4. 参考 SQL**
> ```sql
> insert overwrite table dwd.user_dedup partition (dt = '${bizdate}')
> select user_id, name, phone, email, update_time, src_system
> from (
>     select
>         user_id, name, phone, email, update_time, src_system,
>         row_number() over (
>             partition by user_id
>             order by update_time desc, id desc   -- id 兜底保证确定性
>         ) as rn
>     from ods.user
>     where dt = '${bizdate}'
> ) t
> where rn = 1;
> ```
> **验证方法（三条）**
> ① **唯一性**：`select user_id, count(*) from dwd.user_dedup where dt='${bizdate}' group by user_id having count(*)>1;` 应返回 0 行。
> ② **行数守恒**：`count(*)`（去重后）应等于 `count(distinct user_id)`（去重前）。
> ③ **保留规则正确**：抽几个有重复的 `user_id`，人工比对是否保留了 `update_time` 最大的那条。
> ④ **可复现性**：连跑两次，结果应完全一致（验证排序键唯一性）。
>
> **5. 完整方案**
> ```sql
> -- 步骤1：各来源标准化（字段对齐 + 类型统一 + 空值处理）
> with app as (
>     select order_id, user_id,
>            cast(amount as decimal(18,4)) as amount,
>            coalesce(pay_time, create_time) as pay_time,
>            update_time, 'app' as src
>     from ods.order_app where dt = '${bizdate}'
> ),
> web as (
>     select order_id, user_id,
>            cast(total_amount as decimal(18,4)) as amount,   -- 字段名不同，需映射
>            pay_time,
>            update_time, 'web' as src
>     from ods.order_web where dt = '${bizdate}'
> ),
> mini as (
>     select order_id, user_id,
>            cast(amount as decimal(18,4)) as amount,
>            coalesce(pay_time, create_time) as pay_time,
>            update_time, 'mini' as src
>     from ods.order_mini where dt = '${bizdate}'
> ),
> -- 步骤2：合并（UNION ALL，不去重）
> all_src as (
>     select * from app
>     union all select * from web
>     union all select * from mini
> ),
> -- 步骤3：按 order_id 去重，保留最新
> dedup as (
>     select order_id, user_id, amount, pay_time, update_time, src
>     from (
>         select *, row_number() over (
>             partition by order_id
>             order by update_time desc, src desc    -- src 作为兜底
>         ) as rn
>         from all_src
>     ) t where rn = 1
> )
> -- 步骤4：写入
> insert overwrite table dwd.order_all partition (dt = '${bizdate}')
> select * from dedup;
> ```
> **每一步的作用与坑**
> | 步骤 | 作用 | 坑 |
> | --- | --- | --- |
> | 标准化 | 统一字段名/类型 | 金额字段名不同（`amount` vs `total_amount`），漏映射会错位；类型不一致会导致 union 后变 string |
> | `UNION ALL` | 拼接 | **不要用 `UNION`**，否则会在合并阶段就去重（按全列），可能把「同一订单不同状态」误合并 |
> | 去重 | 按 `order_id` 保留最新 | 排序键要唯一（`update_time` 可能相同）；`src desc` 只是兜底，业务上要确认优先级 |
> | 写入 | 分区覆盖 | 必须 `insert overwrite`，否则重跑会产生重复 |
>
> **额外注意**：① 三个来源的 `order_id` 编码规则是否一致（有的带前缀）？不一致要先规范化；② `update_time` 的时区是否统一？③ 去重后要对比各来源的订单数之和，差额应该等于重复数。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 上游 | [[01-03 SQL 执行顺序与常见误区]] | 子查询/CTE 的执行时机 |
| 上游 | [[01-04 多表 Join 语义与陷阱]] | semi/anti join 是 `IN` 的替代 |
| 上游 | [[01-06 窗口函数与帧边界]] | `row_number` 去重的核心 |
| 直接下游 | [[01-08 分区表设计与分区裁剪]] | 写入方式（overwrite/append）与分区 |
| 直接下游 | [[01-12 增量合并：MERGE INTO 与幂等设计]] | 增量场景的幂等去重 |
| 延伸 | [[00_模块2导航]] | 拉链表、SCD2 都依赖去重逻辑 |

> [!tip] 学习建议
> 这一节的实战价值极高。建议**把 2.6 的「去重方案全景表」和 3.4 的模板抄进你的代码片段库**——去重是数仓里每天都要写的逻辑，有一套固定模板能省很多时间，也能避免踩「排序键不唯一」这种坑。
