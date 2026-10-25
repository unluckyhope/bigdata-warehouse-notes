---
title: 02-06 拉链表实现（Hive、Spark）
module: 模块2 离线数仓维度建模
difficulty: ⭐⭐⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - 数仓
  - 建模
  - 拉链表
  - SCD2
  - Hive
  - Spark
---

# 02-06 拉链表实现（Hive、Spark）

> [!abstract] 一句话结论
> 拉链表 = 把「每个版本」压成「每个时间段一行」：普通快照是**每天一张全量照片**，拉链表是**每个人一张履历表**。
> 所以它的查询**必须带时间条件**——`start_date <= 某天 <= end_date`；漏掉时间条件，join 就会按「版本数」成倍放大。
> 它省的是存储，贵的是**正确性**：90% 的拉链事故不是算错，而是 join 写错。

---

## 一、心智模型：照片 vs 履历

| 方案 | 类比 | 存储量 | 查询 |
| --- | --- | --- | --- |
| 每日全量快照 | 每天给全公司拍一张集体照 | 天数 T × 人数 N | 简单，取分区 |
| 拉链表 | 每个人一张履历，每次变更加一段 | 人数 N + 变更次数 | 复杂，要按区间匹配 |
| SCD1 覆盖 | 只有最新一页 | 人数 N | 简单，但没历史 |

```
1001  2026-10-01 ~ 2026-10-01   北京   普通   ← 已闭链
1001  2026-10-02 ~ 2026-10-02   上海   普通   ← 已闭链
1001  2026-10-03 ~ 9999-12-31   上海   VIP    ← 开链（当前）
```

> [!important] 一句话理解
> `start_date` 到 `end_date` 是这条记录的**保质期**。查某天的状态，就是找「保质期覆盖那天」的那条记录。

---

## 二、核心知识点

### 2.1 表结构设计

```sql
create table dim.dim_user_zip (
    user_id    bigint  comment '自然键，可重复',
    user_name  string,
    user_level string,
    city       string,
    is_deleted int     comment '0=正常，1=已删除（墓碑行）',
    start_date string  comment '版本生效日 yyyy-MM-dd',
    end_date   string  comment '版本失效日，当前版本 9999-12-31',
    version    int     comment '第几版，可选',
    etl_time   timestamp comment '入库时间，可选'
) partitioned by (dt string comment '拉链快照日期：这份拉链表是 dt 那天的全量')
stored as orc;
```

**开链 / 闭链**：

- **开链**：`end_date = '9999-12-31'`，表示当前仍有效。
- **闭链**：`end_date = 变更前一天`，表示已被新版本替代。

> [!warning] 两种存储形态，别搞混
> | 形态 | 说明 | 存储 | 查询 |
> | --- | --- | --- | --- |
> | 每日全量快照式（推荐） | 每天存一份完整拉链，`partition(dt=...)` | 高（每天一份全量） | 简单，取一个分区 |
> | 增量追加式 | 只追加变化记录，不分区或按月分区 | 低 | 复杂，要合并所有历史 |
>
> 生产优先「每日全量快照式」：查询简单、可回溯、出问题能回滚。增量式只在亿级以上且查询模式固定时用。

### 2.2 初始化：从变更流水构建

```sql
insert overwrite table dim.dim_user_zip partition (dt = '2026-10-01')
select
    user_id, user_name, user_level, city, 0 as is_deleted,
    to_date(update_time) as start_date,
    coalesce(date_sub(to_date(lead(update_time) over (partition by user_id order by update_time)), 1),
             '9999-12-31') as end_date,
    row_number() over (partition by user_id order by update_time) as version,
    current_timestamp() as etl_time
from ods.ods_user_change
where dt <= '2026-10-01';
```

> [!tip] `lead` 比自关联快
> `lead(update_time)` 一次排序拿到下一版起始日，`date_sub(...,1)` 得到本版失效日。自关联「取大于当前的最小变更日」是 O(n²)，大维表会跑挂。

### 2.3 每日增量合并：三种写法

统一前提：今天的新版本先按 `user_id` 去重（同日多次只留最后一次），记为 `new`。

> [!warning] 自读自写在 Spark 上可能报错
> `insert overwrite t select ... from t` 在 Hive 3 通常可用（写临时目录再替换），但 Spark 3.5 可能抛 `Cannot overwrite a path that is being read`。生产统一「先写 tmp 表/分区，校验后再 exchange 或 overwrite 正式表」，既避开限制，又能回滚。

```sql
-- 所有写法共用的 new
with new as (
    select user_id, user_name, user_level, city, is_deleted, update_time
    from (
        select *, row_number() over (partition by user_id order by update_time desc) as rn
        from ods.ods_user_change where dt = '2026-10-03'
    ) t where rn = 1
)
```

#### 写法一：`full outer join` 判定 + `union all` 输出（经典）

> [!warning] 只用一条 full outer join 是错的
> 一个用户「关闭旧行」和「插入新行」是**两行输出**，而 `full outer join` 对同一个 key 只产生一行。所以经典写法是：先用 `full outer join` 判定「新增/变更/未变」，再 `union all` 分别输出。

```sql
insert overwrite table dim.dim_user_zip partition (dt = '2026-10-03')
-- ① 历史闭链行：原样保留
select user_id, user_name, user_level, city, is_deleted, start_date, end_date, version, etl_time
from dim.dim_user_zip
where dt = '2026-10-02' and end_date <> '9999-12-31'

union all

-- ② 旧开链行：未变则保留，变了则闭链（属性仍用旧值）
select o.user_id, o.user_name, o.user_level, o.city, o.is_deleted,
       o.start_date,
       case when n.user_id is not null then date_sub('2026-10-03', 1)
            else o.end_date end as end_date,
       o.version, o.etl_time
from (select * from dim.dim_user_zip
      where dt = '2026-10-02' and end_date = '9999-12-31') o
full outer join new n on o.user_id = n.user_id
where o.user_id is not null

union all

-- ③ 新版本：变更用户的新版本 + 新用户（属性用新值）
select n.user_id, n.user_name, n.user_level, n.city, n.is_deleted,
       '2026-10-03' as start_date, '9999-12-31' as end_date,
       1 as version, current_timestamp() as etl_time
from new n;
```

优点：每一段职责单一（历史/旧行/新行），最容易讲清楚。
缺点：三次扫表 + `union all`；`version` 这里简化成 1，生产要用 `max(version)+1`。
#### 写法二：`union all + row_number` 打标（推荐）

```sql
insert overwrite table dim.dim_user_zip partition (dt = '2026-10-03')
select user_id, user_name, user_level, city, is_deleted, start_date,
       case when src = 2 then '9999-12-31'
            when src = 1 and end_date = '9999-12-31' and has_new = 1
                 then date_sub('2026-10-03', 1)
            else end_date end as end_date,
       row_number() over (partition by user_id order by start_date) as version,
       etl_time
from (
    select *,
        row_number() over (partition by user_id, src order by start_date desc) as rn_src,
        max(case when src = 2 then 1 else 0 end) over (partition by user_id) as has_new
    from (
        -- src=1：昨天的全部版本
        select user_id, user_name, user_level, city, is_deleted,
               start_date, end_date, version, etl_time, 1 as src
        from dim.dim_user_zip where dt = '2026-10-02'
        union all
        -- src=2：今天的新版本
        select user_id, user_name, user_level, city, is_deleted,
               '2026-10-03' as start_date, '9999-12-31' as end_date,
               cast(null as int) as version, current_timestamp() as etl_time, 2 as src
        from new
    ) u
) t
where (src = 1 and (end_date <> '9999-12-31' or rn_src = 1))
   or (src = 2 and rn_src = 1);
```

思路：把「旧版本」和「新版本」`union all` 到一起打 `src` 标；旧的开链行如果发现有新版本（`has_new=1`）就改成闭链，其余原样输出。
优点：没有 `full outer join`，每一步都能单独调试；`row_number` 顺手解决同日多次变更。
缺点：`has_new` 用窗口算，仍然要按 `user_id` shuffle 一次。

#### 写法三：`merge into`（有事务表才可用）

`merge` 的问题是「一个 key 既要关闭旧行、又要插入新行」，而 `merge` 对同一 key 只能命中一个分支。解法：**source 每个变更用户造两行**，用 `op` 区分。

```sql
-- Spark 3.5 + Delta Lake
merge into dim.dim_user_zip t
using (
    select user_id, user_name, user_level, city, is_deleted, update_time, 'CLOSE' as op
    from new
    union all
    select user_id, user_name, user_level, city, is_deleted, update_time, 'NEW' as op
    from new
) s
on t.user_id = s.user_id
   and t.end_date = '9999-12-31'
   and t.dt = '2026-10-03'
when matched and s.op = 'CLOSE' then update set
    t.end_date = date_sub('2026-10-03', 1)
when not matched and s.op = 'NEW' then insert (
    user_id, user_name, user_level, city, is_deleted, start_date, end_date, version, etl_time, dt
) values (
    s.user_id, s.user_name, s.user_level, s.city, s.is_deleted,
    '2026-10-03', '9999-12-31', 1, current_timestamp(), '2026-10-03'
);
```

> [!warning] `not matched` 的判断依赖 ON 条件
> 上面 ON 里带了 `t.end_date='9999-12-31'`，所以「同一用户有旧开链行」时 `NEW` 行也会走 `not matched` → 插入成功；如果没有旧行（新用户），同样 `not matched` → 插入。这就是「两行 source」能同时完成关闭和插入的原因。
> 但要注意：如果目标表里同一用户有多条开链行，`CLOSE` 会更新多行，必须先保证唯一性。

**Hive 3.x ACID 版本**（限制严格）：

```sql
set hive.support.concurrency = true;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing = true;

create table dim.dim_user_zip_acid (
    user_id bigint, user_name string, user_level string, city string,
    is_deleted int, start_date string, end_date string, version int
) clustered by (user_id) into 16 buckets
stored as orc
tblproperties ('transactional' = 'true');

-- source 同样是「CLOSE + NEW」两行
merge into dim.dim_user_zip_acid t
using (
    select user_id, user_name, user_level, city, is_deleted, 'CLOSE' as op from new
    union all
    select user_id, user_name, user_level, city, is_deleted, 'NEW'   as op from new
) s
on t.user_id = s.user_id and t.end_date = '9999-12-31'
when matched and s.op = 'CLOSE' then update set t.end_date = date_sub('2026-10-03', 1)
when not matched and s.op = 'NEW' then insert values (
    s.user_id, s.user_name, s.user_level, s.city, s.is_deleted,
    '2026-10-03', '9999-12-31', 1
);
```

| 引擎 / 表格式 | MERGE | 前提 | 代价 |
| --- | --- | --- | --- |
| Hive 3.x ACID | ✅ | ORC + 分桶 + `transactional=true`；不能设 `transactional_properties=insert_only` | 小文件多、需 compaction、查询要合 delta |
| Hive 普通外部表 | ❌ | — | 只能分区覆盖重算 |
| Spark + Hive 表 | ❌ | Spark 不能写 Hive ACID 表 | 同上 |
| Spark + Delta / Iceberg / Hudi | ✅ | Delta 2.x+ / Iceberg 0.13+ / Hudi 0.10+ | 需 OPTIMIZE / compaction |

> [!warning] 参数默认值以自己集群为准
> 上面三个 `set` 是 Hive 3.x 的常见组合，不同发行版（CDP / HDP / 自建）默认值不同，`hive.txn.manager` 的类名也可能有差异——上线前用 `set -v` 实际确认。

#### 工程写法：临时分区 + 交换（保证幂等与可回滚）

```sql
-- ① 写到临时表的当天分区（不碰正式表）
insert overwrite table dim.dim_user_zip_tmp partition (dt = '2026-10-03')
select ...;   -- 同写法二

-- ② 校验：行数、开链数、区间无重叠
select count(*) as cnt,
       sum(case when end_date = '9999-12-31' then 1 else 0 end) as open_cnt
from dim.dim_user_zip_tmp where dt = '2026-10-03';

-- ③ 校验通过后交换分区（Hive 3，要求同构 managed 表；外部表需 待验证）
alter table dim.dim_user_zip exchange partition (dt = '2026-10-03')
with table dim.dim_user_zip_tmp;
```

`exchange partition` 是元数据操作，秒级完成，失败不影响正式分区；比直接 `insert overwrite` 多一层回滚能力。若集群不支持，退化为「写 tmp → `insert overwrite` 正式表」，仍然幂等。

### 2.4 查询姿势

```sql
-- ① 查最新（当前有效）
select * from dim.dim_user_zip
where dt = '2026-10-03' and end_date = '9999-12-31';

-- ② 查某天快照（拉链表的核心能力）
select * from dim.dim_user_zip
where dt = '2026-10-03'
  and start_date <= '2026-10-02' and end_date >= '2026-10-02';

-- ③ 事实表关联（正确姿势）
select f.order_id, f.dt, f.amount, d.user_level
from dwd.dwd_fact_order f
join dim.dim_user_zip d
  on f.user_id = d.user_id
 and f.dt >= d.start_date and f.dt <= d.end_date
where d.dt = '2026-10-03'      -- 必须限定拉链分区
  and f.dt between '2026-10-01' and '2026-10-03';
```

> [!danger] 忘记时间条件 → join 放大
> 设事实表 F 行，用户平均版本数 `E[v]`（按事实行加权），漏写时间条件后结果行数：
> **`F × E[v]`**。
> 例：1 亿订单，60% 用户 1 版、30% 2 版、10% 3 版 → `E[v] = 0.6×1 + 0.3×2 + 0.1×3 = 1.5` → **1.5 亿行**，金额同步放大 1.5 倍。
> 如果连 `d.dt` 也没限定，再乘分区数 P：`F × E[v] × P`。3 个分区就是 **4.5 亿行**。

### 2.5 性能：区间 join 不是等值 join

`f.dt between d.start_date and d.end_date` 无法走 hash join，只能 broadcast nested loop 或 sort-merge + 区间判断。维表超过几百万行就会很慢。

| 手段 | 做法 | 适用 |
| --- | --- | --- |
| 广播维表 | `/*+ broadcast(d) */`，阈值调到 100MB | 维表 < 几百万行 |
| 先收敛事实表 | 先按 `(user_id, dt)` 聚合/过滤，再 join | 事实表远大于维表 |
| 日期展开 | 把版本区间展开成每天一行，改等值 join | 只查最近 N 天 |
| 当前版本冗余表 | `dim_user_current` 只放开链行 | 90% 查询只看当前 |
| 混合方案 | 近 30~90 天用快照表，更早用拉链 | 历史查询少 |

> [!note] 不要按 `start_date` 分区
> 拉链表按「快照日期 `dt`」分区，不按 `start_date`。按 `start_date` 分区会把同一用户的多个版本散到不同分区，查某天要扫全部分区；按 `dt` 分区则「一个分区 = 某天的完整拉链」，查询只扫一个分区。`start_date` 只作为过滤列，不承担分区裁剪。

```sql
-- 推荐：先把事实表收敛到 (user_id, dt)，再关联
select t.user_id, t.dt, t.amount, d.user_level
from (
    select user_id, dt, sum(amount) as amount
    from dwd.dwd_fact_order
    where dt between '2026-10-01' and '2026-10-03'
    group by user_id, dt
) t
join dim.dim_user_zip d
  on t.user_id = d.user_id
 and t.dt >= d.start_date and t.dt <= d.end_date
where d.dt = '2026-10-03';
```

> [!tip] 分区裁剪比 join 优化更重要
> 先 `where f.dt between ...` 把事实表收敛到目标日期，再 join，比任何 hint 都有效。反之，先 join 全量事实表再过滤，会把区间 join 的代价放大几十倍。

### 2.6 成本对比：什么时候值得用拉链

设天数 `T`、实体数 `N`、行宽 `W`、日均变更率 `r`：

| 方案 | 行数 | 存储 |
| --- | --- | --- |
| 每日全量快照 | `T × N` | `T × N × W` |
| 拉链表 | `N + N × r × T` | `(N + N×r×T) × W` |

**节省倍数** = `T / (1 + r×T)`。

| 场景 | T | r | 倍数 | 结论 |
| --- | --- | --- | --- | --- |
| 用户维表，3 年 | 1095 | 0.5% | 1095 / 6.5 ≈ **169×** | 拉链明显划算 |
| 商品维表，3 年 | 1095 | 10% | 1095 / 110.5 ≈ **9.9×** | 拉链仍划算 |
| 商品价格，3 年 | 1095 | 30% | 1095 / 329.5 ≈ **3.3×** | 优势变小，且区间 join 慢 |
| 余额/积分，3 年 | 1095 | 100% | 1095 / 1096 ≈ **1×** | 不值得，用快照或 SCD3 |

> [!important] 变更率决定一切
> 当 `r → 100%`，拉链行数 ≈ 快照行数，却多了区间 join 的代价和合并逻辑的复杂度——**纯亏**。
> 余额、积分这类天天变的属性，本来就不该做维度：它们是**半可加度量**，应放周期快照事实表（见 [02-04 三类事实表](02-04%20%E4%B8%89%E7%B1%BB%E4%BA%8B%E5%AE%9E%E8%A1%A8.md)）。

### 2.7 删除与迟到数据

**删除**：不要物理删。两种做法：

| 做法 | 写法 | 优缺点 |
| --- | --- | --- |
| 只闭链，不插新行 | `end_date = 删除日 - 1` | 简单，但「已删除」这个状态丢了，无法区分「删除」和「从未存在」 |
| 墓碑行（推荐） | 闭旧链 + 插 `is_deleted=1`、`start_date=删除日`、`end_date=9999-12-31` | 可追溯删除时点，可做「注销用户历史消费」 |

**迟到数据**：一笔 10-01 的变更 10-04 才到，不能只重跑 10-04。

```
受影响区间 = [迟到变更的业务日期, 当前最新分区]
```

三种修复：

1. **全量重算**：从变更流水重放整个区间，最稳，适合维表不大。
2. **插入中间版本**：插入新版本，并修正前驱版本的 `end_date`；要保证「任一时点只命中一行」。
3. **重跑下游**：拉链修好后，所有依赖它的分区（10-01 之后）都要重跑，否则事实表关联的是旧拉链。

> [!danger] 补数是拉链表最难的部分
> 拉链是「链式依赖」：改了 10-01，10-02 的 `start_date` 可能不再正确。**必须从变更日一直重跑到最新**，不能只补一天。
> 设计时就要让任务支持「指定起始日期重跑」，而不是只能跑 T+1。

### 2.8 幂等重跑

| 写入方式 | 幂等 | 说明 |
| --- | --- | --- |
| `insert into` | ❌ | 重跑追加，开链行翻倍 |
| `insert overwrite` 静态分区 | ✅ | 覆盖当天分区，结果一致 |
| `insert overwrite` 动态分区 | ⚠️ | Hive 会清空所有分区；Spark 只覆盖涉及分区 |
| `merge into` | ✅ | 按主键合并，但 source 必须去重 |

**重跑同一天**：`insert overwrite partition(dt='D')` 会整体重算 D 分区（输入是 D-1 分区 + D 的变更），不需要先删。但如果用 `merge` 或 `insert into`，必须先 `delete from ... where dt='D'`，或保证 source 主键唯一。

> [!warning] 重跑 D 天，D+1 之后也要重跑
> 因为 D 天的拉链变了，D+1 的「昨日拉链」输入就变了。只重跑 D 会造成链式不一致——这是拉链任务和普通分区任务最大的区别。

---

## 三、动手实验

### 3.1 造 3 天变更流水（含同日多次、含删除）

```sql
create table ods.ods_user_change (
    user_id bigint, user_name string, user_level string, city string,
    is_deleted int, update_time string, dt string
) partitioned by (dt string) stored as orc;

-- 第 1 天：初始化 3 个用户
insert into ods.ods_user_change partition (dt = '2026-10-01') values
(1001,'张三','普通','北京',0,'2026-10-01 10:00:00'),
(1002,'李四','普通','广州',0,'2026-10-01 10:00:00'),
(1003,'王五','普通','深圳',0,'2026-10-01 10:00:00');

-- 第 2 天：1001 搬家，1004 新增，1003 同日两次变更
insert into ods.ods_user_change partition (dt = '2026-10-02') values
(1001,'张三','普通','上海',0,'2026-10-02 09:00:00'),
(1004,'赵六','普通','杭州',0,'2026-10-02 11:00:00'),
(1003,'王五','VIP','深圳',0,'2026-10-02 09:00:00'),
(1003,'王五','SVIP','深圳',0,'2026-10-02 15:00:00');

-- 第 3 天：1001 升级，1002 注销
insert into ods.ods_user_change partition (dt = '2026-10-03') values
(1001,'张三','VIP','上海',0,'2026-10-03 08:00:00'),
(1002,'李四','普通','广州',1,'2026-10-03 10:00:00');
```

### 3.2 初始化（10-01）

```sql
create table dim.dim_user_zip (
    user_id bigint, user_name string, user_level string, city string, is_deleted int,
    start_date string, end_date string, version int, etl_time timestamp
) partitioned by (dt string) stored as orc;

insert overwrite table dim.dim_user_zip partition (dt = '2026-10-01')
select user_id, user_name, user_level, city, is_deleted,
       to_date(update_time) as start_date,
       coalesce(date_sub(to_date(lead(update_time) over (partition by user_id order by update_time)), 1),
                '9999-12-31') as end_date,
       row_number() over (partition by user_id order by update_time) as version,
       current_timestamp()
from ods.ods_user_change where dt <= '2026-10-01';
```

预期 3 行，全部开链：1001 北京普通、1002 广州普通、1003 深圳普通。

### 3.3 第 2 天更新（含同日两次变更）

```sql
insert overwrite table dim.dim_user_zip partition (dt = '2026-10-02')
select user_id, user_name, user_level, city, is_deleted, start_date,
       case when src = 2 then '9999-12-31'
            when src = 1 and end_date = '9999-12-31' and has_new = 1
                 then date_sub('2026-10-02', 1)
            else end_date end as end_date,
       row_number() over (partition by user_id order by start_date) as version,
       etl_time
from (
    select *,
        row_number() over (partition by user_id, src order by start_date desc) as rn_src,
        max(case when src = 2 then 1 else 0 end) over (partition by user_id) as has_new
    from (
        select user_id, user_name, user_level, city, is_deleted,
               start_date, end_date, version, etl_time, 1 as src
        from dim.dim_user_zip where dt = '2026-10-01'
        union all
        select user_id, user_name, user_level, city, is_deleted,
               '2026-10-02' as start_date, '9999-12-31' as end_date,
               cast(null as int) as version, current_timestamp() as etl_time, 2 as src
        from (
            select *, row_number() over (partition by user_id order by update_time desc) as rn
            from ods.ods_user_change where dt = '2026-10-02'
        ) n where rn = 1
    ) u
) t
where (src = 1 and (end_date <> '9999-12-31' or rn_src = 1))
   or (src = 2 and rn_src = 1);
```

预期：

| user_id | city | level | start_date | end_date | is_deleted |
| --- | --- | --- | --- | --- | --- |
| 1001 | 北京 | 普通 | 10-01 | 10-01 | 0 |
| 1001 | 上海 | 普通 | 10-02 | 9999-12-31 | 0 |
| 1002 | 广州 | 普通 | 10-01 | 9999-12-31 | 0 |
| 1003 | 深圳 | 普通 | 10-01 | 10-01 | 0 |
| 1003 | 深圳 | SVIP | 10-02 | 9999-12-31 | 0 |
| 1004 | 杭州 | 普通 | 10-02 | 9999-12-31 | 0 |

注意 1003：同日 VIP → SVIP，只留 SVIP 一版（`rn=1`），不会产生两条开链。

### 3.4 第 3 天更新（含删除墓碑）

```sql
insert overwrite table dim.dim_user_zip partition (dt = '2026-10-03')
select user_id, user_name, user_level, city, is_deleted, start_date,
       case when src = 2 then '9999-12-31'
            when src = 1 and end_date = '9999-12-31' and has_new = 1
                 then date_sub('2026-10-03', 1)
            else end_date end as end_date,
       row_number() over (partition by user_id order by start_date) as version,
       etl_time
from (
    select *,
        row_number() over (partition by user_id, src order by start_date desc) as rn_src,
        max(case when src = 2 then 1 else 0 end) over (partition by user_id) as has_new
    from (
        select user_id, user_name, user_level, city, is_deleted,
               start_date, end_date, version, etl_time, 1 as src
        from dim.dim_user_zip where dt = '2026-10-02'
        union all
        select user_id, user_name, user_level, city, is_deleted,
               '2026-10-03' as start_date, '9999-12-31' as end_date,
               cast(null as int) as version, current_timestamp() as etl_time, 2 as src
        from (
            select *, row_number() over (partition by user_id order by update_time desc) as rn
            from ods.ods_user_change where dt = '2026-10-03'
        ) n where rn = 1
    ) u
) t
where (src = 1 and (end_date <> '9999-12-31' or rn_src = 1))
   or (src = 2 and rn_src = 1);
```

预期新增：

| user_id | city | level | start_date | end_date | is_deleted |
| --- | --- | --- | --- | --- | --- |
| 1001 | 上海 | 普通 | 10-02 | 10-02 | 0 |
| 1001 | 上海 | VIP | 10-03 | 9999-12-31 | 0 |
| 1002 | 广州 | 普通 | 10-01 | 10-02 | 0 |
| 1002 | 广州 | 普通 | 10-03 | 9999-12-31 | 1 |

1002 的墓碑行 `is_deleted=1`，`end_date=9999-12-31`——「已注销」是一个当前状态，不是闭链。

### 3.5 验证：查某天快照 vs 查最新

```sql
-- 查 10-02 当天快照（应该 4 个用户）
select user_id, user_name, city, user_level, is_deleted
from dim.dim_user_zip
where dt = '2026-10-03'
  and start_date <= '2026-10-02' and end_date >= '2026-10-02'
order by user_id;

-- 查最新（应该 4 行，1002 是 is_deleted=1）
select user_id, user_name, city, user_level, is_deleted
from dim.dim_user_zip
where dt = '2026-10-03' and end_date = '9999-12-31'
order by user_id;
```

**10-02 快照预期**：1001 上海普通、1002 广州普通、1003 深圳 SVIP、1004 杭州普通。
**最新预期**：1001 上海 VIP、1002 广州普通（`is_deleted=1`）、1003 深圳 SVIP、1004 杭州普通。

### 3.6 验证：漏时间条件的放大倍数

```sql
create table dwd.dwd_fact_order (
    order_id string, user_id bigint, amount decimal(18,2), dt string
) partitioned by (dt string) stored as orc;

insert into dwd.dwd_fact_order partition (dt = '2026-10-01') values
('O1',1001,100.00),('O2',1002,200.00),('O3',1003,300.00);
insert into dwd.dwd_fact_order partition (dt = '2026-10-02') values
('O4',1001,400.00),('O5',1003,500.00),('O6',1004,600.00);
insert into dwd.dwd_fact_order partition (dt = '2026-10-03') values
('O7',1001,700.00),('O8',1002,800.00);

-- ❌ 漏时间条件（且漏分区限定）：每条事实匹配该用户所有版本 × 所有分区
select count(*) from dwd.dwd_fact_order f
join dim.dim_user_zip d on f.user_id = d.user_id;

-- ✅ 正确：限定分区 + 时间区间
select count(*) from dwd.dwd_fact_order f
join dim.dim_user_zip d
  on f.user_id = d.user_id
 and f.dt >= d.start_date and f.dt <= d.end_date
where d.dt = '2026-10-03';
```

**预期**：正确结果 8 行（每条订单恰好命中一版）；错误结果远大于 8（按版本数 × 分区数放大）。用 `count(distinct f.order_id)` 对比更直观：正确 = 8，错误会重复计数。

### 3.7 行数与存储对比

```sql
-- 拉链表行数
select count(*) from dim.dim_user_zip where dt = '2026-10-03';   -- 8

-- 等价「每日全量快照」行数：3 天 × 每天用户数
-- 10-01: 3，10-02: 4，10-03: 4 → 11
```

小数据看不出差距。按 2.6 的公式放大到 100 万用户、3 年、日变更 0.5%：快照 10.95 亿行，拉链约 648 万行，差 **169 倍**。

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| join 后行数翻倍 | 没限定 `d.dt`，匹配了多个拉链分区 | join 前后 `count(*)` 对比 | 加 `d.dt='${bizdate}'` |
| 历史订单关联到当前等级 | 用 `end_date='9999-12-31'` 关联历史 | 对比两种关联的指标分布 | 改 `f.dt between start_date and end_date` |
| 一个用户多条开链 | 关闭旧行时漏了 `end_date='9999-12-31'` 条件 | `group by user_id having sum(open)>1` | 补条件，重刷拉链 |
| 同一天插入两条开链 | 同日多次变更没去重 | 查当天 `user_id` 重复 | `row_number` 去重保留最后 |
| 某天查不到数据（空洞） | 闭链/开链日期差一天 | 校验 `date_add(end,1)=next_start` | 统一 `end = next_start - 1` |
| 拉链越跑越大 | 变更率高（价格、库存、余额） | 看行数增长与 r | 改每日快照或 SCD3 |
| 补数后拉链断裂 | 只重跑了某一天 | 跑空洞校验 | 从变更日起全部重跑 |
| 重跑后开链翻倍 | 用了 `insert into` | `count(*)` 对比 | 改 `insert overwrite` 分区 |
| 关联极慢 | 区间 join 无法走 hash | 看执行计划 | 广播 / 先收敛事实表 / 日期展开 |
| `!=` 比较漏掉 NULL 变化 | `NULL != x` 是 UNKNOWN | 抽样看 NULL 字段 | `nvl(a,'') != nvl(b,'')` |
| 用户删除后仍能查到 | 删除没做墓碑/闭链 | 对比源表与拉链用户数 | 闭链 + `is_deleted=1` 墓碑 |
| 事实表外键为 NULL | 拉链没覆盖新用户 | `count(*)-count(user_id)` | 预置 `-1` 未知成员 |

---

## 五、面试追问

> [!question] 1. 拉链表怎么实现？
> **一句话答法**：一行存一个版本，`start_date/end_date` 划有效期；每日把「昨日拉链」和「今日变更」合并——未变的保留、变的闭旧链、新的开新链。
> **追问陷阱**：「闭链的 end_date 是哪天」——变更日的前一天（变更当天生效）；若语义是「次日起生效」，则闭链到变更日，两种都可以但必须全公司统一。

> [!question] 2. 为什么不用全量快照？
> **一句话答法**：快照存储是 `T×N×W`，拉链是 `(N+N×r×T)×W`；变更率低时拉链省 1~2 个数量级。但变更率高时拉链会膨胀到接近快照，还多了区间 join 的代价。
> **追问陷阱**：「什么时候不该用拉链」——余额、积分、库存这类天天变的属性，`r≈100%`，拉链不省空间，应该用周期快照事实表。

> [!question] 3. 数据量大到什么程度才值得用拉链表？
> **一句话答法**：看的是「变更率 × 天数」，不是绝对数据量。`T/(1+rT)` 大于 3~5 倍才值得；100 万用户、3 年、日变更 0.5% 时约 169 倍，很划算；日变更 30% 时只有 3 倍，还要承担区间 join 的成本。
> **追问陷阱**：「10 万用户要不要用」——10 万 × 1095 天 = 1.1 亿行快照，普通 Hive 也能扛；如果查询简单，快照反而更省心。拉链不是越早用越好。

> [!question] 4. 拉链表怎么查某天？
> **一句话答法**：`where dt='最新分区' and start_date <= '目标日' and end_date >= '目标日'`。`dt` 限定最新拉链分区，区间条件命中唯一版本。
> **追问陷阱**：「为什么不用那天的分区」——每日全量快照式拉链可以用那天的分区；增量式拉链没有那天的完整分区，只能靠区间还原。

> [!question] 5. 删除怎么处理？
> **一句话答法**：闭旧链 + 插墓碑行（`is_deleted=1`，`start_date=删除日`，`end_date=9999-12-31`）。不要物理删，否则删除前的事实关联不上。
> **追问陷阱**：「只闭链不插墓碑行行不行」——可以，但无法区分「删除」和「从未存在」，也做不了「注销用户历史消费」分析。

> [!question] 6. 重跑怎么保证幂等？
> **一句话答法**：用 `insert overwrite` 静态分区，重跑 D 天就整体重算 D 分区；不要用 `insert into`。用 `merge` 时 source 必须先按主键去重。
> **追问陷阱**：「只重跑 D 天就够了吗」——不够，D 天变了，D+1 的输入就变了，必须把 D 之后的所有分区一起重跑。

> [!question] 7. 拉链表 join 事实表为什么会放大？
> **一句话答法**：漏了时间条件，一条事实会匹配该用户的所有版本；漏了 `dt` 限定，再乘分区数。放大倍数 = `E[版本数] × 分区数`。
> **追问陷阱**：「怎么快速验证」——对比 join 前后的 `count(*)` 和 `count(distinct 事实主键)`，后者变大就是放大。

> [!question] 8. 三种合并写法怎么选？
> **一句话答法**：`full outer join` 经典但字段多时难维护；`union all + row_number` 最易读、易调试，推荐；`merge into` 只在有 ACID/Delta/Hudi 时用，且 source 要造 CLOSE/NEW 两行。
> **追问陷阱**：「普通 Hive 外部表能用 merge 吗」——不能，只能分区覆盖重算。

> [!question] 9. 拉链表性能怎么优化？
> **一句话答法**：广播小维表、先把事实表按 `(user_id, dt)` 收敛、日期展开改等值 join、维护当前版本冗余表、近端用快照远端用拉链。
> **追问陷阱**：「日期展开有什么代价」——行数 = 用户数 × 天数，只适合窄日期范围（如最近 30 天）。

> [!question] 10. 迟到数据怎么回补？
> **一句话答法**：受影响区间 = `[迟到变更的业务日期, 最新分区]`，从变更日重放到最新；只补一天会造成链式断裂。
> **追问陷阱**：「能插入中间版本吗」——能，但要同时修正前驱版本的 `end_date`，并校验任一时点只命中一行，风险比全量重放高。

> [!question] 11. `end_date` 为什么用 `9999-12-31` 而不是 NULL？
> **一句话答法**：NULL 参与比较是 UNKNOWN，`between` 查不出当前行；用极大值能保证区间比较成立，且 `end_date='9999-12-31'` 可直接判断开链。
> **追问陷阱**：「用 NULL 怎么补救」——每次查询 `coalesce(end_date,'9999-12-31')`，容易漏，且无法走分区裁剪。

> [!question] 12. 拉链表和 SCD2 什么关系？
> **一句话答法**：SCD2 是建模概念（保留版本），拉链表是物理实现（区间化存储）。维表场景几乎等价，拉链更强调每日增量合并和查询带时间条件。
> **追问陷阱**：「事实表能不能用拉链」——一般不用；事实表是 append-only，用拉链反而破坏「一行一个事件」的语义，只有累积快照才需要 update。

---

## 六、自测题

> [!question] 1.（单选）拉链表中「当前版本」的 `end_date` 通常是？
> A. `NULL`　B. 当天日期　C. `9999-12-31`　D. 空字符串

> [!question] 2.（单选）事实表关联拉链表，正确的写法是？
> A. `on f.user_id=d.user_id and d.end_date='9999-12-31'`
> B. `on f.user_id=d.user_id and f.dt between d.start_date and d.end_date`，并限定 `d.dt`
> C. `on f.user_id=d.user_id`
> D. `on f.user_id=d.user_id and d.start_date=f.dt`

> [!question] 3.（判断）拉链表比每日全量快照省存储，所以任何场景都应该用拉链表。

> [!question] 4.（场景设计）商品价格表每天约 30% 的商品变价，共 500 万商品，要保留 3 年。需求：① 每天看当前价格；② 偶尔分析某商品的价格曲线。请判断用拉链还是快照，给出理由和设计方案。

> [!question] 5.（场景设计）线上「关联用户维表后订单从 1 亿涨到 3 亿」。请给出排查步骤、最可能原因、修复 SQL，并说明如何防止复发。

> [!success]- 答案与解析（点击展开）
> **1. C**
> `9999-12-31`（或 `2999-12-31`）。用 NULL 会导致区间比较失效。
>
> **2. B**
> 必须限定拉链分区（避免匹配多个 `dt`），并按事件日期匹配区间（避免篡改历史）。
>
> **3. 错误**
> 变更率高时拉链会膨胀：500 万 × 30% × 1095 ≈ 16 亿行，且区间 join 极慢，可能比快照还差。
>
> **4. 结论：用每日全量快照（或「变化记录 + 月末快照」）。**
> 理由：`r=30%`，`T/(1+rT)=1095/329.5≈3.3`，省的存储有限；需求②按 `sku_id` 扫多天分区是等值查询，比区间 join 快得多。
> 设计：
> ```sql
> create table dim.dim_sku_price_snapshot (
>     sku_id bigint, sku_name string, price decimal(18,4), price_band string
> ) partitioned by (dt string) stored as orc;
>
> -- 当前价格
> select * from dim.dim_sku_price_snapshot where dt = '${bizdate}';
> -- 价格曲线
> select dt, price from dim.dim_sku_price_snapshot
> where sku_id = 12345 and dt between '2026-01-01' and '2026-10-25' order by dt;
> ```
> 存储优化：只保留「有变化的记录」+ 每月末全量；冷热分层，3 个月热、1 年温、更早归档。
>
> **5. 排查步骤**
> ① `show partitions dim_user_zip` 看有多少分区；② `select user_id,count(*) from dim_user_zip where dt='${bizdate}' group by user_id order by 2 desc limit 10` 看版本数；③ 跑区间重叠校验；④ 对比 join 前后行数。
> **最可能原因**：join 没限定 `d.dt`，匹配了 3 个拉链分区 → 3 倍；或区间重叠，一条事实命中 3 个版本。
> 修复：
> ```sql
> select f.order_id, d.user_level
> from dwd.dwd_fact_order f
> join dim.dim_user_zip d
>   on f.user_id = d.user_id
>  and f.dt between d.start_date and d.end_date
> where d.dt = '${bizdate}';
> ```
> 防复发：把「拉链 join 必须限定分区 + 时间区间」写进 SQL 规范；用视图封装（视图里已限定最新分区），业务方只访问视图；上线前跑「join 前后行数比」校验。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 前置 | [02-04 三类事实表](02-04%20%E4%B8%89%E7%B1%BB%E4%BA%8B%E5%AE%9E%E8%A1%A8.md) | 累积快照的 merge 与拉链合并是同一套 upsert 思路 |
| 前置 | [02-05 维度表设计与缓慢变化维 SCD](02-05%20%E7%BB%B4%E5%BA%A6%E8%A1%A8%E8%AE%BE%E8%AE%A1%E4%B8%8E%E7%BC%93%E6%85%A2%E5%8F%98%E5%8C%96%E7%BB%B4%20SCD.md) | SCD2 是概念，拉链是它的物理实现 |
| 直接下游 | [02-07 一致性维度与总线矩阵](02-07%20%E4%B8%80%E8%87%B4%E6%80%A7%E7%BB%B4%E5%BA%A6%E4%B8%8E%E6%80%BB%E7%BA%BF%E7%9F%A9%E9%98%B5.md) | 拉链是维度一致性的载体，全域要统一版本口径 |
| 直接下游 | [02-08 指标口径与指标体系](02-08%20%E6%8C%87%E6%A0%87%E5%8F%A3%E5%BE%84%E4%B8%8E%E6%8C%87%E6%A0%87%E4%BD%93%E7%B3%BB.md) | 「按当时状态」的指标必须声明用拉链的哪个版本 |
| 直接下游 | [02-11 建模评审 Checklist](02-11%20%E5%BB%BA%E6%A8%A1%E8%AF%84%E5%AE%A1%20Checklist.md) | 上线前检查开链唯一、区间无重叠、重跑幂等 |
| 实战 | [02-12 建模实战：电商订单域](02-12%20%E5%BB%BA%E6%A8%A1%E5%AE%9E%E6%88%98%EF%BC%9A%E7%94%B5%E5%95%86%E8%AE%A2%E5%8D%95%E5%9F%9F.md) | 用户维拉链在订单域中的落地 |
| 上游基础 | [00_模块1导航](../01%20%E6%A8%A1%E5%9D%971%20Hive%26Spark%20SQL/00_%E6%A8%A1%E5%9D%971%E5%AF%BC%E8%88%AA.md) | 窗口函数、`lead`、`row_number` 是拉链的工具 |
| 下游 | [00_模块3导航](../03%20%E6%A8%A1%E5%9D%973%20Spark%E8%B0%83%E4%BC%98%E3%80%81%E6%95%B0%E6%8D%AE%E8%B4%A8%E9%87%8F%E8%A1%80%E7%BC%98/00_%E6%A8%A1%E5%9D%973%E5%AF%BC%E8%88%AA.md) | 区间 join 的倾斜与调优在模块3展开 |

> [!tip] 学习建议
> 拉链表最值得亲手跑的是三件事：① 同日多次变更只留一版；② 查「某天快照」和「最新」的结果差异；③ 故意去掉时间条件，看行数放大几倍。
> 跑完你会记住一句话：**拉链表的正确性，全在 join 的那两个条件里。**
