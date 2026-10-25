---
title: 02-11 建模评审 Checklist
module: 模块2 离线数仓维度建模
difficulty: ⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - 数仓
  - 建模
  - 评审
---

# 02-11 建模评审 Checklist

> [!abstract] 一句话结论
> 评审的唯一目的是**把「事后修数据」的成本前移到「事前看设计」**。一次修历史数据的成本 ≈ 一次评审成本的 **100 倍**：要重跑 N 天分区、通知 M 个下游、解释口径、重建信任，而评审只是四个人看半小时文档。
> 所以评审不是走流程，是用最低成本的一次性投入，买断后面所有返工。

---

## 一、心智模型：成本曲线决定了评审必须存在

同一个错误，在不同阶段被发现的代价：

| 发现时点 | 典型动作 | 相对成本 |
| --- | --- | --- |
| 设计评审 | 改一段文字、加一条规则 | 1× |
| 开发自测 | 改 SQL、重跑当天 | 5× |
| 上线后 T+1 | 修 SQL、重跑 1 天 + 下游 3 张表 | 20× |
| 上线后 1 年（已进 KPI） | 重跑 365 天 + 财务对账 + 解释去年数字为什么变了 | 1000× |

成本不是线性增长，是按「影响天数 × 下游数量」乘出来的。

> [!important] 评审要回答的三个问题
> 1. **这张表一行是什么？**（粒度说不清就是没想清）
> 2. **主键唯一吗？**（不唯一，后面所有金额都会翻倍）
> 3. **口径谁签字？**（没人签字的口径，三个月后一定漂移）

20 项检查的分组逻辑，从「结构」到「运维」，越往后越容易事后补救：

```
A 粒度与主键   → 错了整张表作废，只能重建
B 维度与关联   → 错了金额翻倍 / 丢数，要重跑
C 指标与口径   → 错了数字含义变了，要重跑 + 解释
D 分区与存储   → 错了查询慢 / 删不掉，可渐进修
E 幂等与可重跑 → 错了天天救火，可加兜底
```

---

## 二、核心知识点

### 2.1 A 组：粒度与主键

| # | 检查项 | 为什么重要 | 怎么检查 | 不合格后果 |
| --- | --- | --- | --- | --- |
| A1 | 粒度能用一句话写清 | 粒度是地基，说不清一定混 | 文档里找「一行 = 一个 ___」 | 开发和下游各按各的理解用 |
| A2 | 主键唯一且非空 | 不唯一 = 一对多，金额必翻倍 | `group by 主键 having count(*)>1` | 金额/件数翻倍，且很难发现 |
| A3 | 不存在混合粒度 | 订单级行和明细级行混在一张表 | `count(*)/count(distinct order_id)` 是否稳定 | 聚合结果随机偏大或偏小 |
| A4 | 只存一个业务过程 | 下单和支付混表，时间口径必打架 | 看是否同时有 `pay_time`/`refund_time` 且都可空 | 一行两个时间，指标按哪个算说不清 |

```sql
-- A2 主键唯一（应为 0 行）+ 非空（null_pk 应为 0）
select order_id, order_item_id, count(*) as cnt
from dwd.dwd_trade_order_detail_di
where dt = '2026-10-01'
group by order_id, order_item_id
having count(*) > 1;

select count(*) as total,
       sum(case when order_id is null or order_item_id is null then 1 else 0 end) as null_pk
from dwd.dwd_trade_order_detail_di where dt = '2026-10-01';

-- A3 混合粒度：items_per_order 应稳定在 2~3；变成 1.0 说明混进了订单级行
select count(*) as rows_cnt,
       count(distinct order_id) as order_cnt,
       round(count(*) / count(distinct order_id), 2) as items_per_order,
       sum(case when order_item_id is null then 1 else 0 end) as null_item_rows
from dwd.dwd_trade_order_detail_di where dt = '2026-10-01';
```

> [!tip] A1 的验收方式
> 让建模的人**不看文档**说一遍：「这张表一行是一个订单的一个商品，一个订单有 3 个商品就有 3 行。」
> 说得磕巴，就是没想清楚——这时候改文档比上线后改数据便宜 100 倍。

### 2.2 B 组：维度与关联

| # | 检查项 | 为什么重要 | 怎么检查 | 不合格后果 |
| --- | --- | --- | --- | --- |
| B1 | 维表关联键唯一 | 维表重复 = 事实表被放大 m 倍 | 维表 `group by 主键 having count(*)>1`；比对 join 前后行数 | 金额翻倍，大促后才暴露 |
| B2 | 维度口径一致 | 编码/层级不一致，join 不上或冲突 | 事实表编码 `left anti join` 维表 | 静默丢数，报表少一块 |
| B3 | 有未知成员 `-1` | 维表缺失时不能丢事实行 | 维表是否有 `-1` 行；关联后 null 占比 | 新商品/新渠道整行消失 |
| B4 | 退化维度合理 | 订单号这类无属性维度不该建维表 | 是否有只有 ID 的 `dim_order` | 多一次 join，无收益 |

```sql
-- B1 维表唯一（应为 0 行）
select sku_id, count(*) as cnt
from dim.dim_sku where dt = '2026-10-01'
group by sku_id having count(*) > 1;

-- B1 关联后不能放大：两个数字必须相等
select
  (select count(*) from dwd.dwd_trade_order_detail_di where dt = '2026-10-01') as fact_rows,
  (select count(*) from dwd.dwd_trade_order_detail_di f
   left join dim.dim_sku d on f.sku_id = d.sku_id and d.dt = '2026-10-01'
   where f.dt = '2026-10-01') as joined_rows;

-- B2 维度一致性：事实表里在维表找不到的编码
select f.province_code, count(*) as cnt
from dwd.dwd_trade_order_detail_di f
left anti join dim.dim_region r on f.province_code = r.province_code
where f.dt = '2026-10-01'
group by f.province_code order by cnt desc limit 20;
```

> [!warning] B1 是最容易被跳过的一项
> 所有人都会查「事实表主键唯一」，很少人查「维表主键唯一」。而金额翻倍事故里**维表重复占一半以上**——维表是外部同步进来的，双写、重推、全量与增量混跑都会产生重复。
> 防御：join 前对维表 `row_number() over (partition by 主键 order by update_time desc)` 去重 + 每日唯一性 DQ。

### 2.3 C 组：指标与口径

| # | 检查项 | 为什么重要 | 怎么检查 | 不合格后果 |
| --- | --- | --- | --- | --- |
| C1 | 有原子指标定义 | 没有分子/分母/过滤条件就无法复现 | 字典每行有「口径、分子、分母、时间口径、负责人」 | 两个人算出两个数 |
| C2 | 分子分母同源 | 分子来自订单、分母来自埋点，口径天然不一致 | 检查两个数是否同一张 DWS / 同一批用户 | 转化率 > 100% 或长期偏低 |
| C3 | 去重指标不跨维相加 | UV 类指标不可加 | `sum(各维 uv)` 与 `count(distinct)` 对比 | 分渠道 UV 相加是全站的 3 倍 |
| C4 | 退款/取消口径写死 | 「GMV 含不含退款」理解不同 | 字典里有「含退款/取消/未支付」三个开关 | 口径漂移，月度数字对不上 |

```sql
-- C3 去重指标不可加：sum_channel_uv 一定 ≥ real_uv，差额是跨渠道重复的人
select (select sum(uv) from dws.dws_traffic_channel_1d where dt = '2026-10-01') as sum_channel_uv,
       (select count(distinct user_id) from dwd.dwd_traffic_page_view_di
        where dt = '2026-10-01') as real_uv;

-- C4 双口径对比：把两种理解都算出来，差多少一目了然
select
  (select sum(pay_amount) from dwd.dwd_trade_order_pay_di where dt = '2026-10-01') as gmv_pay,
  (select sum(refund_amount) from dwd.dwd_trade_refund_di
   where dt = '2026-10-01' and refund_status = 'SUCCESS') as refund_amt,
  (select sum(pay_amount) from dwd.dwd_trade_order_pay_di where dt = '2026-10-01')
  - (select sum(refund_amount) from dwd.dwd_trade_refund_di
     where dt = '2026-10-01' and refund_status = 'SUCCESS') as gmv_net;
```

> [!important] 口径不是技术问题，是签字问题
> 技术只能保证「算得对」，保证不了「算的是大家要的」。C 组的通过标准不是「SQL 跑通」，而是**下游使用方在指标字典上签字**。没签字的指标一律标为「实验口径」，不进正式报表。

### 2.4 D 组：分区与存储

| # | 检查项 | 为什么重要 | 怎么检查 | 不合格后果 |
| --- | --- | --- | --- | --- |
| D1 | 分区键是业务日期 | 处理日期无法重跑/回补 | 对比 `dt` 与 `min/max(order_time)` | 补数写错分区，报表少一天 |
| D2 | 分区粒度匹配数据量 | 太细元数据爆炸，太粗单 task 跑不动 | 单分区压缩后是否 128MB~1GB | 编译几十秒，或一个 task 跑 3 小时 |
| D3 | 有生命周期策略 | 不删就无限涨，成本失控 | 元数据是否有 `retention_days`；最早分区是否超期 | 存储成本每年翻倍，没人敢删 |
| D4 | 小文件数量达标 | 小文件拖垮 NN 和 task 启动 | `hdfs dfs -count` 看 `FILE_COUNT` | 读 1GB 起 2000 个 task |

```sql
-- D1 分区语义：max_ot 应落在 dt 当天 23:59:59 之前
select dt, min(order_time) as min_ot, max(order_time) as max_ot, count(*) as cnt
from dwd.dwd_trade_order_detail_di where dt = '2026-10-01' group by dt;
```

```bash
# D2 / D4：单分区大小与文件数
hdfs dfs -du -s -h /warehouse/dwd.db/dwd_trade_order_detail_di/dt=2026-10-01
hdfs dfs -count    /warehouse/dwd.db/dwd_trade_order_detail_di/dt=2026-10-01
# 输出 DIR_COUNT FILE_COUNT CONTENT_SIZE；FILE_COUNT > 分区大小/128MB × 2 即小文件超标
```

### 2.5 E 组：幂等与可重跑

| # | 检查项 | 为什么重要 | 怎么检查 | 不合格后果 |
| --- | --- | --- | --- | --- |
| E1 | 用 `insert overwrite` 分区 | `insert into` 重跑必翻倍 | 全文搜 `insert into` | 每次重跑数据翻倍，无报错 |
| E2 | 重跑不影响其他日期 | Spark 默认 STATIC 覆盖全表 | 重跑 1 天后比对其他日期行数 | 修一天，毁一个月 |
| E3 | 有唯一性兜底 | 上游重复推送不能写出重复行 | 目标表主键重复检查是否为 0 | 上游抖一下，下游全错 |
| E4 | 有行数/金额波动监控 | 静默少 30% 数据没人发现 | 环比波动 SQL 是否接告警 | 数字错三天才被业务发现 |

```sql
-- E3 唯一性兜底：dup_cnt 必须为 0
select count(*) as dup_cnt from (
  select order_id, order_item_id, count(*) as c
  from dwd.dwd_trade_order_detail_di where dt = '2026-10-01'
  group by order_id, order_item_id having c > 1
) t;

-- E4 行数与金额环比：|rows_dod| > 20% 或 |amt_dod| > 15%（经验阈值）告警
with t as (
  select dt, count(*) as rows_cnt, sum(pay_amount) as amt
  from dwd.dwd_trade_order_detail_di
  where dt between date_sub('2026-10-01', 7) and '2026-10-01'
  group by dt
)
select dt, rows_cnt, amt,
       round((rows_cnt - lag(rows_cnt) over (order by dt))
             / lag(rows_cnt) over (order by dt), 4) as rows_dod,
       round((amt - lag(amt) over (order by dt))
             / lag(amt) over (order by dt), 4) as amt_dod
from t order by dt;
```

### 2.6 评审流程：文档 → 会 → 通过标准 → 回检

**① 设计文档模板**

```markdown
# 表设计：dwd_trade_order_detail_di
## 1. 业务背景  谁用、解决什么问题、不做会怎样（3 句话）
## 2. 业务过程  下单 / 支付 / 退款 / 加购 / 浏览（只选一个）
## 3. 粒度声明  一行 = 一个订单的一个商品（order_id + order_item_id 唯一）
## 4. 维度清单  用户 user_id→dim_user_scd2；商品 sku_id→dim_sku；订单 order_id（退化维度）
## 5. 事实与指标  pay_amount decimal(18,2)：实付 = 商品金额 - 优惠 + 运费分摊，可加
## 6. 分区与生命周期  dt 业务日期 yyyy-MM-dd；保留 3 年；热 30/温 150/冷归档
## 7. 更新方式  每日 insert overwrite 分区；重跑只影响当天
## 8. 上下游依赖  上游 ods_order_item；下游 dws_trade_user_1d、ads_gmv_d
## 9. 风险点  ① 退款状态不回刷，退款金额需 join 退款事实 ② 维表可能重复，join 前去重
```

**② 参与角色（缺一个就漏一类问题）**

| 角色 | 关心什么 | 能拦住什么 |
| --- | --- | --- |
| 建模 | 粒度、维度、事实 | A 组、B 组 |
| 开发 | 实现成本、依赖、调度 | D 组、E 组 |
| 下游使用方 | 口径是否符合业务理解 | C 组 |
| DQ | 校验规则、监控阈值 | B2、E4 |

**③ 通过标准**：20 项无 ❌（有 ⚠️ 必须写兜底方案和 owner）；指标字典有下游签字；校验 SQL 已进 DQ 平台，不停留在文档里。

**④ 上线后回检**：T+1 跑全量 20 项（自动化，有 ❌ 阻断下游）；T+7 看行数/金额曲线确认无缓慢漂移；T+30 与业务对数；每次口径变更重走 C 组并把变更记进字典版本历史。

### 2.7 三个「评审没拦住」的事故

**事故一：金额翻倍（维表重复）**——大促后 GMV 涨 80%，财务对账多出 3000 万。`dim_sku` 同步双写，同一 `sku_id` 两条，join 后商品被复制两份。
能拦住的是 **B1**（一条 `group by sku_id having count(*)>1` 就能发现）+ 关联后行数比对；没拦住是因为只查了事实表主键，DQ 也没有维表规则。定位：`count(*)` 与 `count(distinct 主键)` 不等 → 一定放大。

**事故二：拉链断裂（同一天多次变更）**——`dim_user_scd2` 某天同一 `user_id` 出现两条 `is_current=1`，历史订单关联后一个订单变两行，用户数虚高 12%。
能拦住的是 **A2**（拉链主键是 `user_id + start_date`）+ **E3** + 专项检查「`is_current=1` 每人一条」；没拦住是因为 `row_number` 只按 `user_id` 排序，同日两条 `start_date` 相同，`rn=1` 只去掉一条。

**事故三：口径漂移（GMV 换了定义）**——ADS 的 GMV 从支付口径改成下单口径没通知下游，月度会两套报表差 12%。
能拦住的是 **C1** + **C4** + 字典变更流程；没拦住是因为字典只写「GMV = 成交金额」，没写时间口径和是否含退款，改的人以为在「修 bug」。

---

## 三、动手实验

### 3.1 造一张「故意做错」的表

```sql
drop table if exists dwd.dwd_trade_order_bad;
create table dwd.dwd_trade_order_bad (
  order_id      bigint        comment '订单ID',
  order_item_id bigint        comment '明细ID（订单级行为空）',
  user_id       bigint        comment '用户ID',
  sku_id        bigint        comment '商品ID',
  pay_amount    decimal(18,2) comment '金额',
  order_time    string        comment '下单时间',
  etl_time      string        comment '跑批时间'
)
comment '反面教材：混粒度 + 处理日期分区 + insert into'
partitioned by (etl_date string comment '跑批日期')
stored as textfile;

-- ❌ 四个错误一次犯完：
-- ① left join 让没有明细的订单也进来（order_item_id 为 NULL）→ 混粒度
-- ② insert into → 重跑翻倍
-- ③ 分区用 etl_date（跑批日）→ 业务日期错位
-- ④ textfile 无压缩 → 小文件 + 存储浪费
insert into table dwd.dwd_trade_order_bad partition (etl_date = '2026-10-01')
select o.order_id, i.order_item_id, o.user_id, i.sku_id,
       coalesce(i.pay_amount, o.total_amount) as pay_amount,
       o.order_time, current_timestamp() as etl_time
from ods.ods_order o
left join ods.ods_order_item i on o.order_id = i.order_id
where o.dt = '2026-10-01';

-- 再造一个「维表双写」：同一 sku_id 两条
insert into dim.dim_sku partition (dt = '2026-10-01')
values (1001, '商品A', 10, '2026-10-01 00:00:00'),
       (1001, '商品A', 10, '2026-10-01 01:00:00'),
       (1002, '商品B', 20, '2026-10-01 00:00:00');
```

### 3.2 逐项跑检查，把问题全部找出来

```sql
-- ① A2/A3：非空、粒度
select count(*) as rows_cnt,
       sum(case when order_item_id is null then 1 else 0 end) as null_item_rows,
       round(count(*) / count(distinct order_id), 2) as items_per_order
from dwd.dwd_trade_order_bad where etl_date = '2026-10-01';

-- ② 金额守恒：dwd 应等于 ods，差额就是被放大的部分
select (select sum(pay_amount) from ods.ods_order_item where dt = '2026-10-01') as ods_amt,
       (select sum(pay_amount) from dwd.dwd_trade_order_bad where etl_date = '2026-10-01') as dwd_amt;

-- ③ D1：max_ot 比 etl_date 早一天 → etl_date 是跑批日
select etl_date, min(order_time) as min_ot, max(order_time) as max_ot
from dwd.dwd_trade_order_bad where etl_date = '2026-10-01' group by etl_date;

-- ④ B1：维表重复，join 会放大 2 倍
select sku_id, count(*) as cnt from dim.dim_sku
where dt = '2026-10-01' group by sku_id having count(*) > 1;
```

预期现象：`null_item_rows > 0`（混粒度）、`items_per_order ≈ 1.0`（订单级行混入）、`dwd_amt > ods_amt`（放大）、`max_ot` 比 `etl_date` 早 1 天（处理日期）、1001 出现 2 次（维表重复）。再把 3.1 的 `insert into` 执行一次，行数翻倍（非幂等）。

### 3.3 修正版

```sql
create table dwd.dwd_trade_order_detail_di (
  order_id      bigint        comment '订单ID',
  order_item_id bigint        comment '订单明细ID',
  user_id       bigint        comment '用户ID',
  sku_id        bigint        comment '商品ID',
  shop_id       bigint        comment '店铺ID',
  pay_amount    decimal(18,2) comment '实付金额',
  order_time    timestamp     comment '下单时间',
  etl_time      timestamp     comment 'ETL 写入时间（不参与分区）'
)
comment '订单明细事务事实表：一行 = 一个订单的一个商品'
partitioned by (dt string comment '业务日期 yyyy-MM-dd')
stored as parquet
tblproperties ('parquet.compression' = 'SNAPPY');

-- ✅ inner join 只取有明细的行 + 分区覆盖 + 业务日期分区
insert overwrite table dwd.dwd_trade_order_detail_di partition (dt = '2026-10-01')
select o.order_id, i.order_item_id, o.user_id, i.sku_id, o.shop_id,
       i.pay_amount, o.order_time, current_timestamp() as etl_time
from ods.ods_order o
join ods.ods_order_item i on o.order_id = i.order_id
where o.dt = '2026-10-01' and i.dt = '2026-10-01';
```

回跑 3.2 的检查，`dup_cnt`、`null_pk` 应为 0，`dwd_amt = ods_amt`，`max_ot` 落在 `dt` 当天——六项全绿才算过评审。

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| 评审会开成技术方案会 | 没有 Checklist，讨论发散 | 会前发文档 + 20 项清单 | 固定议程 A→B→C→D→E，每项 1 分钟 |
| 评审通过但上线仍翻倍 | 只审文档没跑检查 SQL | 评审现场跑 A2/B1 | 检查 SQL 进 DQ，不靠人记 |
| 口径每次开会都吵 | 字典没有签字栏 | 看是否有「负责人 + 版本」 | 口径变更版本化 + 通知下游 |
| 主键唯一但金额仍错 | 主键选错（用了自增 ID/代理键） | 用业务唯一键再查一次 | 主键必须是业务键组合 |
| 维表重复没人发现 | 只查事实表不查维表 | `group by 维表主键 having count(*)>1` | 维表加每日唯一性 DQ |
| 重跑后下游全错 | `insert into` 或 Spark STATIC 覆盖 | 重跑 1 天后比对其他日期 | 改 overwrite + dynamic |
| 20 项没人执行 | 清单太长、没有分级 | 统计哪几项真拦住过事故 | 分 P0（5 项）/ P1（其余） |
| 新人也说「评审过了」 | 结论没有记录 | 看是否有评审纪要和遗留项 | 纪要存档，遗留项有 owner |

---

## 五、面试追问

> [!question] 1. 你们建模评审怎么做？
> **一句话答法**：文档提前发 → 四个人过 20 项清单 → 有 ❌ 不放行 → 检查 SQL 进 DQ → T+1/T+7/T+30 回检。
> **追问陷阱**：「会不会流于形式」——会，除非清单可勾选、SQL 可执行、结论有记录，缺一样就退化。

> [!question] 2. 上线前你最看什么？
> **一句话答法**：粒度能否一句话说清、主键是否唯一非空、口径有没有下游签字。
> **追问陷阱**：「只能看一项」——看主键唯一性：一条 SQL 能跑，能挡住最高频的金额翻倍。

> [!question] 3. 怎么保证数据质量？
> **一句话答法**：事前评审 + 事中幂等去重 + 事后 DQ 与波动告警，只做事后等于天天救火。
> **追问陷阱**：「DQ 写多少条」——按「能拦住历史事故」定，每条有 owner 和处置动作。

> [!question] 4. 已上线表粒度错了怎么补救？
> **一句话答法**：评估影响面 → 建新表重刷并双跑 → 下游切视图过渡 → 旧表只读留 30 天 → 补评审项防复发。
> **追问陷阱**：「直接改原表行不行」——不行，下游同时在用，会让所有下游一起错且无法回滚。

> [!question] 5. 维表重复怎么发现、怎么预防？
> **一句话答法**：发现靠维表主键分组计数 + join 前后行数比对；预防靠 join 前 `row_number` 去重 + 每日 DQ。
> **追问陷阱**：「上游就是会重复推」——在 DWD 侧兜底，别指望上游保证，它保证不了。

> [!question] 6. 两个部门口径不一致怎么解？
> **一句话答法**：做成两个原子指标（不同名字）同时产出，让差异可见，再推动业务选正式口径。
> **追问陷阱**：「业务不肯统一」——可以并存，但必须命名区分，禁止同名不同义。

> [!question] 7. 幂等怎么保证？
> **一句话答法**：分区覆盖 + 业务日期分区 + 写入前去重 + 唯一性校验，三条缺一不可。
> **追问陷阱**：「Spark 有什么坑」——`partitionOverwriteMode` 默认 STATIC，动态分区写入会覆盖全表。

> [!question] 8. 业务要求「先上线再说」怎么办？
> **一句话答法**：分级放行——P0 五项必须过，其余可带 ⚠️ 上线但有 owner 和 deadline。
> **追问陷阱**：「P0 也不过」——风险写成书面结论 + 预写回滚方案。评审不是阻止上线，是让风险被看见。

> [!question] 9. 20 项太多，团队执行不下去怎么办？
> **一句话答法**：分级 + 自动化。P0 人工必查，其余脚本化，清单进 MR 模板，不勾完不能合并。
> **追问陷阱**：「自动化到什么程度」——能跑 SQL 的全自动，只有「粒度说清」「口径签字」必须人工。

> [!question] 10. 数据质量监控怎么做才不被忽略？
> **一句话答法**：告警少而准，每条带「影响哪个报表、谁负责、怎么处理」，并分阻断/警告两级。
> **追问陷阱**：「告警太多」——按历史事故反推规则，删掉从没触发过的；告警疲劳比没告警更危险。

---

## 六、自测题

> [!question] 1.（单选）上线前最应该优先检查的一项是？
> A. 表注释是否完整　B. 主键是否唯一且非空　C. 字段命名是否规范　D. 存储格式是否 parquet

> [!question] 2.（单选）维表关联键有重复，最直接的后果是？
> A. 查询变慢　B. 事实表行数和金额被放大　C. 分区裁剪失效　D. 小文件增多

> [!question] 3.（判断）只要 SQL 跑通、结果看起来合理，就可以上线。

> [!question] 4.（场景设计）同事提交了 `dwd_order_all`：字段有 `order_id, order_item_id, pay_time, refund_time, ship_time, pay_amount`，按 `etl_date` 分区，`insert into` 写入，关联 `dim_sku` 直接 `left join`。指出至少 5 个问题并对应到检查项。

> [!question] 5.（场景设计）一张已上线 3 个月的表被发现粒度错了（实际订单级，下游按明细级用）。给出完整补救方案：影响评估、技术步骤、下游沟通、防复发。

> [!success]- 答案与解析（点击展开）
> **1. B**
> 主键唯一非空是「金额不翻倍、聚合不重复」的前提，一条 SQL 就能验证，性价比最高。注释、命名、格式错了可以渐进修，主键错了只能重建。
>
> **2. B**
> 维表一条主键对应多行时，事实表每行被复制多份，`sum()` 翻倍、`count(*)` 虚高。查询慢和小文件是副作用，不是主要后果。
>
> **3. 错误**
> SQL 跑通只说明语法对，不说明语义对。粒度、主键、口径、幂等这四类问题都能「跑通但结果错」——这正是评审存在的理由。
>
> **4. 六个问题**
> | # | 问题 | 检查项 | 后果 |
> | 1 | 表名 `_all` 暗示什么都放，混了订单/支付/退款/履约四个过程 | A4 | 一行四个时间，指标按哪个算说不清 |
> | 2 | `pay_time`/`refund_time`/`ship_time` 同时存在且可空 | A4 | 状态互相矛盾（已退款却有 ship_time） |
> | 3 | 主键未声明，`order_id+order_item_id` 未验证唯一 | A2 | 重复行导致金额翻倍 |
> | 4 | 按 `etl_date` 分区 | D1 | 重跑/回补写错分区 |
> | 5 | `insert into` 写入 | E1 | 重跑翻倍 |
> | 6 | `dim_sku` 直接 `left join` 未去重 | B1 | 维表重复时金额翻倍 |
> 补充：`left join` 还会引入无明细的订单行（A3 混粒度）。
> 正确做法：拆成 `dwd_trade_order_detail_di`（下单）+ `dwd_trade_order_pay_di`（支付）+ `dwd_trade_refund_di`（退款）+ `dwd_trade_order_process_acc`（累积快照）。
>
> **5. 补救方案**
> **① 影响评估（半天）**：查血缘看哪些 DWS/ADS/报表在读；用 `count(*)` 与 `count(distinct order_id)` 的比例估算下游虚高多少；确认从哪天开始错、要回刷多少天。
> **② 技术步骤**：建正确粒度的新表（主键 `order_id+order_item_id`，业务日期分区）→ 按月分批 `insert overwrite` 回刷，每批校验行数金额 → 双跑 3~7 天逐日比对 → 下游切视图（`create view ... as select * from 新表`）→ 再切物理表 → 旧表只读留 30 天 → 确认无回滚后下线。
> **③ 下游沟通**：提前告知「数字会变、变多少、为什么变」，给新旧对照表；已发出的报表准备口径说明；切换窗口避开月初和大促。
> **④ 防复发**：把「粒度一句话 + 主键唯一」加进 MR 模板必填项；DQ 加主键唯一和 `count(*)/count(distinct 主键)` 波动阈值；复盘写清「哪一项 Checklist 能拦住」。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 上游 | [02-02 维度建模四步法](02-02%20%E7%BB%B4%E5%BA%A6%E5%BB%BA%E6%A8%A1%E5%9B%9B%E6%AD%A5%E6%B3%95.md) | A 组粒度声明来自四步法第二步 |
| 上游 | [02-04 三类事实表](02-04%20%E4%B8%89%E7%B1%BB%E4%BA%8B%E5%AE%9E%E8%A1%A8.md) | A4「只存一个业务过程」是三类事实表的分界 |
| 上游 | [02-05 维度表设计与缓慢变化维 SCD](02-05%20%E7%BB%B4%E5%BA%A6%E8%A1%A8%E8%AE%BE%E8%AE%A1%E4%B8%8E%E7%BC%93%E6%85%A2%E5%8F%98%E5%8C%96%E7%BB%B4%20SCD.md) | B1/B3 的维表唯一性与未知成员 |
| 上游 | [02-08 指标口径与指标体系](02-08%20%E6%8C%87%E6%A0%87%E5%8F%A3%E5%BE%84%E4%B8%8E%E6%8C%87%E6%A0%87%E4%BD%93%E7%B3%BB.md) | C 组四项全部来自指标字典 |
| 直接上游 | [02-10 分区策略与生命周期管理](02-10%20%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E4%B8%8E%E7%94%9F%E5%91%BD%E5%91%A8%E6%9C%9F%E7%AE%A1%E7%90%86.md) | D 组四项是 02-10 的检查版 |
| 直接下游 | [02-12 建模实战：电商订单域](02-12%20%E5%BB%BA%E6%A8%A1%E5%AE%9E%E6%88%98%EF%BC%9A%E7%94%B5%E5%95%86%E8%AE%A2%E5%8D%95%E5%9F%9F.md) | 步骤 9 的 6 段校验是本节 Checklist 的落地 |
| 延伸 | [00_模块3导航](../03%20%E6%A8%A1%E5%9D%973%20Spark%E8%B0%83%E4%BC%98%E3%80%81%E6%95%B0%E6%8D%AE%E8%B4%A8%E9%87%8F%E8%A1%80%E7%BC%98/00_%E6%A8%A1%E5%9D%973%E5%AF%BC%E8%88%AA.md) | DQ 平台、血缘、告警的工程化在模块 3 |

> [!tip] 学习建议
> 把 20 项压成一张 A4 贴在工位。前三个月逐项对照，三个月后你会发现自己在**设计阶段就能预判**这些问题——那时 Checklist 不再是清单，而是直觉。
