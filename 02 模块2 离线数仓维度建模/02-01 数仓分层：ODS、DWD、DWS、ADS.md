---
title: 02-01 数仓分层：ODS、DWD、DWS、ADS
module: 模块2 离线数仓维度建模
difficulty: ⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - 数仓
  - 建模
  - 分层
---

# 02-01 数仓分层：ODS、DWD、DWS、ADS

> [!abstract] 一句话结论
> 分层的本质是**数据加工流水线 + 责任边界**：每层只做一类加工，做错了能定位、改口径能重跑、下游能复用。决定成败的不是层数，而是**粒度纪律**——DWD 一旦聚合，更细的维度就永久失去。
> 面试不问「有几层」，问「为什么去掉 DWS」「DWD 能不能轻度聚合」「ADS 能不能直读 ODS」。

---

## 一、心智模型

### 1.1 原料仓 → 净菜加工 → 按菜系备料 → 摆盘上桌

| 后厨环节 | 对应层 | 这一步只做什么 | 谁在用 |
| --- | --- | --- | --- |
| 原料仓（带泥的土豆） | **ODS** | 收货、贴标签、入库，**不洗不切** | 只有后厨，客人不能进 |
| 净菜加工（洗、切、分装） | **DWD** | 清洗、标准化，按最小可用单元分装 | 各菜系灶台 |
| 按菜系备料 | **DWS** | 按「菜系 + 菜品」组合预配 | 具体档口 |
| 摆盘上桌 | **ADS** | 直接可上桌的成品 | 客人（业务方/BI） |

三条洞察：

1. **「不做什么」比「做什么」更重要**。原料仓提前削了土豆皮，三天后要用带皮土豆做另一道菜，就回不去了——这就是 ODS 不做业务清洗的原因。
2. **不能跳过中间层取原料**。灶台跑去原料仓搬整箱土豆，短期省事，长期没人知道这道菜用的是哪批货（血缘断裂）。
3. **层数是成本，不是荣耀**。四个环节刚好覆盖「原始 → 干净 → 可复用 → 可交付」，再拆就是加流程、加人、加扯皮。

### 1.2 分层的本质：责任边界，不是目录结构

不是「在 HDFS 上建四个文件夹」，而是四个**可验证的责任边界**：

| 层 | 责任 | 可验证的判据 | 违反了会怎样 |
| --- | --- | --- | --- |
| ODS | 忠实记录源系统长什么样 | 能 1:1 对上源表，不做业务判断 | 上游逻辑改了无法回溯，只能求业务方重导 |
| DWD | 把「一个业务事件」洗干净、放到最小粒度 | 一行 = 一个事件的最小单元 | 下游拆细分维度时拆不出来，只能回退重算 |
| DWS | 把常用维度组合预先算好 | 一行 = 一个维度组合，被 ≥2 个下游复用 | 过细等于没汇总，过粗只能服务一张报表 |
| ADS | 把应用要的数字摆好 | 一行 = 一个报表格子，业务方不用再算 | 业务方自己写 SQL，口径彻底失控 |

> [!important] 判断一张表该放哪层
> 不看表名，看**粒度**和**是否可复用**：与源系统同粒度、无业务判断 → ODS；业务过程最小单元、未聚合 → DWD；维度组合、可复用 → DWS；只服务一个应用的一行 → ADS。

---

## 二、核心知识点

### 2.1 ODS：贴源，只做「无损搬运」

**是什么**：与源系统表结构基本 1:1 的落地层，回答「源系统在某时刻到底长什么样」。

**允许做的四件事**（做完仍是无损的）：

| 动作 | 为什么允许 | 注意 |
| --- | --- | --- |
| 换存储格式（text → orc/parquet） | 只影响物理层 | 压缩比与列裁剪收益巨大，必做 |
| 加分区 `ds` | 物理组织，不改行内容 | `ds` 语义是**采集日期**，不是业务日期 |
| 加 `etl_time` 采集时间 | 多一列元数据，用于排查延迟 | 不要拿它替代业务时间 |
| 加 `md5(concat_ws(...))` 去重辅助列 | 只加列不删行 | 去重动作本身属于 DWD |

**禁止做的**：类型强转（`'1,000'` → NULL，原始信息丢失）、去重、过滤、join、枚举翻译、聚合。**一旦过滤，被丢掉的数据没有任何地方能找回来**。

**为什么禁止业务方直查**，四个理由缺一不可：

1. **性能**：全量原始数据，无预聚合，一张报表扫几十 TB
2. **口径**：有重复投递、未提交事务、测试数据，业务方算出来一定错
3. **权限**：手机号、身份证、地址等敏感字段，直开等于把合规风险外包
4. **稳定性**：源系统一改字段，所有直查报表同时挂，且没人知道谁在用

**覆盖写 vs 追加写**——ODS 最容易踩的坑：

| 源数据形态 | 写入方式 | 幂等 | 用错的后果 |
| --- | --- | --- | --- |
| 每日全量快照 | `insert overwrite ... partition(ds=...)` | 天然幂等 | 用 append → 每天叠一份全量，一月后膨胀 30 倍 |
| 每日增量 / binlog | `insert into ... partition(ds=...)` | 不幂等，需先清分区 | 用 overwrite → 覆盖历史增量，永久丢失 |

```sql
-- 增量表幂等的标准写法：先删分区再写
alter table ods.ods_order drop if exists partition (ds = '2026-10-02');
insert into table ods.ods_order partition (ds = '2026-10-02')
select * from staging.ods_order_stg where ds = '2026-10-02';
```

> [!danger] 一个真实事故形态
> binlog 增量表用 `insert overwrite` 写入且不分分区，某天调度重跑覆盖了前一天数据，下游重算后发现「历史订单全没了」，而备份是 7 天前的。

### 2.2 DWD：粒度 = 业务过程最小粒度，绝不做聚合

**是什么**：清洗后的明细层，回答「发生了什么业务事件，一件事一行」。

DWD 干四件事：**清洗**（去重、脏数据隔离、格式统一）、**规范化**（命名统一、枚举统一 `1/2/3` → `WAIT_PAY/PAID/CLOSED`、时间格式统一）、**维度关联宽表化**（冗余用户名、商品名、类目，让下游少 join）、**不改粒度**。

**为什么绝不做聚合**——本节最重要的一条：

```sql
-- ❌ 一旦这么做，这张表就死了
create table dwd.dwd_trade_order_agg as
select order_id, user_id, shop_id, sum(item_amount) as pay_amount
from ods.ods_order_item
group by order_id, user_id, shop_id;
```

三个月后运营要「按类目看销量」，这张表里**没有 sku_id**，只剩两条路：回退到 ODS 重算（分层白做，ODS 可能已清理），或让运营「大概看一下」（信任崩塌）。

更糟的是**不可逆**：7 个商品合成 1 个数，永远无法还原是哪 7 个。聚合是有损压缩，而数仓的核心价值是「随时换一个角度重看」。

> [!important] 判断一张 DWD 有没有偷偷聚合
> ① 一行代表什么？答「多个业务事件」就已经聚合了。② 有没有只可能由多行汇总得来的字段（`sum`/`count`/`avg`）？有就是聚合。③ 下游按更细维度拆得出来吗？拆不出来就是聚合了。

**允许的例外**（不改粒度就不算聚合）：去重（`row_number` 取最新，行数变少但每行仍是完整事件）、关联唯一维表（行数不变）、打平（一单 + N 明细 → N 行宽明细，这是**细化**）。

> [!warning] 订单粒度表放 DWD 还是 DWS
> `pay_amount` 是交易系统订单主表**自带**的 → `dwd_trade_order` 合法，它只是另一个粒度的原始事实。
> `pay_amount` 是你 `sum(item_amount)` **算出来**的 → 本质是 DWS，应改名 `dws_trade_order_1d`。
> 区别不在表名，在「这个数是抄来的还是算出来的」。

### 2.3 DWS：按「主题 + 维度组合」轻度汇总

**是什么**：按主题组织的汇总层，**粒度 = 维度组合**。命名 `dws_<数据域>_<主题>_<粒度>_<周期>`：

```
dws_trade_shop_day     -- 店铺 + 天
dws_trade_user_day     -- 用户 + 天
dws_trade_sku_day      -- 商品 + 天
```

**为什么必须有这一层**：

1. **复用**：`dws_trade_shop_day` 同时服务店铺日报、店铺排行、同比环比、商家后台四张报表；没有它，改口径要改四处
2. **减少重复 join**：DWD 算一个指标可能 join 8 张维表，DWS 做一次并物化
3. **性能**：行数比 DWD 小 1~2 个数量级（650 万明细 → 3 万店铺日汇总），分钟级降到秒级

**粒度取舍**是 DWS 唯一的设计难点：

| 粒度 | 后果 | 适用 |
| --- | --- | --- |
| 过细（店铺+商品+小时+渠道+新老客） | 行数接近 DWD，等于没汇总还多一份存储 | 几乎没有 |
| 过粗（只按天） | 只能服务大盘日报，店铺维度拆不出来 | 只有一个固定报表 |
| 刚好（店铺+天） | 覆盖 80% 店铺分析，行数收敛两个数量级 | 最常见 |

> [!tip] 怎么定粒度
> 把未来半年**已知报表需求**列出来，找维度交集，交集就是粒度。不要为「可能会有的需求」设计——需求来了再加一张表，成本远低于维护一张没人用的超细表。

### 2.4 ADS：面向应用的指标结果表

**是什么**：直接对接应用的结果层，**一行 = 报表上一个格子 / 接口一个字段**。

- **可宽可窄**：`ads_gmv_day` 5 个字段；`ads_shop_dashboard` 80 个字段横铺
- **可以反范式**：维度名直接写死成字符串，前端拿来即用
- **可以含派生指标**：同比、环比、排名、占比、累计，这些需要跨期对比，放 ADS 最合适

**ADS 与 DWS 的边界**，一句话：DWS 回答「这个维度组合下的原子指标是多少」，ADS 回答「这个报表要展示什么」。

例：`dws_trade_shop_day` 有 `gmv`/`order_cnt`/`buyer_cnt`（原子，可复用）；`ads_shop_rank_day` 有 `rank`/`gmv_yoy`/`gmv_share`（派生，需跨行跨期）。

> [!warning] ADS 三个禁忌
> ① 不要被 DWS 反向引用（ADS 是最下游）。② 不要在 ADS 做多表 join，输入应只有 DWS 和 DIM，超过 3 张表说明粒度没设计好。③ 不要指望 ADS 复用，两个应用要同一份数据应该在 DWS 复用。

### 2.5 DIM：不属于四层流水线，是横向共享的

**这是最多人讲不清的一点**。ODS→DWD→DWS→ADS 是**纵向流水线**（数据一天天往下流），DIM 是**横向的**：它不在这条流水线的任何一级，而是被每一级同时引用。

```
        ┌──────────────────────────────────────┐
        │  DIM（dim_user / dim_sku / dim_shop） │
        └───┬──────────┬──────────┬────────────┘
            ▼          ▼          ▼
   ODS ──→ DWD ──→ DWS ──→ ADS
```

**为什么必须横向**：若每个 DWD 各自维护一份「商品名称」，商品改名后 A 表新名、B 表旧名，join 出来的报表自相矛盾。维度一致性是 [02-07 一致性维度与总线矩阵](02-07%20%E4%B8%80%E8%87%B4%E6%80%A7%E7%BB%B4%E5%BA%A6%E4%B8%8E%E6%80%BB%E7%BA%BF%E7%9F%A9%E9%98%B5.md) 的核心，DIM 就是它的物理落点。

四个特点：**全公司一份**（不允许交易域、流量域各建一个）、**有代理键**（`sku_key` 而非 `sku_id`，支持 SCD2 多版本）、**有生命周期**（改名、换类目、关店，见 [02-05 维度表设计与缓慢变化维 SCD](02-05%20%E7%BB%B4%E5%BA%A6%E8%A1%A8%E8%AE%BE%E8%AE%A1%E4%B8%8E%E7%BC%93%E6%85%A2%E5%8F%98%E5%8C%96%E7%BB%B4%20SCD.md)）、**可被 DWD 冗余**（宽表化抄字段允许，但源头仍是 DIM）。

> [!note] DIM 放哪一层
> 物理上和 DWD 平级（`dim` 库），逻辑上是「共享资源」而非「流水线一级」。
> 面试问「你们几层」，答「ODS/DWD/DWS/ADS 四层 + 独立 DIM 维度层」，比答「五层」准确。

### 2.6 DWM / 中间层：阿里体系里的位置

阿里 OneData 的链路是 `ODS → DWD → DWM → DWS → ADS`。**DWM 是介于明细和汇总之间的中间层宽表**：粒度仍是明细级，但字段更宽、join 已做完。

**需要 DWM 的场景**：多个 DWS 共享**同一批复杂 join**（10 张表 join 出交易宽表）；这份 join **计算成本很高**（大表 join 跑 2 小时）；有 ≥3 个下游。

**过度设计的场景**：只有 2 张 DWS 用它；join 逻辑很简单（3 张表内，直接写在 DWS 里优化器还能做谓词下推）；团队只有 3 个人，维护 5 层的元数据/血缘/调度，人力成本超过收益。

> [!tip] 一句话
> **DWM 是「物化的 CTE」**：有 ≥3 个下游复用才建，没复用就是纯负债。
> 被问「你们有没有 DWM」，答「没有，复用靠 CTE，只有 XX 场景才物化」比硬凑五层专业。

### 2.7 分层的 5 个收益，各配一个反例

**收益 1：复用**
正例：`dws_trade_shop_day` 被 4 张报表共用，改一次口径 4 张同时生效。
反例：4 张报表各自从 ODS join 6 张表，运营说「GMV 要剔除运费」，你改 4 处 SQL，漏一处就出现两个 GMV。

**收益 2：血缘清晰**
正例：`ads_gmv_day.gmv` 能一路追到 `ods_order.pay_amount`，出问题 10 分钟定位。
反例：ADS 直连 ODS，源系统把 `pay_amount` 拆成 `goods_amount + freight_amount`，凌晨 12 张报表同时挂，没人知道谁依赖它。

**收益 3：问题隔离定位**
正例：指标不对，查 ADS（对 → 问题在下面）→ 查 DWS → 查 DWD → 查 ODS，三段二分。
反例：一条 300 行 SQL 从 ODS 直算到报表，数字不对，只能 `select * limit 10` 一段段加着调。

**收益 4：口径统一**
正例：GMV 定义写在表注释和 [02-08 指标口径与指标体系](02-08%20%E6%8C%87%E6%A0%87%E5%8F%A3%E5%BE%84%E4%B8%8E%E6%8C%87%E6%A0%87%E4%BD%93%E7%B3%BB.md) 的口径文档里，全公司一份。
反例：交易团队算 GMV 含运费、财务不含，月度会差 3%，吵三天发现是两套 SQL。

**收益 5：成本可控**
正例：报表查 DWS（3 万行），秒级返回。
反例：20 张报表每天各扫一遍 50TB 的 ODS 明细，集群 80% 算力花在重复扫描。

> [!important] 代价也要说清楚
> 多一层 = 多一份存储 + 多一段调度延迟 + 多一个故障点。
> 「要不要多加一层」的正确答案永远是：**复用收益是否大于这三项成本**，而不是「阿里这么分所以我们也这么分」。

### 2.8 分层误区（重点）

#### 误区 1：DWD 做聚合，导致下游不可复用

最致命。识别信号：**DWD 表名带 `_agg`/`_sum`/`_1d` 后缀**，基本可判违规。修复路径是把聚合下沉到 DWS、DWD 恢复明细；若历史已聚合，只能从 ODS 重跑——所以发现得越晚代价越大。

#### 误区 2：ODS 做复杂清洗，导致无法回溯

```sql
-- ❌ 在 ODS 就做业务判断
insert overwrite table ods.ods_order partition(ds='2026-10-02')
select order_id, user_id,
       case when status = 1 then 'PAID' else 'OTHER' end as status,   -- 语义翻译
       pay_amount
from staging.order_stg
where pay_amount > 0;   -- 过滤！负金额（退款冲正）被永久丢弃
```

半年后财务对账，发现冲正订单在数仓里根本不存在。

#### 误区 3：ADS 直接读 ODS，跨层

```sql
-- ❌ 报表直查 ODS
select to_date(create_time) as dt, sum(pay_amount)
from ods.ods_order where order_status = 'PAID' group by to_date(create_time);
```

三个后果：血缘图上 ADS 直连 ODS，中间层成装饰品；重复投递没去重，GMV 天生偏高；这张报表被认可后，第二张第三张都会这么写，分层瓦解。
**判断方法**：画血缘图，出现跨层边（ADS→ODS、ADS→DWD 明细）即违规。

#### 误区 4：层数堆到 6~7 层

见过最夸张的：`ODS → DWD → DWB → DWM → DWS → DWT → ADS`。后果：调度 7 级串行，任一级延迟 1 小时报表就晚 7 小时；同一份数据存 7 遍；血缘图像迷宫；每层都写一遍清洗逻辑。
**经验值**：4 层（+DIM）覆盖 95% 场景，超过 5 层要有能写进文档的理由。

#### 误区 5：DWS 粒度过细，等于没汇总

```sql
-- ❌ 这个粒度基本等于 DWD
create table dws.dws_trade_shop_sku_hour_day as
select shop_id, sku_id, hour(create_time) as hr, dt, sum(pay_amount) as gmv
from dwd.dwd_trade_order_detail
group by shop_id, sku_id, hour(create_time), dt;
```

行数只比 DWD 少一点，却多一份存储、多一层调度，下游还要再聚合。**这叫「为了分层而分层」**。

#### 误区 6：DWD 完全不清洗，把脏数据往下传

和误区 2 相反的极端。重复投递还在，DWS 的 `sum` 直接翻倍；枚举有 `1/PAID/已支付` 三种写法，`where status='PAID'` 漏掉三分之二。**清洗是 DWD 的价值所在，不是可选项**。

#### 误区 7：只分目录不分责任

表名带 `dwd_` 内容却是聚合结果，表名带 `ads_` 却被 `dws_` 引用。这种「命名分层」比不分层更危险，因为它给人错误预期。**检验**：抽 5 张表让新人只看表名猜粒度，猜错超过 2 张说明分层名存实亡。

### 2.9 实时链路的分层映射（一句话）

Flink 实时链路同样分层，只是介质变了：`ODS`（Kafka/binlog）→ `DWD`（双流 join 后的明细宽表，落 Kafka 或 Hudi/Iceberg）→ `DWM`（可选）→ `DWS`（轻度汇总，落 ClickHouse/Doris），ADS 往往由 DWS 直接对外提供。**分层思想不变，变的只是每层落在哪**。

---

## 三、动手实验

用订单域造一条完整链路，观察「行数收敛」和「不分层的重复成本」。

### 3.1 建表

```sql
create database if not exists ods;  create database if not exists dwd;
create database if not exists dws;  create database if not exists ads;
create database if not exists dim;

-- ODS：贴源，只加分区（ds = 采集日期）和采集时间
drop table if exists ods.ods_order;
create table ods.ods_order (
    order_id     string        comment '订单号',
    user_id      bigint        comment '用户ID',
    shop_id      string        comment '店铺ID',
    order_status string        comment '源端原始状态值',
    pay_amount   decimal(16,2) comment '实付金额',
    create_time  string        comment '下单时间',
    pay_time     string        comment '支付时间，未支付为 NULL',
    update_time  string        comment '源端更新时间',
    etl_time     string        comment '采集时间'
) partitioned by (ds string comment '采集日期') stored as orc;

drop table if exists ods.ods_order_item;
create table ods.ods_order_item (
    order_id    string        comment '订单号',
    sku_id      string        comment '商品ID',
    qty         int           comment '数量',
    item_amount decimal(16,2) comment '明细金额',
    etl_time    string        comment '采集时间'
) partitioned by (ds string comment '采集日期') stored as orc;

-- DIM：横向共享
drop table if exists dim.dim_sku;
create table dim.dim_sku (
    sku_id string, sku_name string, cat1_name string, brand_name string
) stored as orc;

-- DWD：粒度 = 一个订单的一个商品
drop table if exists dwd.dwd_trade_order_detail;
create table dwd.dwd_trade_order_detail (
    order_id     string        comment '订单号',
    sku_id       string        comment '商品ID',
    user_id      bigint        comment '用户ID',
    shop_id      string        comment '店铺ID',
    order_status string        comment '规范化后的状态',
    qty          int           comment '数量',
    item_amount  decimal(16,2) comment '明细金额',
    sku_name     string        comment '冗余：商品名',
    cat1_name    string        comment '冗余：一级类目',
    create_time  string        comment '下单时间',
    pay_time     string        comment '支付时间'
) partitioned by (dt string comment '业务日期 = 下单日期') stored as orc;

-- DWS：粒度 = 店铺 + 天
drop table if exists dws.dws_trade_shop_day;
create table dws.dws_trade_shop_day (
    shop_id        string        comment '店铺ID',
    order_cnt      bigint        comment '下单订单数',
    paid_order_cnt bigint        comment '支付订单数',
    gmv            decimal(20,2) comment 'GMV = 支付订单实付金额之和',
    buyer_cnt      bigint        comment '下单买家数',
    sku_cnt        bigint        comment '动销商品数'
) partitioned by (dt string comment '业务日期 = 下单日期') stored as orc;

-- ADS：粒度 = 天（大盘）
drop table if exists ads.ads_gmv_day;
create table ads.ads_gmv_day (
    dt               string        comment '业务日期',
    gmv              decimal(20,2) comment 'GMV',
    paid_order_cnt   bigint        comment '支付订单数',
    buyer_cnt        bigint        comment '支付买家数',
    avg_order_amount decimal(20,2) comment '客单价 = GMV / 支付订单数'
) stored as orc;
```

### 3.2 造数

```sql
-- O1001 有两条（模拟 binlog 重复投递），O1004 在 10-04 才采集到
insert into table ods.ods_order partition (ds = '2026-10-02') values
('O1001', 1, 'S01', 'PAID',     200.00, '2026-10-01 10:00:00', '2026-10-01 10:05:00', '2026-10-01 10:05:00', '2026-10-02 01:00:00'),
('O1001', 1, 'S01', 'PAID',     200.00, '2026-10-01 10:00:00', '2026-10-01 10:05:00', '2026-10-01 11:00:00', '2026-10-02 01:00:00'),
('O1002', 2, 'S01', 'PAID',     150.00, '2026-10-01 23:50:00', '2026-10-02 00:10:00', '2026-10-02 00:10:00', '2026-10-02 01:00:00'),
('O1003', 1, 'S02', 'WAIT_PAY',  80.00, '2026-10-01 15:00:00', null,                  '2026-10-01 15:00:00', '2026-10-02 01:00:00');

insert into table ods.ods_order partition (ds = '2026-10-03') values
('O1005', 2, 'S03', 'CLOSED',    50.00, '2026-10-02 11:00:00', null,                  '2026-10-02 11:00:00', '2026-10-03 01:00:00');

insert into table ods.ods_order partition (ds = '2026-10-04') values
('O1004', 3, 'S02', 'PAID',     300.00, '2026-10-01 20:00:00', '2026-10-03 09:00:00', '2026-10-03 09:00:00', '2026-10-04 01:00:00');

insert into table ods.ods_order_item partition (ds = '2026-10-02') values
('O1001', 'SKU-A', 1, 120.00, '2026-10-02 01:00:00'),
('O1001', 'SKU-B', 1,  80.00, '2026-10-02 01:00:00'),
('O1002', 'SKU-C', 2, 150.00, '2026-10-02 01:00:00'),
('O1003', 'SKU-D', 1,  80.00, '2026-10-02 01:00:00'),
('O1005', 'SKU-G', 1,  50.00, '2026-10-03 01:00:00');

insert into table ods.ods_order_item partition (ds = '2026-10-04') values
('O1004', 'SKU-E', 1, 200.00, '2026-10-04 01:00:00'),
('O1004', 'SKU-F', 1, 100.00, '2026-10-04 01:00:00');

insert into dim.dim_sku values
('SKU-A','手机壳','数码','A品牌'), ('SKU-B','钢化膜','数码','A品牌'),
('SKU-C','T恤','服饰','B品牌'),    ('SKU-D','牛仔裤','服饰','B品牌'),
('SKU-E','耳机','数码','C品牌'),   ('SKU-F','充电线','数码','C品牌'),
('SKU-G','袜子','服饰','B品牌');
```

> [!note] 版本差异
> 多行 `values` 在 Hive 3.x / Spark 3.5 可直接跑；若环境不支持，改用 `union all` 拼 `select`。**待验证**：以自己环境实测为准。

### 3.3 DWD：去重 + 规范化 + 宽表化（不改粒度）

```sql
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

insert overwrite table dwd.dwd_trade_order_detail partition (dt)
select o.order_id, i.sku_id, o.user_id, o.shop_id,
       case o.order_status when 'WAIT_PAY' then 'WAIT_PAY'
                           when 'PAID'     then 'PAID'
                           when 'CLOSED'   then 'CLOSED'
                           else 'UNKNOWN' end        as order_status,
       i.qty, i.item_amount, s.sku_name, s.cat1_name,
       o.create_time, o.pay_time,
       to_date(o.create_time)                        as dt
from (
    -- 去重：同一 order_id 只留源端最新一条（不改变「一订单」语义）
    select * from (
        select *, row_number() over (partition by order_id order by update_time desc) as rn
        from ods.ods_order where ds >= '2026-10-02'
    ) t where rn = 1
) o
join (select order_id, sku_id, qty, item_amount
      from ods.ods_order_item where ds >= '2026-10-02') i
  on o.order_id = i.order_id
left join dim.dim_sku s on i.sku_id = s.sku_id;
```

检查粒度有没有被破坏：

```sql
select dt, count(*) as rows, count(distinct order_id) as orders, sum(item_amount) as amount
from dwd.dwd_trade_order_detail group by dt;
```

预期：`2026-10-01` 6 行 / 4 单 / 730.00；`2026-10-02` 1 行 / 1 单 / 50.00。**一个订单 2 个商品就是 2 行**。

### 3.4 DWS：按店铺 + 天汇总

```sql
insert overwrite table dws.dws_trade_shop_day partition (dt)
select shop_id,
       count(distinct order_id)                                          as order_cnt,
       count(distinct case when order_status='PAID' then order_id end)   as paid_order_cnt,
       sum(case when order_status='PAID' then item_amount else 0 end)    as gmv,
       count(distinct user_id)                                           as buyer_cnt,
       count(distinct sku_id)                                            as sku_cnt,
       dt
from dwd.dwd_trade_order_detail
group by shop_id, dt;
```

> [!warning] 两个必须用 `distinct` 的坑
> ① `count(distinct order_id)` 而不是 `count(*)`——一行是「订单+商品」，一单 2 个商品会算成 2 单。
> ② `count(distinct user_id)` 而不是 `count(user_id)`——同一用户下 3 单只算 1 个买家。
> 这是 DWS 从明细汇总时**最高频的错误**，比语法错误隐蔽得多。

### 3.5 ADS：大盘 GMV

```sql
insert overwrite table ads.ads_gmv_day
select dt, sum(gmv) as gmv, sum(paid_order_cnt) as paid_order_cnt, sum(buyer_cnt) as buyer_cnt,
       round(sum(gmv) / nullif(sum(paid_order_cnt), 0), 2) as avg_order_amount
from dws.dws_trade_shop_day
group by dt;
```

**预期结果**：

| dt | gmv | paid_order_cnt | buyer_cnt | avg_order_amount |
| --- | --- | --- | --- | --- |
| 2026-10-01 | 650.00 | 3 | 3 | 216.67 |
| 2026-10-02 | 0.00 | 0 | 0 | NULL |

**逐层行数收敛**：

| 层 | 表 | 行数 | 说明 |
| --- | --- | --- | --- |
| ODS | ods_order + ods_order_item | 6 + 7 = 13 | 含 1 条重复投递 |
| DWD | dwd_trade_order_detail | 7 | 去重后 5 单的明细 |
| DWS | dws_trade_shop_day | 3 | 3 个「店铺+天」 |
| ADS | ads_gmv_day | 2 | 2 天 |

13 → 7 → 3 → 2。**收敛比就是查询加速比**，也是分层最直观的收益。

### 3.6 对比实验：不分层直接算的重复成本

三张报表（大盘日报、店铺日报、类目日报），不分层方案里每张都要**重写一遍去重逻辑**：

```sql
-- 报表1：大盘
select to_date(o.create_time) as dt, sum(o.pay_amount) as gmv
from (select * from (select *, row_number() over (partition by order_id order by update_time desc) rn
                     from ods.ods_order where ds >= '2026-10-02') t where rn = 1) o
where o.order_status = 'PAID' group by to_date(o.create_time);

-- 报表2：店铺（同样的去重，第二遍）
select o.shop_id, to_date(o.create_time) as dt, sum(o.pay_amount) as gmv
from (select * from (select *, row_number() over (partition by order_id order by update_time desc) rn
                     from ods.ods_order where ds >= '2026-10-02') t where rn = 1) o
where o.order_status = 'PAID' group by o.shop_id, to_date(o.create_time);

-- 报表3：类目（去重第三遍 + join 明细和维表）
select s.cat1_name, to_date(o.create_time) as dt, sum(i.item_amount) as gmv
from (select * from (select *, row_number() over (partition by order_id order by update_time desc) rn
                     from ods.ods_order where ds >= '2026-10-02') t where rn = 1) o
join ods.ods_order_item i on o.order_id = i.order_id
left join dim.dim_sku s on i.sku_id = s.sku_id
where o.order_status = 'PAID' group by s.cat1_name, to_date(o.create_time);
```

| 对比项 | 不分层 | 分层 |
| --- | --- | --- |
| 扫描表次数（3 张报表） | 3 × (ods_order + ods_order_item + dim_sku) | 3 次读小表（dws 3 行 / dwd 7 行） |
| 去重逻辑份数 | 3 份，改口径要改 3 处 | 1 份，在 DWD |
| 新增第 4 张报表 | 再写一遍去重 + join | 加 10 行 SQL |
| 口径一致性 | 靠人肉对齐，容易漂移 | 物理上只有一份 GMV 定义 |
| 明细量涨 10 倍后 | 每张报表都慢 10 倍 | DWD 慢 10 倍，DWS/ADS 不变 |

### 3.7 口径实验：GMV 按哪个日期归属

`dws_trade_shop_day` 按 `dt = 下单日期` 分区，O1002（10-01 下单、10-02 支付）被算进 10-01。财务若要求按支付日期：

```sql
select to_date(pay_time) as pay_dt, sum(item_amount) as gmv
from dwd.dwd_trade_order_detail where order_status='PAID' group by to_date(pay_time);
```

| 口径 | 10-01 | 10-02 | 10-03 |
| --- | --- | --- | --- |
| 按下单日期 | 650.00 | 0.00 | 0.00 |
| 按支付日期 | 200.00 | 150.00 | 300.00 |

**总数都是 650，每天的分布完全不同**。这就是「同一个 GMV 两个部门对不上」的真相：不是谁算错，是**归属日期不同**。

> [!important] 生产上怎么解
> ① 口径写进文档，见 [02-08 指标口径与指标体系](02-08%20%E6%8C%87%E6%A0%87%E5%8F%A3%E5%BE%84%E4%B8%8E%E6%8C%87%E6%A0%87%E4%BD%93%E7%B3%BB.md)。② DWS 同时出 `dt`（下单）和 `pay_dt`（支付）两个分区字段让下游选，而不是让下游重算。③ ADS 表名带口径暗示，如 `ads_gmv_day_by_paydt`，不要两个口径都叫 `ads_gmv_day`。

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| 报表数字比业务系统大 2 倍 | ODS 有重复投递，DWD 没去重 | `group by order_id having count(*)>1` | DWD 用 `row_number` 按 `update_time desc` 去重 |
| 历史某天突然少一半 | ODS 用 `overwrite` 写增量，覆盖历史 | 对比 `show partitions` 与行数趋势 | 改 `insert into`，写前 `drop partition` 保幂等 |
| 退款冲正订单在数仓找不到 | ODS 过滤了 `pay_amount <= 0` | 拿源库主键到 ODS 反查 | ODS 不过滤，脏数据打标记，DWD 分流到异常表 |
| 下游要按商品分析但 DWD 没 sku | DWD 提前聚合 | 看 DWD 是否有 `sum`/`count` 字段 | 从 ODS 重跑明细，聚合下沉 DWS |
| 指标对不上但两层各自都对 | DWS 用 `count(*)` 当订单数 | 对比 `count(*)` 与 `count(distinct order_id)` | 汇总时对实体一律 `count(distinct)` |
| 报表 SQL 扫了 30TB | ADS 直连 ODS | 看执行计划扫描量与血缘图 | 补 DWS，ADS 只读 DWS |
| 调度链路跑 8 小时 | 分了 7 层，逐层串行 | 看 DAG 关键路径 | 砍无复用的中间层，合并 DWM |
| `dwd_` 表被 `dws_` 引用后又聚合一次 | 只分目录不分责任 | 抽样检查表名与粒度是否匹配 | 建表评审，见 [02-11 建模评审 Checklist](02-11%20%E5%BB%BA%E6%A8%A1%E8%AF%84%E5%AE%A1%20Checklist.md) |
| 分区没数据但任务显示成功 | 上游延迟，ODS 空分区下游空跑 | 校验 ODS 分区行数 > 0 | 加质量校验，空分区阻断下游 |
| 维度改名后历史报表全变了 | DWD 冗余维度名，DIM 更新覆盖历史 | 对比同一指标前后两天的值 | 维度走 SCD2，见 [02-05 维度表设计与缓慢变化维 SCD](02-05%20%E7%BB%B4%E5%BA%A6%E8%A1%A8%E8%AE%BE%E8%AE%A1%E4%B8%8E%E7%BC%93%E6%85%A2%E5%8F%98%E5%8C%96%E7%BB%B4%20SCD.md) |

---

## 五、面试追问

> [!question] 1. 为什么要分层？
> **一句话答法**：解决四个问题——复用（一份汇总服务多张报表）、血缘（出问题能定位到层）、隔离（改一层不影响其他层）、成本（避免每张报表重复扫全量）。
> **追问陷阱**：「不分层也能跑，为什么要多花存储」——要答**代价对比**：多一层存储是线性成本，重复计算和口径漂移是超线性成本，报表越多差距越大。

> [!question] 2. 能不能少一层？比如去掉 DWS？
> **一句话答法**：能，当且仅当没有 ≥2 个下游复用同一份汇总。只有一张报表时 DWS 是纯开销，直接从 DWD 出 ADS 更合理。
> **追问陷阱**：「怎么判断有没有复用」——看未来半年报表清单的维度交集是否稳定。答「拍脑袋觉得以后会用」是减分项。

> [!question] 3. DWD 到底要不要轻度聚合？
> **一句话答法**：不要。粒度必须是业务过程最小粒度，聚合有损且不可逆，一旦聚合，下游更细的维度分析全部做不了。
> **追问陷阱**：「性能怎么办，每次扫明细很慢」——性能靠 DWS 解决，不靠 DWD 聚合；DWS 覆盖不了说明粒度设计有问题，应该加一张 DWS。
> **二次追问**：「订单主表自带的 `pay_amount` 算聚合吗」——不算，那是源系统原始字段，不是 `sum` 出来的。

> [!question] 4. ODS 该不该清洗？
> **一句话答法**：不做业务清洗，只做无损加工（换列存、加分区、加采集时间）。ODS 的价值是可回溯，一旦过滤或翻译，丢掉的信息找不回来。
> **追问陷阱**：「那脏数据怎么办」——DWD 分流：合法的进主表，异常的进 `dwd_xxx_err` 并告警，而不是在 ODS 删掉。

> [!question] 5. DWS 和 ADS 的边界怎么划？
> **一句话答法**：DWS 出**原子指标**（可被多张报表复用的度量），ADS 出**派生指标和展示形态**（同比、环比、排名、占比、宽表拼接）。
> **追问陷阱**：「同比环比为什么不能放 DWS」——同比需跨期数据，DWS 一行只有当期；放 DWS 会导致每加一个时间窗口就重刷历史分区。

> [!question] 6. 宽表放哪一层？
> **一句话答法**：看是「明细宽」还是「汇总宽」。明细粒度宽表放 DWD（join 完维度，行数不变），汇总粒度宽表放 DWS 或 ADS。
> **追问陷阱**：「DWD 宽表化和 DWS 区别」——DWD 宽表化不减行数，只是把 join 提前；DWS 一定减行数。**行数有没有变是唯一判据**。

> [!question] 7. 分层的代价是什么？
> **一句话答法**：三项——存储（同一份数据存多遍）、延迟（串行调度每层加时长）、维护（每层都要元数据、血缘、监控、重跑脚本）。
> **追问陷阱**：「怎么量化」——算「复用次数 × 单次节省的扫描量」对比「多一份存储 + 一段调度时长」，复用 < 2 次的层基本不划算。

> [!question] 8. ODS 用覆盖写还是追加写？
> **一句话答法**：看源数据形态。每日全量快照用 `insert overwrite partition`（天然幂等），增量/binlog 用 `insert into partition`（写前先 `drop partition` 保证幂等）。
> **追问陷阱**：「增量表能不能 overwrite」——不能，会覆盖历史增量。这是最典型的数据丢失事故，往往在重跑时才暴露。

> [!question] 9. 实时链路怎么分层？
> **一句话答法**：ODS（Kafka/binlog）→ DWD（双流 join 明细宽表）→ DWM（可选）→ DWS（轻度汇总，落 OLAP 引擎），ADS 通常由 DWS 直接对外提供。
> **追问陷阱**：「实时需要 DWD 吗，能不能直接从 Kafka 算指标」——能跑，但失去明细回溯，算错无法重算（Kafka 有保留期）。实时更需要 DWD，因为排错窗口更短。

> [!question] 10. 给你一张新表，怎么判断该放哪层？
> **一句话答法**：三步——① 看粒度：与源系统一致 → ODS；业务过程最小单元 → DWD；维度组合 → DWS；报表格子 → ADS。② 看复用：只有 1 个下游就直接放 ADS，不建 DWS。③ 看是否聚合：聚合了一定不在 DWD。
> **追问陷阱**：「既明细又带累计值怎么办」——拆两张：明细放 DWD，累计值放 DWS。混在一起就是粒度污染，见 [02-02 维度建模四步法](02-02%20%E7%BB%B4%E5%BA%A6%E5%BB%BA%E6%A8%A1%E5%9B%9B%E6%AD%A5%E6%B3%95.md)。

---

## 六、自测题

> [!question] 1.（单选）以下哪种操作**违反** ODS 层设计原则？
> A. 把 text 转成 orc 存储
> B. 增加 `ds` 分区和 `etl_time` 字段
> C. 过滤掉 `pay_amount <= 0` 的异常订单
> D. 增加 `md5(concat_ws('|', ...))` 去重辅助列

> [!question] 2.（单选）某 DWD 表一行代表「一个订单」，含 `sum(item_amount) as pay_amount`。问题是？
> A. 没有分区
> B. 在 DWD 做了聚合，粒度不是业务过程最小粒度，下游无法按商品分析
> C. 字段命名不规范
> D. 没问题，订单粒度也是明细

> [!question] 3.（判断）DIM 层是 ODS → DWD → DWS → ADS 流水线中的一级，位置在 DWD 之后。

> [!question] 4.（场景设计）12 张报表全部直接查 `ods_order` 和 `ods_order_item`，现在要改 GMV 口径（剔除运费）。请给出改造方案：建哪些表、每张表什么粒度、如何保证 12 张报表同时生效。

> [!question] 5.（场景设计）业务方要求「实时看每店当天 GMV，延迟 ≤ 1 分钟」，同时「历史任意一天的 GMV 要能重算」。请设计分层方案，说明实时与离线链路分别怎么分，以及两者如何对齐口径。

> [!success]- 答案与解析（点击展开）
> **1. C**
> ODS 只做无损加工。过滤 `pay_amount <= 0` 会丢掉退款冲正订单，这些数据在数仓里永久消失，财务对账时无法还原。正确做法：ODS 全量保留，DWD 用 `case when` 打标记或分流到异常表。
>
> **2. B**
> `sum(item_amount)` 是聚合，聚合后一行代表「多个商品」，粒度从「订单+商品」退化成「订单」。下游要按类目看销量时没有 `sku_id`，只能回退 ODS 重算。
> 注意区分：若 `pay_amount` 是交易系统订单主表**自带**字段，这张表是合法 DWD；只有「自己 sum 出来的」才算违规。
>
> **3. 错误**
> DIM 是**横向共享**层，不在纵向流水线的任何一级，被 DWD（宽表化）、DWS（维度汇总）、ADS（维度展示）同时引用。若它变成流水线一级，就意味着只有它下游那一层能用维度，显然不对。
> 标准答法：「四层 + 一个独立的维度层」，而不是「五层」。
>
> **4. 参考方案**
> **第一步 DWD（1 张）**：`dwd_trade_order_detail`，粒度 = 一个订单的一个商品，`dt` = 下单日期。清洗（去重、状态规范化）+ 宽表化（冗余 `sku_name`/`cat1_name`/`shop_name`）。这是 12 张报表的**唯一明细来源**。
>
> **第二步 DWS（2~3 张）**：把 12 张报表的 `group by` 列出来找高频维度组合，通常落在 `dws_trade_shop_day`（店铺+天）、`dws_trade_cat1_day`（类目+天）、`dws_trade_user_day`（用户+天）。不要为单张报表建 DWS。
>
> **第三步 ADS**：按报表数量建，或在 DWS 之上做视图，派生指标（同比、环比、排名）放这里。
>
> **第四步 口径只改一处**：新 GMV（剔除运费）落在 `dwd_trade_order_detail.goods_amount`（ODS 有运费字段就减，没有就推动源系统补），DWS 全部引用它。12 张报表只改 `from` 子句，口径定义只有一份。
>
> **第五步 灰度对账**：新旧链路并行 7 天逐日对账，差异逐条归因（通常是去重逻辑或口径差异），确认后下线老报表。
>
> **关键点**：不要一上来建 12 张 ADS。先建对 DWD（粒度、去重、口径字段），再按维度交集建 2~3 张 DWS，最后 ADS。顺序反了必然返工。
>
> **5. 参考方案**
> **实时链路**：`ODS`（binlog → Kafka，按 `order_id` 分区保证同单有序）→ `DWD`（Flink 消费，按 `order_id` 用 state 去重、广播关联 `dim_sku`）→ `DWS`（1 分钟滚动窗口按 `shop_id` 聚合，写 ClickHouse/Doris）→ `ADS`（接口直查 DWS）。
>
> **离线链路**：`ODS`（同一份 binlog 落 HDFS）→ `DWD`（同一套清洗规则）→ `DWS`（按天汇总）→ `ADS`。
>
> **口径对齐三个动作**：
> ① **同一份清洗逻辑**：去重、状态映射、金额口径抽成公共 UDF/配置，实时离线共用，避免两套 `case when`。
> ② **T+1 对账**：每天用离线结果校验实时结果，差异超阈值告警。差异常见来源是迟到数据。
> ③ **迟到数据处理**：实时侧用 watermark 允许 N 分钟迟到（N **待验证**，按业务定），超期走离线修正，DWS 按天做「实时值 + 离线修正」合并。
>
> **分层关键差异**：实时链路通常没有 DWM（延迟不允许），且 DWD 去重必须靠状态而非 `row_number`（无界流没有「全量」概念）。能答出这点，说明真做过。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 前置 | [00_模块1导航](../01%20%E6%A8%A1%E5%9D%971%20Hive%26Spark%20SQL/00_%E6%A8%A1%E5%9D%971%E5%AF%BC%E8%88%AA.md) | 分区、去重、窗口函数是分层落地的基本功 |
| 直接下游 | [02-02 维度建模四步法](02-02%20%E7%BB%B4%E5%BA%A6%E5%BB%BA%E6%A8%A1%E5%9B%9B%E6%AD%A5%E6%B3%95.md) | 分层定「在哪算」，四步法定「这一层的表怎么设计」 |
| 直接下游 | [02-03 星型模型 vs 雪花模型](02-03%20%E6%98%9F%E5%9E%8B%E6%A8%A1%E5%9E%8B%20vs%20%E9%9B%AA%E8%8A%B1%E6%A8%A1%E5%9E%8B.md) | DWD 宽表化时维度怎么组织 |
| 直接下游 | [02-09 数据域与主题划分](02-09%20%E6%95%B0%E6%8D%AE%E5%9F%9F%E4%B8%8E%E4%B8%BB%E9%A2%98%E5%88%92%E5%88%86.md) | DWS 的「主题」怎么切，决定 DWS 有几张表 |
| 强相关 | [02-08 指标口径与指标体系](02-08%20%E6%8C%87%E6%A0%87%E5%8F%A3%E5%BE%84%E4%B8%8E%E6%8C%87%E6%A0%87%E4%BD%93%E7%B3%BB.md) | 分层解决「在哪算」，口径解决「算什么」 |
| 强相关 | [02-10 分区策略与生命周期管理](02-10%20%E5%88%86%E5%8C%BA%E7%AD%96%E7%95%A5%E4%B8%8E%E7%94%9F%E5%91%BD%E5%91%A8%E6%9C%9F%E7%AE%A1%E7%90%86.md) | 各层分区粒度与保留周期不同（ODS 长、ADS 短） |
| 强相关 | [02-11 建模评审 Checklist](02-11%20%E5%BB%BA%E6%A8%A1%E8%AF%84%E5%AE%A1%20Checklist.md) | 分层是否合规是评审第一项 |
| 综合实战 | [02-12 建模实战：电商订单域](02-12%20%E5%BB%BA%E6%A8%A1%E5%AE%9E%E6%88%98%EF%BC%9A%E7%94%B5%E5%95%86%E8%AE%A2%E5%8D%95%E5%9F%9F.md) | 本节实验的完整放大版 |
| 下游模块 | [00_模块3导航](../03%20%E6%A8%A1%E5%9D%973%20Spark%E8%B0%83%E4%BC%98%E3%80%81%E6%95%B0%E6%8D%AE%E8%B4%A8%E9%87%8F%E8%A1%80%E7%BC%98/00_%E6%A8%A1%E5%9D%973%E5%AF%BC%E8%88%AA.md) | 分层决定血缘图形状，血缘是数据质量的基础 |
| 下游模块 | [00_模块4导航](../04%20%E6%A8%A1%E5%9D%974%20OLAP%E5%BC%95%E6%93%8E/00_%E6%A8%A1%E5%9D%974%E5%AF%BC%E8%88%AA.md) | DWS 宽表是 OLAP 引擎的主要输入 |

> [!tip] 学习建议
> 这节最容易「看懂了但不会用」。检验标准只有一个：**给你一张现成的表，30 秒内说出它该放哪层、粒度是什么、有没有违规**。
> 练法：打开公司数仓元数据，随机抽 20 张表，只看表名和字段猜粒度，再 `desc` 验证。猜错的就是还没掌握的地方。
