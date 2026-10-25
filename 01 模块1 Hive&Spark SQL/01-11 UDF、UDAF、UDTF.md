---
title: 01-11 UDF、UDAF、UDTF
module: 模块1 Hive & Spark SQL
difficulty: ⭐⭐
created: 2026-10-25
status: 未开始
tags:
  - Hive
  - SparkSQL
  - UDF
  - 自定义函数
---

# 01-11 UDF、UDAF、UDTF

> [!abstract] 一句话结论
> 三种自定义函数的边界很清楚：**UDF 一行进一行出**（标量转换）、**UDAF 多行进一行出**（自定义聚合）、**UDTF 一行进多行出**（炸开）。
> 生产上更重要的判断是：**能不写 UDF 就不写**——UDF 是黑盒，优化器无法优化、无法向量化、跨引擎不可移植，且容易成为性能瓶颈。先穷尽内置函数，再考虑 UDF。

---

## 一、心智模型：三种函数的「形状」

```
UDF   : 1 行 → 1 行     f(x) = y
UDAF  : N 行 → 1 行     f(x1, x2, ..., xN) = y      （聚合）
UDTF  : 1 行 → N 行     f(x) = (y1, y2, ..., yN)    （炸开）
```

| 类型 | 输入 | 输出 | 类比内置函数 | 典型用途 |
| --- | --- | --- | --- | --- |
| UDF | 1 行 | 1 个值 | `upper`、`abs` | 加密、脱敏、复杂解析 |
| UDAF | 多行 | 1 个值 | `sum`、`avg` | 自定义聚合（如中位数、加权平均） |
| UDTF | 1 行 | 多行（可多列） | `explode` | 解析 JSON 数组、拆分行 |

> [!important] 一条判断链
> 1. 内置函数能做到吗？→ 能就别写
> 2. 能用 SQL 表达式组合出来吗？→ 能就别写
> 3. 必须写 → 优先 UDF（最简单），其次 UDAF，最后 UDTF（最影响性能）

---

## 二、核心知识点

### 2.1 UDF：一行进一行出

**Hive UDF 的三种写法**：

| 方式 | 类 | 特点 |
| --- | --- | --- |
| 继承 `UDF` | `org.apache.hadoop.hive.ql.exec.UDF` | 最简单，`evaluate()` 方法 |
| 继承 `GenericUDF` | `org.apache.hadoop.hive.ql.udf.generic.GenericUDF` | 支持复杂类型、可变参数，性能更好 |
| 注解方式 | `@UDFType(deterministic = true)` | 标记确定性，便于优化 |

**最简 UDF 示例**（手机号脱敏）：

```java
package com.example.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MaskPhone extends UDF {
    public Text evaluate(Text phone) {
        if (phone == null) return null;
        String p = phone.toString().trim();
        if (p.length() < 7) return new Text(p);
        return new Text(p.substring(0, 3) + "****" + p.substring(p.length() - 4));
    }
}
```

**注册与使用**：

```sql
-- Hive：临时函数（会话级）
add jar /path/to/udf.jar;
create temporary function mask_phone as 'com.example.udf.MaskPhone';

-- Hive：永久函数（需要放到 HDFS 并创建）
create function mask_phone as 'com.example.udf.MaskPhone'
using jar 'hdfs:///udf/udf.jar';

-- Spark：临时函数
create temporary function mask_phone as 'com.example.udf.MaskPhone'
using jar 'hdfs:///udf/udf.jar';

select mask_phone('13800001234');   -- 138****1234
```

> [!warning] UDF 的性能代价
> 1. **无法向量化**：Spark 的 whole-stage codegen 对 UDF 无效，每行都要走 Java 调用
> 2. **无法被优化器下推**：`where mask_phone(phone) = 'xxx'` 无法下推，必须全表计算
> 3. **JVM 开销**：大量小对象的创建/销毁（如 `new Text(...)`）
> 4. **跨引擎不兼容**：Hive UDF 在 Spark 里通过兼容层调用，性能更差

> [!tip] UDF 优化建议
> - 复用对象：不要每次 `new Text()`，用成员变量或 `Text.set()`
> - 避免在 UDF 里做 IO（查数据库、读文件）——每行一次，灾难
> - 能用内置函数组合的，别写 UDF。例如脱敏可以用 `concat(substr(phone,1,3),'****',substr(phone,-4))`
> - 优先用 `GenericUDF`（避免 boxing/unboxing）

### 2.2 UDAF：多行进一行出

**Hive UDAF 的两种接口**：

| 接口 | 复杂度 | 特点 |
| --- | --- | --- |
| `UDAF`（简单版） | 低 | 用 `UDAFEvaluator` + `@UDAF` 注解，但只能处理基本类型 |
| `GenericUDAFResolver2` | 高 | 支持复杂类型、可变参数，**生产推荐** |

**简单版 UDAF 示例**（求平均值，理解结构）：

```java
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;

@UDAF
public class AvgUDAF {
    public static class AvgEvaluator implements UDAFEvaluator {
        private double sum;
        private long count;

        public AvgEvaluator() { init(); }

        @Override
        public void init() { sum = 0; count = 0; }

        // 处理一行
        public boolean iterate(Double value) throws HiveException {
            if (value != null) { sum += value; count++; }
            return true;
        }

        // 返回部分聚合结果（map 端）
        public Double terminatePartial() {
            return count == 0 ? null : sum / count;   // ⚠️ 这里其实有 bug，见下
        }

        // 合并部分结果（reduce 端）
        public boolean merge(Double other) throws HiveException {
            if (other != null) { sum += other; count++; }
            return true;
        }

        // 返回最终结果
        public Double terminate() {
            return count == 0 ? null : sum / count;
        }
    }
}
```

> [!danger] 这个「教科书示例」其实有 bug
> `terminatePartial()` 返回的是平均值，但 `merge()` 把它当成「一个值」累加——两个 map 各算出 avg=10（100 行）和 avg=20（10 行），merge 后变成 `(10+20)/2 = 15`，而正确值是 `1100/110 ≈ 10.9`。
> **正确做法**：`terminatePartial()` 返回一个包含 `sum` 和 `count` 的 `struct`，`merge()` 分别累加 sum 和 count。
> 这个例子正好说明：**写 UDAF 必须理解「两阶段聚合」**（见 01-05），否则结果会错。

**正确的 UDAF 结构**：

```java
// terminatePartial 返回 struct<sum:double, count:long>
// merge 分别累加 sum 和 count
// terminate 用 sum/count 算最终值
```

**Spark UDAF（`Aggregator`）**：

```scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

case class AvgBuffer(var sum: Double, var count: Long)

object AvgAgg extends Aggregator[Double, AvgBuffer, Double] {
    def zero: AvgBuffer = AvgBuffer(0.0, 0L)
    def reduce(b: AvgBuffer, a: Double): AvgBuffer = {
        if (a != null) { b.sum += a; b.count += 1 }
        b
    }
    def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
        b1.sum += b2.sum; b1.count += b2.count; b1
    }
    def finish(b: AvgBuffer): Double = if (b.count == 0) 0.0 else b.sum / b.count
    def bufferEncoder: Encoder[AvgBuffer] = Encoders.product[AvgBuffer]
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// 注册
spark.udf.register("avg_agg", functions.udaf(AvgAgg))
```

> [!note] Spark 3.x 的 `Aggregator` 是类型安全的
> 相比 Hive UDAF，Spark 的 `Aggregator` 用 case class 做 buffer，编译期检查，不容易出错。
> 但**跨引擎不可移植**，如果两个引擎都要跑，用 Hive UDAF。

### 2.3 UDTF：一行进多行出

**UDTF 的典型场景**：一行里的 JSON 数组要拆成多行。

```java
import org.apache.hadoop.hive.ql.exec.UDTF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import java.util.ArrayList;

public class SplitToRows extends UDTF {
    private String[] parts;
    private int idx;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) throw new UDFArgumentException("需要 2 个参数");
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("part");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (args[0] == null) return;
        String s = args[0].toString();
        String sep = args[1] == null ? "," : args[1].toString();
        parts = s.split(sep);
        idx = 0;
        // 注意：forward 在 process 里调用，每次 forward 输出一行
    }

    @Override
    public void close() throws HiveException { }

    // 实际实现中通常用 forward(new Object[]{part}) 逐个输出
}
```

> [!warning] UDTF 的三个限制
> 1. **必须配合 `lateral view` 使用**，不能和其他列直接 select
> 2. **会放大数据量**，数组长度决定放大倍数
> 3. **性能开销大**：每行都要创建对象、调用 forward

**内置 UDTF 已经覆盖大部分场景**：

| 需求 | 内置函数 |
| --- | --- |
| 数组拆行 | `explode(array)` |
| Map 拆行 | `explode(map)` |
| 带下标拆行 | `posexplode(array)` |
| JSON 数组拆行 | `explode(from_json(json, 'array<string>'))` |
| 字符串按分隔符拆行 | `explode(split(str, ','))` |

> [!success] 结论
> **99% 的「需要 UDTF」场景，用 `explode` + `split` + `from_json` 就能解决。**
> 真正需要手写 UDTF 的情况很少：比如「一行变多行且每行内容需要复杂的状态计算」。

### 2.4 该不该写 UDF：决策树

```
需要自定义逻辑
    │
    ├─ 内置函数能做到？ ──是──> 用内置
    │        │否
    ├─ 能用 SQL 表达式组合？ ──是──> 用表达式
    │        │否
    ├─ 是"行级转换"？ ──是──> UDF（注意性能）
    │        │否
    ├─ 是"聚合"？ ──是──> UDAF（注意两阶段）
    │        │否
    └─ 是"拆行"？ ──是──> 优先 explode，其次 UDTF
```

**反例：本可以不用 UDF 的场景**

```sql
-- ❌ 写 UDF 做手机号脱敏
select mask_phone(phone) from t;

-- ✅ 内置函数组合
select concat(substr(phone,1,3), '****', substr(phone,8,4)) from t;

-- ❌ 写 UDF 做日期格式转换
select my_date_fmt(dt) from t;

-- ✅ 内置函数
select date_format(dt, 'yyyyMMdd') from t;

-- ❌ 写 UDF 做 JSON 取值
select my_json_get(json_str, 'name') from t;

-- ✅ 内置函数
select get_json_object(json_str, '$.name') from t;
-- 或者 Spark 3.x
select from_json(json_str, 'name string').name from t;
```

**真正需要 UDF 的场景**：

| 场景 | 为什么内置不够 |
| --- | --- |
| 加解密（AES、RSA） | 内置没有 |
| 复杂脱敏规则（按业务规则部分掩码） | 规则复杂，表达式难写 |
| 地址解析（省市区拆分） | 需要词典匹配 |
| IP 转地理位置 | 需要 IP 库 |
| 特殊哈希（如一致性哈希） | 算法自定义 |
| 文本相似度（编辑距离） | 内置没有 |

> [!danger] UDF 里做 IO 是绝对的禁忌
> ```java
> // ❌ 灾难：每行查一次数据库
> public String evaluate(String userId) {
>     return jdbcTemplate.queryForObject("select name from user where id=?", ...);
> }
> ```
> 1 亿行 = 1 亿次数据库查询，直接把数据库打挂。
> **正确做法**：把维表加载到内存（`@Setup` 方法里初始化一次），或用 `map join` 代替。

### 2.5 UDF 的工程化实践

**依赖管理**：

```xml
<!-- pom.xml：只打自己的类，Hive 依赖用 provided -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>3.1.3</version>
    <scope>provided</scope>
</dependency>
```

**打包注意事项**：

| 问题 | 解决 |
| --- | --- |
| 依赖冲突（guava 等） | 用 `maven-shade-plugin` 重定位（relocate） |
| jar 太大 | 只打必要依赖；用 `provided` 排除引擎自带包 |
| 类找不到 | 检查 `hive-exec` 版本是否与集群一致 |
| 跨引擎不兼容 | Hive UDF 在 Spark 里通过兼容层调用，性能差；必要时写两套 |

**注册的三种方式**：

```sql
-- 1. 临时函数（会话级，最常用）
create temporary function my_udf as 'com.example.MyUdf' using jar 'hdfs:///udf/my-udf.jar';

-- 2. 永久函数（Hive，需要管理员权限）
create function db.my_udf as 'com.example.MyUdf' using jar 'hdfs:///udf/my-udf.jar';

-- 3. 通过配置文件预注册（Hive 的 auxlib）
-- 把 jar 放到 $HIVE_HOME/auxlib/ 下，重启生效
```

> [!tip] 生产建议
> - **UDF 的 jar 放 HDFS 统一路径**，不要放本地（每个节点都要有）
> - **版本化管理**：`/udf/my-udf-1.0.0.jar`，升级时改路径，方便回滚
> - **加单元测试**：UDF 逻辑错了很难发现（静默返回错误值），必须测边界（NULL、空串、超长、特殊字符）

### 2.6 UDF 的替代方案（更现代的做法）

| 需求 | 传统方案 | 更现代的方案 |
| --- | --- | --- |
| 复杂转换 | UDF | Spark 3.x 的 `transform` / 高阶函数 |
| 复杂聚合 | UDAF | `aggregate` / `reduce` 高阶函数 |
| JSON 解析 | UDTF | `from_json` + `explode` |
| 维表关联 | UDF 查库 | `broadcast join` |
| 文本处理 | UDF | `regexp_replace` / `regexp_extract` |

```sql
-- Spark 3.x 高阶函数：不用 UDF 也能做数组转换
select transform(arr, x -> x * 2) as doubled from t;
select filter(arr, x -> x > 0) as positives from t;
select aggregate(arr, 0, (acc, x) -> acc + x) as total from t;
select exists(arr, x -> x > 100) as has_big from t;
```

> [!success] 高阶函数是 UDF 的最佳替代
> Spark 3.x 的高阶函数（`transform`、`filter`、`aggregate`、`exists`、`zip_with`）能覆盖大量原本需要 UDF 的场景，而且**能被优化器优化、能向量化**。
> 但 **Hive 不支持高阶函数**，跨引擎时不可用。

---

## 三、动手实验

### 3.1 用内置函数替代 UDF（先验证可行性）

```sql
-- 手机号脱敏：三种写法对比
select
    '13800001234' as raw,
    concat(substr('13800001234',1,3), '****', substr('13800001234',8,4)) as by_builtin,
    regexp_replace('13800001234', '(\\d{3})\\d{4}(\\d{4})', '$1****$2') as by_regex
;
```

### 3.2 完整 UDF 示例（可直接编译运行）

```java
package com.example.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(name = "mask_id", value = "mask_id(id) - 身份证脱敏")
public class MaskId extends UDF {
    private final Text result = new Text();   // 复用对象，避免频繁 GC

    public Text evaluate(Text id) {
        if (id == null) return null;
        String s = id.toString().trim();
        if (s.length() < 8) { result.set(s); return result; }
        result.set(s.substring(0, 4) + "**********" + s.substring(s.length() - 4));
        return result;
    }
}
```

```sql
add jar hdfs:///udf/example-udf.jar;
create temporary function mask_id as 'com.example.udf.MaskId';

select mask_id('110101199001011234');   -- 1101**********1234
select mask_id(null);                    -- NULL
select mask_id('');                      -- ''
select mask_id('123');                   -- '123'（长度不足，原样返回）
```

**边界测试清单**（必须测）：

| 输入 | 期望输出 | 说明 |
| --- | --- | --- |
| `null` | `null` | 不能 NPE |
| `''` | `''` | 空串不是 NULL |
| 超短字符串 | 原样 | 边界处理 |
| 超长字符串 | 正常截断 | 性能 |
| 含中文/特殊字符 | 不崩溃 | 编码 |

### 3.3 UDTF 对比：内置 vs 自定义

```sql
-- 造数
drop table if exists tmp.json_lab;
create table tmp.json_lab (id int, tags string);
insert into tmp.json_lab values
(1, '["a","b","c"]'),
(2, '["d"]'),
(3, null),
(4, '[]');

-- 内置：split + explode
select id, tag
from tmp.json_lab
lateral view explode(split(regexp_replace(tags, '\\[|\\]|"', ''), ',')) t as tag;
-- 注意 null 和 [] 的行为

-- 内置（Spark）：from_json + explode
select id, tag
from tmp.json_lab
lateral view explode(from_json(tags, 'array<string>')) t as tag;

-- 对比 outer 版本（保留空行）
select id, tag
from tmp.json_lab
lateral view outer explode(from_json(tags, 'array<string>')) t as tag;
```

**观察点**：
- `tags = null` 时，普通 `lateral view` 该行消失
- `tags = '[]'` 时，explode 后无输出，该行也消失
- `lateral view outer` 会保留这些行，`tag` 为 NULL

### 3.4 性能对比：UDF vs 内置函数

```sql
-- 造 100 万行
drop table if exists tmp.perf_lab;
create table tmp.perf_lab as
select concat('138', lpad(cast(id as string), 8, '0')) as phone
from (select row_number() over (order by 1) as id from tmp.json_lab a cross join tmp.json_lab b cross join tmp.json_lab c) t;

-- A：内置函数
select count(*) from (
    select concat(substr(phone,1,3), '****', substr(phone,8,4)) from tmp.perf_lab
) t;

-- B：UDF（注册后）
select count(*) from (
    select mask_phone(phone) from tmp.perf_lab
) t;
```

**对比**：看 Spark UI 的 task 耗时和 CPU 时间。UDF 通常慢 2~5 倍（无法向量化）。

---

## 四、生产坑点清单

| 现象 | 根因 | 排查手段 | 修复方案 |
| --- | --- | --- | --- |
| UDF 报 `ClassNotFoundException` | jar 没上传到 HDFS / 路径错 | 检查 `add jar` 和路径 | jar 放 HDFS 统一路径 |
| UDF 报 `NoMatchingMethod` | 参数类型不匹配（如传 int 但方法是 Text） | 看报错的方法签名 | 显式 cast 或重载方法 |
| UDF 返回 NULL 但预期有值 | 入参是 NULL 未处理 | 单独 `select udf(null)` 测试 | 加 null 判断 |
| 任务变慢 3 倍 | UDF 无法向量化 | 对比 task CPU 时间 | 改内置函数；或减少 UDF 调用次数 |
| UDAF 结果偏小/偏大 | `terminatePartial` 返回了聚合值而非中间态 | 检查 merge 逻辑 | 返回 sum+count 的 struct |
| UDTF 后行数暴增 | 数组太大 | `max(size(arr))` 检查 | 限制长度；过滤异常 |
| UDTF 后行数变少 | NULL 导致行消失 | 对比 explode 前后 | `lateral view outer` |
| UDF 版本不一致 | 不同任务用不同 jar 版本 | 查 `show functions` | 版本化路径；统一管理 |
| 依赖冲突 | UDF jar 里的 guava 和集群版本冲突 | 看 `NoSuchMethodError` | `maven-shade-plugin` relocate |
| UDF 里查库导致超时 | 每行一次 IO | 看数据库连接数 | 改成广播 join |

---

## 五、面试追问

> [!question] 1. UDF、UDAF、UDTF 的区别？
> **一句话答法**：UDF 一行进一行出（标量）；UDAF 多行进一行出（聚合）；UDTF 一行进多行出（炸开）。
> **追问陷阱**：「UDTF 怎么用」——必须配合 `lateral view`，不能和其他列直接 select。

> [!question] 2. 什么情况你会写 UDF？
> **一句话答法**：内置函数和 SQL 表达式都做不到的时候，比如加解密、地址解析、IP 定位、复杂业务规则。
> **追问陷阱**：「写 UDF 有什么代价」——无法向量化、无法被优化器优化、跨引擎不可移植、性能差。

> [!question] 3. UDAF 的 `terminatePartial` 和 `terminate` 有什么区别？
> **一句话答法**：`terminatePartial` 返回 map 端的**中间态**（必须可合并），`terminate` 返回最终结果。中间态不能是已经聚合好的值。
> **追问陷阱**：「求平均值的中间态是什么」——`struct<sum, count>`，不能是 `sum/count`（否则合并时会错）。

> [!question] 4. UDF 为什么慢？
> **一句话答法**：无法向量化（whole-stage codegen 失效）、每行走 JVM 调用、可能频繁创建对象、无法下推。
> **追问陷阱**：「怎么优化」——用内置函数替代、复用对象（避免 new）、用 `GenericUDF`、减少调用次数（先聚合再调用）。

> [!question] 5. `explode` 和 `lateral view` 的关系？
> **一句话答法**：`explode` 是 UDTF，不能直接和其他列一起 select；`lateral view` 是把 UDTF 的结果「横向展开」成一张虚拟表，从而能和其他列 join。
> **追问陷阱**：「`lateral view outer` 有什么区别」——数组为 NULL 时保留原行（补 NULL），否则该行消失。

> [!question] 6. UDF 能不能访问外部资源（数据库、文件）？
> **一句话答法**：技术上可以，但**强烈不建议**——每行一次 IO，会打挂外部系统。正确做法是把资源加载到内存（`@Setup` 初始化一次），或用 join。
> **追问陷阱**：「如果数据量很大，内存放不下怎么办」——用 broadcast join，让引擎管理内存和分发。

> [!question] 7. Spark 的 UDF 和 Hive 的 UDF 兼容吗？
> **一句话答法**：Spark 能调用 Hive UDF（通过兼容层），但性能比原生 Spark UDF 差，且不支持所有特性。
> **追问陷阱**：「反过来呢」——Hive 不能调用 Spark UDF。

> [!question] 8. 怎么保证 UDF 的正确性？
> **一句话答法**：单元测试 + 边界测试（NULL、空串、超长、特殊字符）+ 与内置函数的结果对比（如果逻辑等价）。
> **追问陷阱**：「线上发现 UDF 结果错了怎么办」——先回滚到旧版本 jar，再定位；所以 jar 必须版本化。

> [!question] 9. Spark 3.x 有什么替代 UDF 的方案？
> **一句话答法**：高阶函数（`transform`、`filter`、`aggregate`、`exists`、`zip_with`），能被优化器优化且能向量化。
> **追问陷阱**：「Hive 支持吗」——不支持，跨引擎时不可用。

> [!question] 10. UDTF 会不会导致数据倾斜？
> **一句话答法**：会。如果某行的数组特别长（如一个用户 10 万条行为），explode 后这一行变成 10 万行，全在一个 task 里。
> **追问陷阱**：「怎么处理」——先过滤异常数据（`where size(arr) <= 1000`），或对超长数组单独处理。

> [!question] 11. 永久函数和临时函数的区别？
> **一句话答法**：临时函数只在当前会话有效（`create temporary function`）；永久函数注册在 metastore 里，所有会话可用（`create function`），但需要 jar 在 HDFS 上。
> **追问陷阱**：「生产上用哪个」——通常用临时函数 + 调度系统统一注入（在 SQL 开头 `add jar` + `create temporary function`），避免权限和版本管理问题。

---

## 六、自测题

> [!question] 1.（单选）要把一行里的 JSON 数组拆成多行，最合适的函数类型是？
> A. UDF
> B. UDAF
> C. UDTF
> D. 窗口函数

> [!question] 2.（单选）UDAF 的 `terminatePartial()` 应该返回什么？
> A. 最终聚合结果
> B. 可合并的中间态（如 sum 和 count）
> C. 输入的第一行
> D. NULL

> [!question] 3.（判断）UDF 可以被 Spark 的 whole-stage codegen 优化，所以性能接近内置函数。

> [!question] 4.（场景设计）业务需要把用户地址「北京市朝阳区xxx街道xxx号」解析成「省/市/区/详细地址」四个字段。内置函数做不到。请设计实现方案，并说明如何保证性能和正确性。

> [!question] 5.（场景设计）你写了一个 UDF 做金额格式化，上线后发现整个任务慢了 4 倍。请分析原因，并给出至少 3 种优化方案（要求其中一种不改 UDF 逻辑）。

> [!success]- 答案与解析（点击展开）
> **1. C**
> UDTF（一行进多行出）。实际用内置的 `explode` + `from_json` 即可。
>
> **2. B**
> 必须返回可合并的中间态。如果返回最终结果（如平均值），`merge` 阶段无法正确合并（分母丢失）。
>
> **3. 错误**
> UDF 恰恰是 whole-stage codegen 的「黑洞」——遇到 UDF 就退化成逐行解释执行，性能远低于内置函数。
>
> **4. 方案设计**
> **实现方案（推荐：UDF + 内存词典）**
> ```java
> public class AddressParser extends GenericUDF {
>     private static Map<String, String> provinceMap;   // 静态，只加载一次
>     private static Map<String, String> cityMap;
>
>     @Override
>     public ObjectInspector initialize(ObjectInspector[] args) {
>         if (provinceMap == null) {
>             // 从 HDFS 或 classpath 加载省市区词典（几万条，几 MB）
>             provinceMap = loadDict("/dict/province.txt");
>             cityMap = loadDict("/dict/city.txt");
>         }
>         return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
>     }
>
>     @Override
>     public Object evaluate(DeferredObject[] args) {
>         String addr = args[0].get() == null ? null : args[0].get().toString();
>         if (addr == null || addr.trim().isEmpty()) return null;
>         // 前缀匹配 + 最长匹配算法
>         return parse(addr, provinceMap, cityMap);
>     }
> }
> ```
> **性能保证**
> ① **词典只加载一次**：用 `static` + `initialize` 里判断，避免每行读文件
> ② **用 Trie 树或前缀 Map**：避免每行做 O(n) 的全词典扫描
> ③ **限制词典大小**：省市区词典通常几万条，完全可以放内存
> ④ **减少调用次数**：如果同一地址重复率高，可以先 `group by address` 去重再解析，最后 join 回去（把 1 亿次调用降到几百万次）
>
> **正确性保证**
> ① **边界测试**：直辖市（北京/上海）、自治区（内蒙古/广西）、简称（京/沪）、无省市（只有区）、特殊字符
> ② **覆盖率统计**：解析失败率超过阈值要告警（`where province is null` 的比例）
> ③ **抽样人工核对**：随机抽 1000 条人工比对
> ④ **与线上系统对比**：如果业务系统已有地址解析服务，抽样对比两边结果
>
> **5. 原因分析**
> ① **无法向量化**：UDF 阻断了 whole-stage codegen，从「批量处理」退化成「逐行 JVM 调用」
> ② **对象创建开销**：如果 UDF 里每行 `new BigDecimal()` / `new SimpleDateFormat()`，GC 压力巨大
> ③ **无法下推**：`where fmt(amount) = 'x'` 这类条件无法下推，必须全表计算
> ④ **调用次数**：如果 UDF 在 `select` 里被多次引用（如 `select fmt(a), fmt(a)+1`），可能被重复计算
>
> **优化方案**
> | # | 方案 | 效果 | 是否改逻辑 |
> | --- | --- | --- | --- |
> | 1 | **改用内置函数**（`format_number` / `printf` / `cast` + `concat`） | 最快，恢复向量化 | 改 |
> | 2 | **复用对象**：`SimpleDateFormat` 提为成员变量（注意线程安全，用 `ThreadLocal`）、`BigDecimal` 复用 | 减少 GC，快 20~50% | 改实现不改逻辑 |
> | 3 | **减少调用次数**：先 `group by amount` 去重（金额的 distinct 值远小于行数），算出结果再 join 回去 | 调用次数从 1 亿降到几千 | 不改 UDF |
> | 4 | **改用 `GenericUDF`**：避免 boxing/unboxing | 快 10~30% | 改实现 |
> | 5 | **先过滤再调用**：`where amount is not null`，减少无效调用 | 视数据而定 | 不改 |
>
> **方案 3 详解**（不改 UDF 逻辑的那个）：
> ```sql
> -- 原来是逐行调用 UDF
> select order_id, my_fmt(amount) as amount_str from dwd.order;
>
> -- 改成先对 distinct 值调用，再 join
> select o.order_id, f.amount_str
> from dwd.order o
> join (
>     select amount, my_fmt(amount) as amount_str
>     from (select distinct amount from dwd.order) t
> ) f on o.amount = f.amount;
> ```
> 这个技巧叫「**字典编码**」或「**去重后计算**」，适用于 UDF 的入参基数远小于行数的场景（金额、状态码、城市名等）。**注意**：如果入参基数接近行数（如 user_id），这个方案无效。

---

## 七、与其他小节的关联

| 关系 | 小节 | 说明 |
| --- | --- | --- |
| 上游 | [[01-05 聚合与多维聚合]] | UDAF 的两阶段聚合原理 |
| 上游 | [[01-06 窗口函数与帧边界]] | `explode` 与窗口的配合 |
| 上游 | [[01-10 Hive 与 Spark SQL 的差异]] | UDF 跨引擎的兼容性 |
| 直接下游 | [[01-13 小文件与写入参数]] | UDTF 放大数据量后的小文件问题 |
| 延伸 | [[00_模块8导航]] | 复杂文本处理可用 LLM / Python UDF 替代 |

> [!tip] 学习建议
> 这一节的学习重点**不是怎么写 UDF，而是判断「该不该写」**。
> 建议把 2.4 的决策树记下来，每次想写 UDF 前先走一遍。真正需要手写 UDF 的场景，一年可能就三五次。
