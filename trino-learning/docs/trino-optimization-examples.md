# Trino 优化器实战案例与EXPLAIN分析

## 📚 目录
1. [EXPLAIN命令详解](#1-explain命令详解)
2. [查询优化前后对比](#2-查询优化前后对比)
3. [Join策略选择案例](#3-join策略选择案例)
4. [动态过滤实战](#4-动态过滤实战)
5. [分区裁剪优化](#5-分区裁剪优化)
6. [统计信息对优化的影响](#6-统计信息对优化的影响)
7. [复杂查询调优实战](#7-复杂查询调优实战)
8. [性能问题诊断流程](#8-性能问题诊断流程)

---

## 1. EXPLAIN命令详解

### 1.1 不同类型的EXPLAIN

```sql
-- 基本逻辑计划
EXPLAIN SELECT * FROM orders WHERE orderdate >= DATE '2023-01-01';

-- 分布式执行计划  
EXPLAIN (TYPE DISTRIBUTED) 
SELECT o.*, c.name 
FROM orders o JOIN customer c ON o.custkey = c.custkey;

-- 详细统计信息
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)
SELECT orderkey, SUM(totalprice) 
FROM orders 
GROUP BY orderkey 
LIMIT 10;

-- IO统计信息
EXPLAIN (TYPE IO) 
SELECT * FROM lineitem 
WHERE shipdate BETWEEN DATE '2023-01-01' AND DATE '2023-12-31';
```

### 1.2 读懂EXPLAIN输出

#### 基本执行计划结构
```
Fragment 0 [SINGLE]
    Output layout: [orderkey, totalprice] 
    Output partitioning: SINGLE []
    - Limit[10]
        │   Layout: [orderkey:bigint, totalprice:double]
        │   Estimates: {rows: 10 (320B), cpu: 0.00, memory: 0B, network: 0B}
        └─ LocalExchange[SINGLE] () 
            │   Layout: [orderkey:bigint, totalprice:double]
            │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
            └─ RemoteSource[1]
                   Layout: [orderkey:bigint, totalprice:double]

Fragment 1 [HASH]
    Output layout: [orderkey, totalprice]
    Output partitioning: HASH [orderkey]
    - TopN[10, orderkey ASC NULLS LAST]
        │   Layout: [orderkey:bigint, totalprice:double]  
        │   Estimates: {rows: 10 (320B), cpu: 15000000.00, memory: 320B, network: 0B}
        └─ TableScan[orders:sf1000]
               Layout: [orderkey:bigint, totalprice:double]
               Estimates: {rows: 15000000 (480MB), cpu: 480000000.00, memory: 0B, network: 0B}
               orderkey := orderkey:bigint:REGULAR
               totalprice := totalprice:double:REGULAR
```

#### 关键指标解读

- **Fragment**: 执行阶段，Fragment 0是最终汇聚阶段
- **Layout**: 每个算子的输出列和类型
- **Estimates**: 代价估算
  - `rows`: 行数估计
  - `cpu`: CPU代价 
  - `memory`: 内存需求
  - `network`: 网络传输量
- **Partitioning**: 数据分布方式 (SINGLE/HASH/BROADCAST)

---

## 2. 查询优化前后对比

### 2.1 未优化的查询

```sql
SELECT 
    c.name,
    COUNT(*) as order_count,
    SUM(o.totalprice) as total_revenue
FROM customer c, orders o, lineitem l  -- 使用逗号连接(隐式笛卡尔积)
WHERE c.custkey = o.custkey 
  AND o.orderkey = l.orderkey
  AND o.orderdate >= DATE '2023-01-01'
  AND c.mktsegment = 'BUILDING'
GROUP BY c.name;
```

#### 未优化的执行计划问题
```
Fragment 0 [SINGLE]
    - Aggregate[GROUP BY name | COUNT(*), SUM(totalprice)]
        └─ RemoteSource[1]

Fragment 1 [HASH] 
    - Filter[c.custkey = o.custkey AND o.orderkey = l.orderkey] -- ❌ 连接条件在Filter中
        │   Estimates: {rows: 750000000000 (!), cpu: ?, memory: ?, network: ?} -- ❌ 笛卡尔积
        └─ CrossJoin  -- ❌ 笛卡尔积
            ├─ RemoteSource[2]  -- customer
            ├─ RemoteSource[3]  -- orders  
            └─ RemoteSource[4]  -- lineitem
```

### 2.2 优化后的查询

```sql
SELECT 
    c.name,
    COUNT(*) as order_count, 
    SUM(o.totalprice) as total_revenue
FROM customer c
JOIN orders o ON c.custkey = o.custkey
JOIN lineitem l ON o.orderkey = l.orderkey  
WHERE o.orderdate >= DATE '2023-01-01'
  AND c.mktsegment = 'BUILDING'
GROUP BY c.name;
```

#### 优化后的执行计划
```
Fragment 0 [SINGLE]
    - Aggregate[GROUP BY name | COUNT(*), SUM(totalprice)]
        └─ RemoteSource[1]

Fragment 1 [HASH]
    Output partitioning: HASH [name]
    - HashJoin[INNER][$hashvalue, $hashvalue_0] 
        │   Distribution: PARTITIONED  -- ✅ 分区Join
        │   Estimates: {rows: 5906 (189KB), cpu: 6.2M, memory: 4MB, network: 189KB}
        ├─ RemoteSource[2] -- orders filtered by date, joined with customer
        └─ RemoteSource[3] -- lineitem

Fragment 2 [BROADCAST]  -- ✅ 广播小表
    - HashJoin[INNER][$hashvalue_1, $hashvalue_2]
        │   Distribution: BROADCAST -- customer表广播
        │   Estimates: {rows: 37500 (1.2MB), cpu: 1.8M, memory: 400KB, network: 1.2MB}  
        ├─ TableScan[orders] -- ✅ 谓词下推
        │     Estimates: {rows: 37500, cpu: 18M, memory: 0B, network: 0B}
        │     Predicate: orderdate >= DATE '2023-01-01' -- ✅ 过滤下推
        └─ LocalExchange[BROADCAST] 
            └─ RemoteSource[4]

Fragment 3 [SOURCE]
    - TableScan[lineitem]
        Layout: [orderkey:bigint, ...]
        
Fragment 4 [SOURCE] 
    - TableScan[customer] -- ✅ 谓词下推
        Predicate: mktsegment = 'BUILDING' -- ✅ 过滤下推  
        Estimates: {rows: 30000, cpu: 3.75M, memory: 0B, network: 0B}
```

#### 优化效果对比

| 指标 | 优化前 | 优化后 | 改善 |
|-----|-------|--------|------|
| 处理行数 | 750B (笛卡尔积) | 5906 | 99.9%↓ |
| CPU代价 | 极高 | 6.2M | 数量级↓ |
| 内存使用 | 极高 | 4MB | 数量级↓ |  
| 网络传输 | 极高 | 189KB | 数量级↓ |

---

## 3. Join策略选择案例

### 3.1 Broadcast Join vs Partitioned Join

```sql
-- 测试查询：大表join小表
SELECT o.orderkey, c.name, o.totalprice
FROM orders o  -- 1500万行
JOIN customer c ON o.custkey = c.custkey  -- 150万行  
WHERE o.orderdate >= DATE '2023-01-01';
```

#### 场景1: 自动选择 (customer表较小)
```sql
SET SESSION join_distribution_type = 'AUTOMATIC';
```

```
Fragment 1 [HASH] 
    - HashJoin[INNER][custkey, custkey]
        │   Distribution: BROADCAST  -- ✅ 自动选择广播
        │   Estimates: {rows: 366K, cpu: 58.2M, memory: 60MB, network: 180MB}
        ├─ TableScan[orders]
        │     Estimates: {rows: 366K, cpu: 17.6M, memory: 0B, network: 0B}  
        │     Predicate: orderdate >= DATE '2023-01-01' -- 先过滤减少数据量
        └─ LocalExchange[BROADCAST] 
            └─ RemoteSource[2] -- 广播customer表
```

#### 场景2: 强制Partitioned Join
```sql
SET SESSION join_distribution_type = 'PARTITIONED';
```

```
Fragment 1 [HASH]
    - HashJoin[INNER][$hashvalue, $hashvalue_0]
        │   Distribution: PARTITIONED  -- 强制分区Join
        │   Estimates: {rows: 366K, cpu: 58.2M, memory: 8MB, network: 400MB} -- 网络开销更大
        ├─ RemoteSource[2] -- orders重分区  
        └─ RemoteSource[3] -- customer重分区
        
Fragment 2 [HASH]
    Output partitioning: HASH [custkey]  -- orders按custkey重分区
    - TableScan[orders]
        Predicate: orderdate >= DATE '2023-01-01'
        
Fragment 3 [HASH] 
    Output partitioning: HASH [custkey]  -- customer按custkey重分区
    - TableScan[customer]
```

#### 性能对比分析

| Join策略 | 网络传输 | 内存使用 | 适用场景 |
|---------|---------|---------|----------|
| BROADCAST | 180MB | 60MB | 右表较小 (<broadcast_threshold) |  
| PARTITIONED | 400MB | 8MB | 右表较大，内存受限 |
| AUTOMATIC | 动态选择 | 动态选择 | **推荐使用** |

### 3.2 动态选择阈值调整

```sql  
-- 查看当前阈值
SHOW SESSION LIKE 'join_distribution%';

-- 调整广播阈值 (默认100MB)
SET SESSION broadcast_join_threshold = '500MB';

-- 重新执行，观察策略变化
EXPLAIN (TYPE DISTRIBUTED) SELECT ...;
```

---

## 4. 动态过滤实战

### 4.1 动态过滤的典型场景

```sql
-- 星型模式查询：事实表JOIN维表
SELECT 
    l.orderkey,
    o.orderdate, 
    c.name,
    SUM(l.extendedprice * (1 - l.discount)) as revenue
FROM lineitem l  -- 事实表: 6亿行
JOIN orders o ON l.orderkey = o.orderkey  -- 维表: 1500万行
JOIN customer c ON o.custkey = c.custkey  -- 维表: 150万行
WHERE o.orderdate BETWEEN DATE '2023-03-01' AND DATE '2023-03-31' -- 高选择性过滤
  AND c.mktsegment = 'BUILDING'  -- 高选择性过滤
GROUP BY l.orderkey, o.orderdate, c.name;
```

#### 启用动态过滤的执行计划

```sql
SET SESSION enable_dynamic_filtering = true;
EXPLAIN (TYPE DISTRIBUTED) SELECT ...;
```

```
Fragment 0 [SINGLE]
    - Aggregate[GROUP BY orderkey, orderdate, name | SUM(revenue)]
        └─ RemoteSource[1]

Fragment 1 [HASH] 
    - HashJoin[INNER][orderkey, orderkey] -- lineitem JOIN orders  
        │   Distribution: PARTITIONED
        │   DynamicFilters: [DF1:orderkey] -- ✅ 动态过滤1
        │   Estimates: {rows: 486K, cpu: 125M, memory: 18MB, network: 15MB}
        ├─ ScanProject[lineitem] 
        │     Layout: [orderkey:bigint, extendedprice:double, discount:double]
        │     Estimates: {rows: 23M, cpu: 1.4B, memory: 0B, network: 0B} -- 应用动态过滤后大幅减少
        │     DynamicFilters: [DF1:orderkey] -- ✅ 接收动态过滤
        └─ RemoteSource[2]

Fragment 2 [BROADCAST]
    - DynamicFilterSource[DF1] -- ✅ 动态过滤源
        └─ HashJoin[INNER][custkey, custkey] -- orders JOIN customer
            │   Distribution: BROADCAST  
            │   Estimates: {rows: 9K, cpu: 2.1M, memory: 400KB, network: 15MB}
            ├─ ScanProject[orders]
            │     Predicate: orderdate BETWEEN ... -- ✅ 静态过滤先执行
            │     Estimates: {rows: 37K, cpu: 18M, memory: 0B, network: 0B}
            └─ LocalExchange[BROADCAST]
                └─ ScanProject[customer] 
                      Predicate: mktsegment = 'BUILDING' -- ✅ 静态过滤先执行
                      Estimates: {rows: 30K, cpu: 3.7M, memory: 0B, network: 0B}
```

#### 动态过滤执行时序

```
时间线: 0ms -----> 100ms -----> 500ms -----> 2000ms
        
t=0:    开始执行所有Fragment
        
t=100:  Fragment2 开始构建orders+customer的join结果
        Fragment1 的 lineitem 扫描已开始，但还未应用动态过滤
        
t=500:  Fragment2 完成join，产生9K行结果
        生成动态过滤器: orderkey IN (9K个值)  
        推送到Fragment1的lineitem扫描
        
t=2000: lineitem扫描应用动态过滤，从6亿行减少到2300万行 (96%过滤率)
        Fragment1 完成最终join和聚合
```

### 4.2 动态过滤效果测量

```sql
-- 查看动态过滤统计
SELECT 
    query_id,
    dynamic_filters_completed,
    dynamic_filtering_data_processed_input,
    dynamic_filtering_data_processed_output
FROM system.runtime.queries 
WHERE query LIKE '%lineitem%'
ORDER BY created DESC LIMIT 5;
```

预期结果:
```
query_id: 20231201_143022_00123_abc12
dynamic_filters_completed: 1
data_processed_input: 24GB   -- lineitem原始数据
data_processed_output: 920MB -- 动态过滤后数据 (96%过滤率!)
```

---

## 5. 分区裁剪优化

### 5.1 按日期分区的表查询

假设orders表按年-月-日分区:
```sql
-- 分区表结构
CREATE TABLE orders_partitioned (
    orderkey BIGINT,
    custkey BIGINT, 
    totalprice DOUBLE,
    orderdate DATE,
    ...
) 
WITH (
    partitioned_by = ARRAY['year', 'month', 'day'],
    format = 'PARQUET'
);
```

#### 分区裁剪的查询
```sql
SELECT orderkey, totalprice
FROM orders_partitioned  
WHERE orderdate = DATE '2023-03-15';  -- 精确日期
```

#### 执行计划分析
```sql
EXPLAIN (TYPE IO, FORMAT JSON) 
SELECT orderkey, totalprice FROM orders_partitioned 
WHERE orderdate = DATE '2023-03-15';
```

```json
{
  "inputTableColumnInfos" : [ {
    "table" : {
      "catalog" : "hive",
      "schemaTable" : {
        "schema" : "default", 
        "table" : "orders_partitioned"
      }
    },
    "columnConstraints" : [ {
      "columnName" : "orderdate",
      "type" : "date",
      "domain" : {
        "nullsAllowed" : false,
        "ranges" : [ {
          "low" : {
            "value" : "2023-03-15",
            "bound" : "EXACTLY"  
          },
          "high" : {
            "value" : "2023-03-15", 
            "bound" : "EXACTLY"
          }
        } ]
      }
    } ],
    "estimate" : {
      "outputRowCount" : 50000,      // ✅ 只扫描单天数据
      "outputSizeInBytes" : 1600000, // ✅ 1.6MB vs 全表480MB
      "cpuCost" : 1600000.0,
      "memoryCost" : 0.0,
      "networkCost" : 0.0
    }
  } ],
  "estimate" : {
    "outputRowCount" : 50000,         // ✅ 分区裁剪后行数
    "outputSizeInBytes" : 1600000,    // ✅ 大幅减少IO
    "cpuCost" : 1600000.0,
    "memoryCost" : 0.0,
    "networkCost" : 0.0
  }
}
```

### 5.2 范围查询的分区裁剪

```sql
-- 查询一个月的数据  
SELECT COUNT(*) 
FROM orders_partitioned
WHERE orderdate BETWEEN DATE '2023-03-01' AND DATE '2023-03-31';
```

#### 分区裁剪日志查看
```sql
-- 在coordinator日志中查看分区裁剪信息
-- 2023-12-01T14:30:22.019Z INFO query.20231201_143022_00124
-- Partition pruning: 
--   Total partitions: 2555 
--   Pruned partitions: 2524
--   Remaining partitions: 31  ✅ 只扫描31个分区 (3月1-31日)
--   Pruning effectiveness: 98.8%
```

### 5.3 分区裁剪最佳实践

```sql
-- ✅ Good: 直接使用分区列
WHERE orderdate = DATE '2023-03-15'
WHERE orderdate >= DATE '2023-03-01' AND orderdate <= DATE '2023-03-31'

-- ❌ Bad: 对分区列使用函数
WHERE YEAR(orderdate) = 2023 AND MONTH(orderdate) = 3  -- 无法裁剪

-- ❌ Bad: 隐式类型转换
WHERE orderdate = '2023-03-15'  -- 可能无法裁剪，应使用DATE '2023-03-15'

-- ✅ Good: 复合分区列条件  
WHERE year = 2023 AND month = 3 AND day >= 15  -- 精确匹配分区结构
```

---

## 6. 统计信息对优化的影响

### 6.1 缺失统计信息的影响

```sql
-- 创建测试表，但不收集统计
CREATE TABLE test_orders AS SELECT * FROM orders WHERE orderdate >= DATE '2023-01-01';

-- 查询缺少统计的表
EXPLAIN SELECT o.*, c.name  
FROM test_orders o JOIN customer c ON o.custkey = c.custkey;
```

#### 缺失统计时的执行计划
```
Fragment 1 [HASH]
    - HashJoin[INNER][custkey, custkey] 
        │   Distribution: PARTITIONED  -- ❌ 错误选择分区join
        │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?} -- ❌ 无法估算
        ├─ TableScan[test_orders]
        │     Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?} -- ❌ 未知大小
        └─ RemoteSource[2]
        
Fragment 2 [HASH]
    Output partitioning: HASH [custkey] -- ❌ 小表也被重分区
    - TableScan[customer]
        Estimates: {rows: 1500000, cpu: 187MB, memory: 0B, network: 0B} -- ✅ 有统计
```

### 6.2 收集统计后的改善

```sql
-- 收集统计信息
ANALYZE TABLE test_orders;

-- 重新查看执行计划
EXPLAIN SELECT o.*, c.name 
FROM test_orders o JOIN customer c ON o.custkey = c.custkey;
```

#### 有统计信息的执行计划
```
Fragment 1 [BROADCAST]  -- ✅ 正确选择广播join
    - HashJoin[INNER][custkey, custkey]
        │   Distribution: BROADCAST -- ✅ customer表广播  
        │   Estimates: {rows: 92K, cpu: 14.5M, memory: 60MB, network: 180MB} -- ✅ 准确估算
        ├─ TableScan[test_orders]
        │     Estimates: {rows: 92500, cpu: 4.4M, memory: 0B, network: 0B} -- ✅ 准确估算
        └─ LocalExchange[BROADCAST]
            └─ TableScan[customer] -- ✅ customer不再重分区
```

### 6.3 统计信息质量检查

```sql
-- 检查表统计的新鲜度
SELECT 
    table_catalog,
    table_schema, 
    table_name,
    row_count,
    data_size,
    row_count_accuracy,
    CASE 
        WHEN row_count_accuracy < 0.8 THEN 'Need ANALYZE'
        WHEN row_count_accuracy < 0.95 THEN 'Recommend ANALYZE'  
        ELSE 'Good'
    END as recommendation
FROM system.metadata.table_statistics
WHERE table_schema = 'your_schema'
ORDER BY row_count_accuracy;

-- 检查列统计质量
SELECT
    table_name,
    column_name,
    distinct_values_count,
    nulls_fraction,
    CASE  
        WHEN distinct_values_count IS NULL THEN 'Missing'
        WHEN distinct_values_count = 0 THEN 'Inaccurate'
        ELSE 'Available'  
    END as ndv_status
FROM system.metadata.column_statistics  
WHERE schema_name = 'your_schema'
  AND distinct_values_count IS NULL  -- 找出缺失NDV的列
ORDER BY table_name, column_name;
```

---

## 7. 复杂查询调优实战

### 7.1 窗口函数查询优化

```sql
-- 复杂分析查询：每个客户的订单排名和累计金额
SELECT 
    custkey,
    orderkey, 
    orderdate,
    totalprice,
    ROW_NUMBER() OVER (PARTITION BY custkey ORDER BY orderdate DESC) as order_rank,
    SUM(totalprice) OVER (PARTITION BY custkey ORDER BY orderdate 
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
FROM orders
WHERE orderdate >= DATE '2023-01-01';
```

#### 优化前的执行计划分析
```sql
EXPLAIN (TYPE DISTRIBUTED) SELECT ...;
```

```
Fragment 0 [SINGLE]
    └─ RemoteSource[1]

Fragment 1 [HASH]
    Output partitioning: HASH [custkey]  -- ✅ 按custkey分区
    - Window  -- 窗口函数计算
        │   partition by: custkey
        │   order by: orderdate DESC  
        │   ROW_NUMBER() OVER (PARTITION BY custkey ORDER BY orderdate DESC)
        │   SUM(totalprice) OVER (PARTITION BY custkey ORDER BY orderdate ...)
        │   Estimates: {rows: 366K, cpu: 73M, memory: 17MB, network: 0B}
        └─ Sort[custkey ASC NULLS LAST, orderdate DESC NULLS LAST] -- ❌ 全排序开销大
               Estimates: {rows: 366K, cpu: 22M, memory: 17MB, network: 14MB}
               └─ RemoteSource[2]

Fragment 2 [HASH] 
    Output partitioning: HASH [custkey] -- 重分区保证同一客户在同一节点
    - TableScan[orders]
        Predicate: orderdate >= DATE '2023-01-01'
        Estimates: {rows: 366K, cpu: 17.6M, memory: 0B, network: 0B}
```

#### 优化策略

```sql
-- 1. 预先按客户分区存储数据
CREATE TABLE orders_by_customer 
WITH (
    partitioned_by = ARRAY['custkey_bucket'],
    bucketed_by = ARRAY['custkey'],  
    bucket_count = 256
) AS  
SELECT *, custkey % 256 as custkey_bucket 
FROM orders;

-- 2. 利用预排序减少窗口函数开销
CREATE TABLE orders_presorted
WITH (
    partitioned_by = ARRAY['custkey_bucket'],
    bucketed_by = ARRAY['custkey'],
    bucket_count = 256,
    sorted_by = ARRAY['custkey', 'orderdate']  -- ✅ 预排序
) AS
SELECT *, custkey % 256 as custkey_bucket
FROM orders  
ORDER BY custkey, orderdate;

-- 3. 使用预排序表重新执行查询
SELECT 
    custkey, orderkey, orderdate, totalprice,
    ROW_NUMBER() OVER (PARTITION BY custkey ORDER BY orderdate DESC) as order_rank,
    SUM(totalprice) OVER (PARTITION BY custkey ORDER BY orderdate 
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
FROM orders_presorted
WHERE orderdate >= DATE '2023-01-01';
```

#### 优化后的执行计划
```
Fragment 1 [HASH] 
    - Window  -- 窗口函数
        │   partition by: custkey
        │   Estimates: {rows: 366K, cpu: 11M, memory: 4MB, network: 0B} -- ✅ CPU和内存大幅减少
        └─ TableScan[orders_presorted] -- ✅ 无需额外排序
               Predicate: orderdate >= DATE '2023-01-01'
               Estimates: {rows: 366K, cpu: 17.6M, memory: 0B, network: 0B}
               -- ✅ 数据已按custkey分区，orderdate排序
```

### 7.2 子查询优化

#### 优化前：相关子查询
```sql  
-- 查找每个客户的最大订单
SELECT c.name, o.orderkey, o.totalprice
FROM customer c, orders o
WHERE c.custkey = o.custkey
  AND o.totalprice = (
    SELECT MAX(o2.totalprice) 
    FROM orders o2 
    WHERE o2.custkey = c.custkey  -- ❌ 相关子查询，为每行执行
  );
```

#### 优化后：窗口函数替代
```sql
-- 使用窗口函数重写
SELECT name, orderkey, totalprice
FROM (
    SELECT 
        c.name, 
        o.orderkey,
        o.totalprice,
        ROW_NUMBER() OVER (PARTITION BY c.custkey ORDER BY o.totalprice DESC) as rn
    FROM customer c
    JOIN orders o ON c.custkey = o.custkey
) ranked
WHERE rn = 1;  -- ✅ 只保留每个客户的最高订单
```

### 7.3 复合索引利用优化

```sql
-- 针对复合查询条件优化统计收集  
ANALYZE TABLE orders (custkey, orderdate, totalprice);  -- ✅ 收集多列统计

-- 查询会利用多列统计进行更准确的选择性估算
SELECT COUNT(*)
FROM orders  
WHERE custkey BETWEEN 1000 AND 2000    -- 选择性: ~6.7%
  AND orderdate >= DATE '2023-03-01'    -- 选择性: ~25%  
  AND totalprice > 100000;              -- 选择性: ~2%
  -- 组合选择性: 0.067 * 0.25 * 0.02 = 0.0003% (更精确的估算)
```

---

## 8. 性能问题诊断流程

### 8.1 问题定位Checklist

当查询性能不佳时，按以下步骤诊断：

#### Step 1: 收集基础信息
```sql
-- 1. 查看查询基本信息
SELECT 
    query_id,
    query,
    state,
    elapsed_time,
    queued_time,
    analysis_time,
    planning_time  
FROM system.runtime.queries
WHERE query LIKE '%your_table%' 
ORDER BY created DESC LIMIT 5;

-- 2. 查看资源使用
SELECT
    query_id, 
    peak_user_memory_bytes,
    peak_total_memory_bytes,  
    total_bytes,
    total_rows,
    wall_time
FROM system.runtime.queries  
WHERE query_id = 'your_query_id';
```

#### Step 2: 分析执行计划
```sql
-- 获取详细执行计划
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) <your_query>;

-- 重点检查：
-- 1. 是否有TableScan的行数估算异常
-- 2. Join策略选择是否合理 
-- 3. 是否有昂贵的Sort/Exchange操作
-- 4. 动态过滤是否生效
```

#### Step 3: 检查统计信息
```sql
-- 检查相关表的统计是否最新
SHOW STATS FOR your_table;

-- 如果统计过时，重新收集
ANALYZE TABLE your_table;
```

#### Step 4: 查看运行时性能
```sql
-- 查看各Stage的性能分布
SELECT 
    stage_id,
    state,
    total_tasks, 
    running_tasks,
    completed_tasks,
    total_splits,
    queued_splits,
    running_splits, 
    wall_time,
    total_cpu_time,
    raw_input_data_size,
    processed_input_data_size
FROM system.runtime.stages 
WHERE query_id = 'your_query_id'
ORDER BY stage_id;
```

### 8.2 常见性能问题及解决方案

#### 问题1: Join策略选择不当
```
症状: 小表被重分区，网络传输量大
解决:
1. SET SESSION join_distribution_type = 'BROADCAST';
2. 收集准确的表统计信息
3. 调整broadcast_join_threshold参数
```

#### 问题2: 缺少分区裁剪
```
症状: 扫描大量不相关分区，IO开销大  
解决:
1. 确保分区列在WHERE条件中
2. 避免对分区列使用函数
3. 使用正确的数据类型匹配
```

#### 问题3: 统计信息过时
```
症状: 行数估算严重偏差，join顺序不当
解决:  
1. 定期ANALYZE关键表
2. 监控统计信息准确度
3. 考虑自动统计收集
```

#### 问题4: 数据倾斜
```sql
-- 检查数据倾斜
SELECT 
    custkey, 
    COUNT(*) as order_count
FROM orders 
GROUP BY custkey 
ORDER BY order_count DESC 
LIMIT 20;  -- 查看热点客户

-- 解决数据倾斜的策略
-- 1. 预处理热点数据
-- 2. 使用随机前缀打散
-- 3. 调整并行度
SET SESSION task_concurrency = 16;
```

### 8.3 性能监控设置

```sql
-- 创建性能监控视图
CREATE VIEW query_performance AS
SELECT 
    date_trunc('hour', created) as hour,
    COUNT(*) as query_count,
    AVG(elapsed_time.seconds) as avg_elapsed_seconds,
    PERCENTILE(elapsed_time.seconds, 0.95) as p95_elapsed_seconds,
    AVG(peak_user_memory_bytes / 1024.0 / 1024.0) as avg_memory_mb,
    SUM(total_bytes) as total_data_bytes
FROM system.runtime.queries
WHERE state = 'FINISHED'
  AND created >= current_timestamp - INTERVAL '7' DAY
GROUP BY 1
ORDER BY 1 DESC;

-- 查看性能趋势  
SELECT * FROM query_performance LIMIT 24;  -- 最近24小时
```

---

## 📋 总结

通过这些实战案例，我们看到了：

1. **EXPLAIN是调优的起点**：学会读懂执行计划，识别性能瓶颈
2. **统计信息至关重要**：准确的统计是CBO优化的基础
3. **Join策略影响巨大**：合理的分布策略能带来数量级的性能提升
4. **动态过滤威力惊人**：在星型模式查询中能过滤90%+的无效数据
5. **分区裁剪不可忽视**：正确的分区设计和查询方式能大幅减少IO
6. **问题诊断要系统化**：按流程逐步排查，定位根本原因

掌握这些技能，你就能够：
- **快速诊断**查询性能问题
- **有效优化**复杂分析查询  
- **合理设计**数据模型和分区策略
- **持续监控**和改进系统性能

希望这些实战案例能帮助你在实际工作中游刃有余地应对Trino性能优化挑战！🚀
