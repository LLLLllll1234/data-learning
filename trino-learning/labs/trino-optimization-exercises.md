# Trino 优化器实战练习与实验

## 🎯 练习概述

这套实战练习旨在帮助你通过实际操作掌握Trino优化器的核心概念和调优技巧。每个练习都包含：
- 🎯 **学习目标**：明确要掌握的知识点
- 📋 **前置准备**：需要的数据和环境设置
- 🔬 **实验步骤**：详细的操作指导
- 💡 **分析与总结**：结果解读和知识点巩固

---

## 📚 练习目录

1. [练习1: EXPLAIN计划解读与性能基线](#练习1-explain计划解读与性能基线)
2. [练习2: 统计信息对优化的影响](#练习2-统计信息对优化的影响)
3. [练习3: Join策略选择实验](#练习3-join策略选择实验)
4. [练习4: 动态过滤效果验证](#练习4-动态过滤效果验证)
5. [练习5: 分区裁剪优化实验](#练习5-分区裁剪优化实验)
6. [练习6: 查询重写与规则优化](#练习6-查询重写与规则优化)
7. [练习7: 复杂查询端到端优化](#练习7-复杂查询端到端优化)
8. [练习8: 性能监控与诊断](#练习8-性能监控与诊断)

---

## 练习1: EXPLAIN计划解读与性能基线

### 🎯 学习目标
- 掌握EXPLAIN命令的不同选项
- 学会读懂执行计划的各个组成部分
- 理解代价估算的含义和影响
- 建立查询性能分析的基础技能

### 📋 前置准备

#### 数据准备
```sql
-- 创建测试表 (使用TPC-H标准数据集)
-- 如果没有现成的数据，可以生成小规模测试数据

-- 客户表
CREATE TABLE customer_test AS 
SELECT 
    row_number() OVER () as custkey,
    'Customer' || cast(row_number() OVER () as varchar) as name,
    'Address' || cast(row_number() OVER () as varchar) as address,
    cast(random() * 25 as integer) as nationkey,
    '123-456-7890' as phone,
    random() * 10000 as acctbal,
    case when random() < 0.2 then 'BUILDING' 
         when random() < 0.4 then 'AUTOMOBILE' 
         when random() < 0.6 then 'MACHINERY'
         when random() < 0.8 then 'HOUSEHOLD' 
         else 'FURNITURE' end as mktsegment,
    'Comment' as comment
FROM unnest(sequence(1, 150000)) as t(n);

-- 订单表  
CREATE TABLE orders_test AS
SELECT 
    row_number() OVER () as orderkey,
    cast(random() * 150000 + 1 as integer) as custkey,
    case when random() < 0.2 then 'O'
         when random() < 0.5 then 'F'
         else 'P' end as orderstatus,
    random() * 500000 as totalprice,
    date_add('day', cast(random() * 2557 as integer), date '1992-01-01') as orderdate,
    cast(random() * 5 + 1 as varchar) || '-HIGH' as orderpriority,
    'Clerk#' || cast(random() * 1000 as varchar) as clerk,
    cast(random() * 7 as integer) as shippriority,
    'Comment' as comment
FROM unnest(sequence(1, 1500000)) as t(n);
```

### 🔬 实验步骤

#### 步骤1: 基础EXPLAIN分析
```sql
-- 1.1 查看基本逻辑计划
EXPLAIN 
SELECT c.name, o.totalprice 
FROM customer_test c 
JOIN orders_test o ON c.custkey = o.custkey 
WHERE o.totalprice > 100000;

-- 📝 记录观察结果:
-- - 算子类型有哪些？
-- - Join算子的分布策略是什么？
-- - 过滤条件在哪个位置？
```

#### 步骤2: 分布式执行计划分析
```sql
-- 1.2 查看分布式执行计划
EXPLAIN (TYPE DISTRIBUTED)
SELECT c.name, o.totalprice 
FROM customer_test c 
JOIN orders_test o ON c.custkey = o.custkey 
WHERE o.totalprice > 100000;

-- 📝 分析要点:
-- - 有几个Fragment？每个Fragment的作用是什么？
-- - 数据是如何在Fragment间流动的？
-- - Exchange类型是什么 (BROADCAST/HASH/SINGLE)？
```

#### 步骤3: 详细统计分析
```sql
-- 1.3 查看详细统计和代价
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)
SELECT c.name, o.totalprice 
FROM customer_test c 
JOIN orders_test o ON c.custkey = o.custkey 
WHERE o.totalprice > 100000;

-- 📝 重点关注:
-- - estimates字段中的行数估算是否合理？
-- - CPU、内存、网络代价的分布如何？
-- - 哪个算子的代价最高？
```

#### 步骤4: IO统计分析
```sql
-- 1.4 查看IO统计
EXPLAIN (TYPE IO)
SELECT c.name, o.totalprice 
FROM customer_test c 
JOIN orders_test o ON c.custkey = o.custkey 
WHERE o.totalprice > 100000;

-- 📝 分析IO模式:
-- - 每个表扫描了多少数据？
-- - 过滤效果如何 (扫描行数 vs 输出行数)？
-- - 是否有分区裁剪的信息？
```

### 💡 练习任务

**任务A**: 对比不同查询的执行计划
```sql
-- 查询1: 简单过滤
SELECT * FROM orders_test WHERE totalprice > 100000;

-- 查询2: 简单聚合
SELECT custkey, COUNT(*), SUM(totalprice) 
FROM orders_test GROUP BY custkey;

-- 查询3: 复杂Join
SELECT c.name, COUNT(*) as order_count
FROM customer_test c 
LEFT JOIN orders_test o ON c.custkey = o.custkey 
GROUP BY c.name;

-- 🎯 分析每个查询的Fragment数量、算子类型和代价分布
```

**任务B**: 代价估算准确性验证
```sql
-- 执行查询并记录实际性能
SELECT c.name, o.totalprice 
FROM customer_test c 
JOIN orders_test o ON c.custkey = o.custkey 
WHERE o.totalprice > 100000;

-- 🎯 对比EXPLAIN的估算与实际执行时间、处理行数
-- 🎯 思考：估算偏差的可能原因？
```

### 📊 结果记录表格

| 指标 | EXPLAIN估算 | 实际执行 | 偏差率 | 备注 |
|------|-------------|----------|--------|------|
| 总行数 |  |  |  |  |
| 执行时间 |  |  |  |  |
| 内存使用 |  |  |  |  |
| CPU代价 |  |  |  |  |

---

## 练习2: 统计信息对优化的影响

### 🎯 学习目标
- 理解统计信息在CBO中的重要性
- 掌握ANALYZE命令的使用
- 观察统计信息缺失对查询计划的影响
- 学会评估统计信息质量

### 🔬 实验步骤

#### 步骤1: 清空统计信息基线测试
```sql
-- 2.1 创建没有统计信息的测试表
CREATE TABLE orders_no_stats AS 
SELECT * FROM orders_test WHERE orderdate >= DATE '1995-01-01';

-- 查看当前统计状态
SHOW STATS FOR orders_no_stats;

-- 🎯 记录：row_count和data_size是否为null？
```

#### 步骤2: 无统计信息的查询计划
```sql
-- 2.2 分析没有统计信息时的执行计划
EXPLAIN (TYPE DISTRIBUTED)
SELECT custkey, COUNT(*) as order_count, SUM(totalprice) as total_revenue
FROM orders_no_stats 
GROUP BY custkey 
HAVING COUNT(*) > 10;

-- 📝 记录观察：
-- - 行数估算显示为多少？
-- - 算子选择是否合理？
-- - 有没有看到 "?" 或异常的估算值？
```

#### 步骤3: 收集统计信息
```sql
-- 2.3 收集表统计信息
ANALYZE TABLE orders_no_stats;

-- 查看收集后的统计
SHOW STATS FOR orders_no_stats;

-- 🎯 记录变化：
-- - row_count现在显示多少？
-- - distinct_values_count如何？
-- - data_size是多少？
```

#### 步骤4: 有统计信息的查询计划对比
```sql
-- 2.4 重新分析相同查询的执行计划
EXPLAIN (TYPE DISTRIBUTED)
SELECT custkey, COUNT(*) as order_count, SUM(totalprice) as total_revenue
FROM orders_no_stats 
GROUP BY custkey 
HAVING COUNT(*) > 10;

-- 🎯 对比分析：
-- - 行数估算是否更准确？
-- - 算子选择有什么变化？
-- - CPU/内存代价估算有什么不同？
```

### 💡 进阶实验

#### 实验A: 不同采样率的影响
```sql
-- 创建大表测试采样效果
CREATE TABLE large_orders AS 
SELECT * FROM orders_test o1, orders_test o2 
WHERE o1.orderkey % 100 = o2.orderkey % 100;

-- 不同采样率收集统计
ANALYZE TABLE large_orders WITH (sample_percentage = 1.0);   -- 1%采样
ANALYZE TABLE large_orders WITH (sample_percentage = 5.0);   -- 5%采样  
ANALYZE TABLE large_orders WITH (sample_percentage = 10.0);  -- 10%采样

-- 🎯 对比不同采样率下的统计准确性和收集时间
```

#### 实验B: 列统计信息的影响
```sql
-- 只收集特定列的统计
ANALYZE TABLE orders_no_stats (custkey, totalprice, orderdate);

-- 对比全表统计和部分列统计的差异
-- 🎯 观察Join和聚合操作的计划差异
```

### 📋 实验记录

#### 统计信息对比表
| 场景 | 行数估算 | Join策略 | 聚合方式 | 执行时间 | 备注 |
|------|----------|----------|----------|----------|------|
| 无统计 |  |  |  |  |  |
| 有统计 |  |  |  |  |  |
| 1%采样 |  |  |  |  |  |
| 10%采样 |  |  |  |  |  |

---

## 练习3: Join策略选择实验

### 🎯 学习目标
- 理解广播Join vs 分区Join的选择机制
- 掌握手动控制Join策略的方法
- 观察不同策略对性能的影响
- 学会分析Join代价权衡

### 🔬 实验步骤

#### 步骤1: 创建不同规模的表
```sql
-- 3.1 创建小表 (1万行)
CREATE TABLE small_customers AS
SELECT * FROM customer_test WHERE custkey <= 10000;

-- 创建大表 (100万行)  
CREATE TABLE large_orders AS
SELECT * FROM orders_test WHERE orderkey <= 1000000;

-- 收集统计信息
ANALYZE TABLE small_customers;
ANALYZE TABLE large_orders;
```

#### 步骤2: 自动策略选择观察
```sql
-- 3.2 让优化器自动选择Join策略
SET SESSION join_distribution_type = 'AUTOMATIC';

EXPLAIN (TYPE DISTRIBUTED)
SELECT c.name, COUNT(*) as order_count
FROM small_customers c
JOIN large_orders o ON c.custkey = o.custkey
GROUP BY c.name;

-- 📝 记录：
-- - 优化器选择了什么Join策略？(BROADCAST/PARTITIONED)
-- - Fragment的数量和分布？
-- - 网络传输的数据量估算？
```

#### 步骤3: 强制广播Join
```sql
-- 3.3 强制使用广播Join
SET SESSION join_distribution_type = 'BROADCAST';

EXPLAIN (TYPE DISTRIBUTED)
SELECT c.name, COUNT(*) as order_count
FROM small_customers c
JOIN large_orders o ON c.custkey = o.custkey
GROUP BY c.name;

-- 执行并记录性能
-- 🎯 观察：
-- - 小表是否被广播到所有节点？
-- - 内存使用估算如何？
-- - 执行时间和网络开销？
```

#### 步骤4: 强制分区Join
```sql
-- 3.4 强制使用分区Join
SET SESSION join_distribution_type = 'PARTITIONED';

EXPLAIN (TYPE DISTRIBUTED)
SELECT c.name, COUNT(*) as order_count
FROM small_customers c
JOIN large_orders o ON c.custkey = o.custkey
GROUP BY c.name;

-- 📝 对比分析：
-- - 两个表都需要重分区吗？
-- - 网络传输量与广播策略的对比？
-- - 内存使用的差异？
```

### 💡 挑战实验

#### 实验A: 阈值边界测试
```sql
-- 查看当前广播阈值
SHOW SESSION LIKE 'broadcast_join_threshold';

-- 调整阈值测试策略变化
SET SESSION broadcast_join_threshold = '1MB';   -- 很小的阈值
-- 重新执行Join查询，观察策略变化

SET SESSION broadcast_join_threshold = '1GB';   -- 很大的阈值  
-- 再次测试策略选择
```

#### 实验B: 复杂多表Join策略
```sql
-- 三表Join策略分析
CREATE TABLE nation_test AS
SELECT 
    row_number() OVER () as nationkey,
    'Nation' || cast(row_number() OVER () as varchar) as name,
    cast(random() * 5 as integer) as regionkey,
    'Comment' as comment
FROM unnest(sequence(1, 25)) as t(n);

ANALYZE TABLE nation_test;

-- 分析三表Join的策略选择
EXPLAIN (TYPE DISTRIBUTED)
SELECT n.name, c.name, COUNT(*) as order_count
FROM nation_test n
JOIN small_customers c ON n.nationkey = c.nationkey
JOIN large_orders o ON c.custkey = o.custkey
GROUP BY n.name, c.name;

-- 🎯 观察：
-- - Join顺序如何？
-- - 每个Join使用了什么策略？
-- - 有没有级联的广播？
```

### 📊 性能对比表

| Join策略 | 网络传输 | 内存峰值 | 执行时间 | CPU使用 | 适用场景 |
|----------|----------|----------|----------|---------|----------|
| AUTOMATIC |  |  |  |  |  |
| BROADCAST |  |  |  |  |  |
| PARTITIONED |  |  |  |  |  |

---

## 练习4: 动态过滤效果验证

### 🎯 学习目标
- 理解动态过滤的工作原理
- 观察动态过滤对查询性能的提升
- 掌握动态过滤的使用场景
- 学会分析动态过滤的效果

### 🔬 实验步骤

#### 步骤1: 创建星型模式测试数据
```sql
-- 4.1 创建事实表 (大表)
CREATE TABLE sales_fact AS
SELECT 
    row_number() OVER () as sale_id,
    cast(random() * 10000 + 1 as integer) as customer_id,
    cast(random() * 1000 + 1 as integer) as product_id,
    date_add('day', cast(random() * 365 as integer), date '2023-01-01') as sale_date,
    random() * 1000 as amount
FROM unnest(sequence(1, 5000000)) as t(n);  -- 500万行

-- 创建维表 (小表)
CREATE TABLE customer_dim AS
SELECT 
    row_number() OVER () as customer_id,
    'Customer' || cast(row_number() OVER () as varchar) as customer_name,
    case when random() < 0.1 then 'VIP'
         when random() < 0.3 then 'GOLD' 
         else 'REGULAR' end as customer_type,
    case when random() < 0.05 then 'BUILDING'
         else 'OTHER' end as segment
FROM unnest(sequence(1, 10000)) as t(n);

ANALYZE TABLE sales_fact;
ANALYZE TABLE customer_dim;
```

#### 步骤2: 禁用动态过滤的基线测试
```sql
-- 4.2 禁用动态过滤
SET SESSION enable_dynamic_filtering = false;

-- 执行星型模式查询
EXPLAIN (TYPE DISTRIBUTED)
SELECT 
    c.customer_name,
    SUM(s.amount) as total_sales
FROM sales_fact s
JOIN customer_dim c ON s.customer_id = c.customer_id
WHERE c.customer_type = 'VIP'
  AND s.sale_date >= DATE '2023-06-01'
GROUP BY c.customer_name;

-- 📝 记录基线性能：
-- - 事实表扫描的行数估算？
-- - Join的代价估算？
-- - 总执行时间？
```

#### 步骤3: 启用动态过滤测试
```sql
-- 4.3 启用动态过滤
SET SESSION enable_dynamic_filtering = true;

EXPLAIN (TYPE DISTRIBUTED)
SELECT 
    c.customer_name,
    SUM(s.amount) as total_sales
FROM sales_fact s
JOIN customer_dim c ON s.customer_id = c.customer_id
WHERE c.customer_type = 'VIP'
  AND s.sale_date >= DATE '2023-06-01'
GROUP BY c.customer_name;

-- 🎯 寻找动态过滤的证据：
-- - 执行计划中是否出现 "DynamicFilter" 关键字？
-- - 事实表扫描的行数估算是否减少？
-- - Fragment之间的数据传输量变化？
```

#### 步骤4: 动态过滤效果测量
```sql
-- 4.4 实际执行查询测量性能差异

-- 禁用动态过滤执行
SET SESSION enable_dynamic_filtering = false;
-- 记录执行时间和处理数据量

-- 启用动态过滤执行  
SET SESSION enable_dynamic_filtering = true;
-- 记录执行时间和处理数据量

-- 🎯 计算性能提升比例
```

### 💡 深度实验

#### 实验A: 不同选择性的动态过滤效果
```sql
-- 测试不同过滤选择性的动态过滤效果

-- 高选择性 (1%数据)
SELECT c.customer_name, SUM(s.amount) 
FROM sales_fact s JOIN customer_dim c ON s.customer_id = c.customer_id
WHERE c.segment = 'BUILDING';

-- 中等选择性 (10%数据) 
SELECT c.customer_name, SUM(s.amount)
FROM sales_fact s JOIN customer_dim c ON s.customer_id = c.customer_id  
WHERE c.customer_type = 'VIP';

-- 低选择性 (90%数据)
SELECT c.customer_name, SUM(s.amount)
FROM sales_fact s JOIN customer_dim c ON s.customer_id = c.customer_id
WHERE c.customer_type IN ('VIP', 'GOLD', 'REGULAR');

-- 🎯 观察动态过滤在不同选择性下的效果差异
```

#### 实验B: 多级动态过滤
```sql
-- 创建三级联结查询测试多级动态过滤
CREATE TABLE product_dim AS
SELECT 
    row_number() OVER () as product_id,
    'Product' || cast(row_number() OVER () as varchar) as product_name,
    case when random() < 0.1 then 'ELECTRONICS'
         when random() < 0.3 then 'CLOTHING'
         else 'OTHER' end as category
FROM unnest(sequence(1, 1000)) as t(n);

ANALYZE TABLE product_dim;

-- 三表Join分析动态过滤级联效果
EXPLAIN (TYPE DISTRIBUTED)
SELECT 
    c.customer_name,
    p.product_name, 
    SUM(s.amount) as total_sales
FROM sales_fact s
JOIN customer_dim c ON s.customer_id = c.customer_id
JOIN product_dim p ON s.product_id = p.product_id  
WHERE c.customer_type = 'VIP'
  AND p.category = 'ELECTRONICS';
```

### 📈 动态过滤效果记录

| 测试场景 | 无动态过滤 | 有动态过滤 | 性能提升 | 数据过滤率 |
|----------|------------|------------|----------|------------|
| 高选择性(1%) |  |  |  |  |
| 中选择性(10%) |  |  |  |  |
| 低选择性(90%) |  |  |  |  |
| 三表Join |  |  |  |  |

---

## 练习5: 分区裁剪优化实验

### 🎯 学习目标
- 理解分区裁剪的工作机制
- 掌握分区表的设计原则
- 观察不同查询模式的分区裁剪效果
- 学会优化分区策略

### 🔬 实验步骤

#### 步骤1: 创建分区表
```sql
-- 5.1 创建按日期分区的订单表
CREATE TABLE orders_partitioned (
    orderkey bigint,
    custkey bigint,
    orderstatus varchar(1),
    totalprice double,
    orderdate date,
    orderpriority varchar(15),
    clerk varchar(15),
    shippriority integer,
    comment varchar(79)
) WITH (
    partitioned_by = ARRAY['year', 'month'],
    format = 'PARQUET'
);

-- 插入不同年月的测试数据
INSERT INTO orders_partitioned 
SELECT 
    orderkey, custkey, orderstatus, totalprice, orderdate,
    orderpriority, clerk, shippriority, comment,
    year(orderdate) as year,
    month(orderdate) as month
FROM orders_test;
```

#### 步骤2: 分区裁剪效果测试
```sql
-- 5.2 测试不同查询条件的分区裁剪

-- 精确日期查询 (应该只扫描1个分区)
EXPLAIN (TYPE IO)
SELECT COUNT(*) 
FROM orders_partitioned 
WHERE year = 2023 AND month = 6;

-- 📝 记录：
-- - 扫描了多少个分区？
-- - 估算处理多少行数据？
-- - 与全表扫描的差异？

-- 范围日期查询 (应该扫描多个连续分区)
EXPLAIN (TYPE IO)
SELECT COUNT(*)
FROM orders_partitioned
WHERE year = 2023 AND month BETWEEN 3 AND 6;

-- 📝 记录分区裁剪效果
```

#### 步骤3: 无效分区裁剪案例
```sql
-- 5.3 测试导致无法分区裁剪的查询模式

-- 在分区列上使用函数 (无法裁剪)
EXPLAIN (TYPE IO)
SELECT COUNT(*)  
FROM orders_partitioned
WHERE CONCAT(cast(year as varchar), '-', cast(month as varchar)) = '2023-6';

-- 隐式类型转换 (可能无法裁剪)
EXPLAIN (TYPE IO) 
SELECT COUNT(*)
FROM orders_partitioned  
WHERE year = '2023';  -- 字符串比较整数

-- 📝 对比：为什么这些查询无法有效利用分区裁剪？
```

### 💡 优化实验

#### 实验A: 分区策略对比
```sql
-- 创建不同分区策略的表进行对比

-- 按年分区 (粗粒度)
CREATE TABLE orders_by_year AS
SELECT *, year(orderdate) as year
FROM orders_test;

-- 按年月日分区 (细粒度)  
CREATE TABLE orders_by_date AS
SELECT *, year(orderdate) as year, month(orderdate) as month, day(orderdate) as day
FROM orders_test;

-- 测试相同查询在不同分区粒度下的效果
-- 🎯 分析分区粒度对查询性能的影响
```

#### 实验B: 复合分区条件优化
```sql
-- 测试复合分区条件的优化

-- 好的查询模式 (直接使用分区列)
EXPLAIN (TYPE IO)
SELECT custkey, SUM(totalprice)
FROM orders_partitioned  
WHERE year = 2023 AND month >= 6
GROUP BY custkey;

-- 改进的查询模式 (添加分区列条件)
EXPLAIN (TYPE IO)
SELECT custkey, SUM(totalprice)
FROM orders_partitioned
WHERE orderdate >= DATE '2023-06-01' 
  AND year = 2023 AND month >= 6  -- 显式添加分区条件
GROUP BY custkey;

-- 🎯 对比两种写法的分区裁剪效果
```

### 📊 分区裁剪效果对比

| 查询类型 | 扫描分区数 | 扫描行数 | 裁剪率 | 执行时间 | 备注 |
|----------|------------|----------|--------|----------|------|
| 精确分区匹配 |  |  |  |  |  |
| 范围分区查询 |  |  |  |  |  |
| 函数条件查询 |  |  |  |  |  |
| 全表扫描 |  |  |  |  |  |

---

## 练习6: 查询重写与规则优化

### 🎯 学习目标
- 理解查询重写规则的作用
- 学会编写优化友好的SQL
- 掌握子查询优化技巧
- 观察规则优化的效果

### 🔬 实验步骤

#### 步骤1: 子查询重写实验
```sql
-- 6.1 相关子查询 vs 窗口函数重写

-- 原始相关子查询 (性能较差)
EXPLAIN (TYPE DISTRIBUTED)
SELECT c.name, o.orderkey, o.totalprice
FROM customer_test c, orders_test o
WHERE c.custkey = o.custkey
  AND o.totalprice = (
    SELECT MAX(o2.totalprice) 
    FROM orders_test o2 
    WHERE o2.custkey = c.custkey  -- 相关条件
  );

-- 📝 记录：
-- - 执行计划有多复杂？
-- - 是否有嵌套循环或重复扫描？

-- 重写为窗口函数 (性能更好)
EXPLAIN (TYPE DISTRIBUTED)
WITH ranked_orders AS (
  SELECT 
    c.name, o.orderkey, o.totalprice, o.custkey,
    ROW_NUMBER() OVER (PARTITION BY o.custkey ORDER BY o.totalprice DESC) as rn
  FROM customer_test c
  JOIN orders_test o ON c.custkey = o.custkey
)
SELECT name, orderkey, totalprice
FROM ranked_orders 
WHERE rn = 1;

-- 🎯 对比两种写法的执行计划复杂度和性能
```

#### 步骤2: EXISTS vs JOIN重写
```sql
-- 6.2 测试EXISTS与JOIN的重写

-- 使用EXISTS (语义清晰但可能性能较差)
EXPLAIN (TYPE DISTRIBUTED)
SELECT c.name, c.custkey
FROM customer_test c
WHERE EXISTS (
  SELECT 1 FROM orders_test o 
  WHERE o.custkey = c.custkey 
    AND o.totalprice > 100000
);

-- 重写为JOIN (通常性能更好)
EXPLAIN (TYPE DISTRIBUTED)  
SELECT DISTINCT c.name, c.custkey
FROM customer_test c
JOIN orders_test o ON c.custkey = o.custkey
WHERE o.totalprice > 100000;

-- 重写为半连接 (推荐写法)
EXPLAIN (TYPE DISTRIBUTED)
SELECT c.name, c.custkey  
FROM customer_test c
WHERE c.custkey IN (
  SELECT DISTINCT o.custkey 
  FROM orders_test o 
  WHERE o.totalprice > 100000
);
```

#### 步骤3: 聚合下推优化
```sql
-- 6.3 聚合下推实验

-- 未优化的聚合查询 (先Join后聚合)
EXPLAIN (TYPE DISTRIBUTED)
SELECT 
  c.mktsegment,
  AVG(order_summary.total_amount) as avg_customer_total
FROM customer_test c
JOIN (
  SELECT custkey, SUM(totalprice) as total_amount
  FROM orders_test 
  GROUP BY custkey
) order_summary ON c.custkey = order_summary.custkey
GROUP BY c.mktsegment;

-- 📝 观察：
-- - 聚合操作在哪些阶段进行？
-- - 是否有部分聚合(partial aggregation)？
-- - 数据传输量如何？
```

### 💡 优化挑战

#### 挑战A: 复杂查询优化重写
```sql
-- 原始复杂查询 (包含多种反模式)
SELECT 
    c.name,
    (SELECT COUNT(*) FROM orders_test o1 WHERE o1.custkey = c.custkey) as total_orders,
    (SELECT AVG(o2.totalprice) FROM orders_test o2 WHERE o2.custkey = c.custkey) as avg_price,
    CASE WHEN c.acctbal > (SELECT AVG(acctbal) FROM customer_test) THEN 'HIGH' ELSE 'LOW' END as balance_level
FROM customer_test c
WHERE c.mktsegment = 'BUILDING'
  AND c.custkey IN (SELECT custkey FROM orders_test WHERE totalprice > 50000);

-- 🎯 挑战任务：
-- 1. 识别查询中的性能问题
-- 2. 重写为优化的版本
-- 3. 对比执行计划和性能差异
```

#### 挑战B: 自定义优化规则验证
```sql
-- 验证常见优化规则的效果

-- 规则1: 常量折叠
EXPLAIN SELECT * FROM orders_test WHERE 1=1 AND totalprice > 100;
EXPLAIN SELECT * FROM orders_test WHERE totalprice > 100;

-- 规则2: 谓词下推  
EXPLAIN SELECT * FROM (SELECT * FROM orders_test WHERE orderkey > 1000) WHERE totalprice > 100;

-- 规则3: 投影下推
EXPLAIN SELECT custkey FROM (SELECT * FROM orders_test) WHERE orderkey < 1000;

-- 🎯 观察优化器是否应用了这些规则
```

---

## 练习7: 复杂查询端到端优化

### 🎯 学习目标
- 综合应用所有优化技术
- 掌握复杂查询的系统性优化方法
- 学会制定优化策略和实施计划
- 建立完整的性能调优思维

### 🔬 综合实验

#### 实验场景: TPC-H Q5 复杂分析查询优化
```sql
-- 7.1 原始复杂查询 (TPC-H Query 5变体)
SELECT 
    n.name as nation_name,
    SUM(l.extendedprice * (1 - l.discount)) as revenue
FROM customer_test c
JOIN orders_test o ON c.custkey = o.custkey
JOIN lineitem_test l ON o.orderkey = l.orderkey  -- 需要先创建lineitem表
JOIN supplier_test s ON l.suppkey = s.suppkey
JOIN nation_test n ON s.nationkey = n.nationkey
JOIN region_test r ON n.regionkey = r.regionkey
WHERE r.name = 'ASIA'
  AND o.orderdate >= DATE '1994-01-01'
  AND o.orderdate < DATE '1995-01-01'
GROUP BY n.name
ORDER BY revenue DESC;
```

#### 步骤1: 基线性能测试
```sql
-- 创建必要的测试数据
-- (为了练习，创建简化版本的lineitem等表)

-- 创建供应商表
CREATE TABLE supplier_test AS
SELECT 
    row_number() OVER () as suppkey,
    'Supplier#' || cast(row_number() OVER () as varchar) as name,
    cast(random() * 25 as integer) as nationkey,
    'Address' as address,
    '123-456-7890' as phone,
    random() * 10000 as acctbal,
    'Comment' as comment
FROM unnest(sequence(1, 10000)) as t(n);

-- 创建订单行表 (大表)
CREATE TABLE lineitem_test AS  
SELECT 
    cast(random() * 1500000 + 1 as integer) as orderkey,
    cast(random() * 7 + 1 as integer) as linenumber,
    cast(random() * 200000 + 1 as integer) as partkey,
    cast(random() * 10000 + 1 as integer) as suppkey,
    cast(random() * 50 + 1 as integer) as quantity,
    random() * 100000 as extendedprice,
    random() * 0.1 as discount,
    random() * 0.08 as tax,
    'N' as returnflag,
    'O' as linestatus,
    date_add('day', cast(random() * 121 as integer), date '1994-01-01') as shipdate,
    date_add('day', cast(random() * 121 as integer), date '1994-01-01') as commitdate,
    date_add('day', cast(random() * 121 as integer), date '1994-01-01') as receiptdate,
    'MAIL' as shipinstruct,
    'AIR' as shipmode,
    'Comment' as comment
FROM unnest(sequence(1, 6000000)) as t(n);

-- 创建区域表
CREATE TABLE region_test AS
SELECT 
    row_number() OVER () as regionkey,
    CASE row_number() OVER () 
      WHEN 1 THEN 'AFRICA'
      WHEN 2 THEN 'AMERICA' 
      WHEN 3 THEN 'ASIA'
      WHEN 4 THEN 'EUROPE'
      WHEN 5 THEN 'MIDDLE EAST'
    END as name,
    'Comment' as comment
FROM unnest(sequence(1, 5)) as t(n);

-- 收集所有表的统计信息
ANALYZE TABLE supplier_test;
ANALYZE TABLE lineitem_test; 
ANALYZE TABLE region_test;
```

#### 步骤2: 系统性优化过程

**阶段1: 执行计划分析**
```sql
-- 分析原始查询的执行计划
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)
SELECT 
    n.name as nation_name,
    SUM(l.extendedprice * (1 - l.discount)) as revenue  
FROM customer_test c
JOIN orders_test o ON c.custkey = o.custkey
JOIN lineitem_test l ON o.orderkey = l.orderkey
JOIN supplier_test s ON l.suppkey = s.suppkey  
JOIN nation_test n ON s.nationkey = n.nationkey
JOIN region_test r ON n.regionkey = r.regionkey
WHERE r.name = 'ASIA'
  AND o.orderdate >= DATE '1994-01-01'
  AND o.orderdate < DATE '1995-01-01'
GROUP BY n.name
ORDER BY revenue DESC;

-- 📝 分析要点：
-- 1. Join顺序是否合理？
-- 2. 哪些Join使用了广播策略？
-- 3. 过滤条件是否被充分下推？
-- 4. 最大的性能瓶颈在哪个算子？
```

**阶段2: 统计信息优化**
```sql
-- 检查关键Join列的统计质量
SHOW STATS FOR customer_test;
SHOW STATS FOR orders_test;
SHOW STATS FOR lineitem_test;

-- 针对性收集高基数列的统计
ANALYZE TABLE lineitem_test (orderkey, suppkey);
ANALYZE TABLE orders_test (custkey, orderkey);
```

**阶段3: 会话参数调优**
```sql
-- 针对复杂查询的参数优化
SET SESSION enable_dynamic_filtering = true;
SET SESSION join_reordering_strategy = 'AUTOMATIC';
SET SESSION join_distribution_type = 'AUTOMATIC';  
SET SESSION query_max_memory_per_node = '4GB';
```

**阶段4: 查询重写优化**
```sql
-- 重写版本1: 预过滤优化
WITH filtered_orders AS (
  SELECT o.orderkey, o.custkey, o.orderdate
  FROM orders_test o  
  WHERE o.orderdate >= DATE '1994-01-01'
    AND o.orderdate < DATE '1995-01-01'
),
asia_suppliers AS (
  SELECT s.suppkey, n.name as nation_name
  FROM supplier_test s
  JOIN nation_test n ON s.nationkey = n.nationkey
  JOIN region_test r ON n.regionkey = r.regionkey  
  WHERE r.name = 'ASIA'
)
SELECT 
    asia_suppliers.nation_name,
    SUM(l.extendedprice * (1 - l.discount)) as revenue
FROM filtered_orders o
JOIN customer_test c ON o.custkey = c.custkey
JOIN lineitem_test l ON o.orderkey = l.orderkey
JOIN asia_suppliers ON l.suppkey = asia_suppliers.suppkey
GROUP BY asia_suppliers.nation_name
ORDER BY revenue DESC;

-- 🎯 对比重写前后的执行计划差异
```

### 📊 优化效果跟踪表

| 优化阶段 | 执行时间 | 处理行数 | 内存使用 | 网络传输 | 主要改进 |
|----------|----------|----------|----------|----------|----------|
| 基线查询 |  |  |  |  | 原始查询 |
| 统计优化 |  |  |  |  | ANALYZE后 |
| 参数调优 |  |  |  |  | 会话参数优化 |
| 查询重写 |  |  |  |  | SQL重构 |

---

## 练习8: 性能监控与诊断

### 🎯 学习目标
- 掌握Trino性能监控方法
- 学会诊断性能问题的系统流程  
- 建立性能基线和告警机制
- 形成持续优化的运维能力

### 🔬 实验步骤

#### 步骤1: 建立性能监控基线
```sql
-- 8.1 创建性能监控视图
CREATE VIEW query_performance_baseline AS
SELECT 
    date_trunc('hour', created) as hour,
    COUNT(*) as query_count,
    COUNT(CASE WHEN state = 'FINISHED' THEN 1 END) as successful_queries,
    COUNT(CASE WHEN state = 'FAILED' THEN 1 END) as failed_queries,
    AVG(elapsed_time.seconds) as avg_runtime_seconds,
    PERCENTILE(elapsed_time.seconds, 0.95) as p95_runtime_seconds,
    AVG(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0) as avg_memory_gb,
    SUM(total_bytes / 1024.0 / 1024.0 / 1024.0) as total_processed_gb
FROM system.runtime.queries
WHERE created >= current_timestamp - INTERVAL '24' HOUR
GROUP BY 1
ORDER BY 1 DESC;

-- 查看基线数据
SELECT * FROM query_performance_baseline LIMIT 24;
```

#### 步骤2: 问题查询识别
```sql
-- 8.2 建立慢查询检测
WITH slow_queries AS (
  SELECT 
    query_id,
    user_name,  
    left(query, 100) as query_preview,
    elapsed_time.seconds as runtime_seconds,
    queued_time.seconds as queue_seconds,
    peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0 as peak_memory_gb,
    total_bytes / 1024.0 / 1024.0 / 1024.0 as processed_gb,
    created
  FROM system.runtime.queries
  WHERE state = 'FINISHED'
    AND elapsed_time.seconds > 60  -- 超过1分钟
    AND created >= current_timestamp - INTERVAL '4' HOUR
)
SELECT 
  *,
  CASE 
    WHEN runtime_seconds > 600 THEN '🔥 Critical'
    WHEN runtime_seconds > 300 THEN '⚠️ Warning'  
    ELSE '💡 Monitor'
  END as priority_level
FROM slow_queries
ORDER BY runtime_seconds DESC;
```

#### 步骤3: 资源使用分析
```sql
-- 8.3 资源使用趋势分析
WITH resource_usage AS (
  SELECT 
    date_trunc('hour', created) as hour,
    user_name,
    COUNT(*) as query_count,
    SUM(total_bytes) / 1024.0 / 1024.0 / 1024.0 as total_data_gb,
    AVG(peak_user_memory_bytes) / 1024.0 / 1024.0 / 1024.0 as avg_memory_gb,
    MAX(peak_user_memory_bytes) / 1024.0 / 1024.0 / 1024.0 as max_memory_gb
  FROM system.runtime.queries
  WHERE created >= current_timestamp - INTERVAL '24' HOUR
  GROUP BY 1, 2
)
SELECT 
  hour,
  user_name, 
  query_count,
  total_data_gb,
  avg_memory_gb,
  max_memory_gb,
  CASE 
    WHEN max_memory_gb > 20 THEN '🚨 High Memory Usage'
    WHEN avg_memory_gb > 10 THEN '⚠️ Elevated Memory'
    ELSE '✅ Normal'
  END as memory_status
FROM resource_usage
WHERE query_count >= 5  
ORDER BY max_memory_gb DESC;
```

### 💡 综合诊断实验

#### 诊断流程实践
```sql
-- 模拟性能问题诊断流程

-- Step 1: 发现异常
-- 假设收到告警：某时段查询性能下降

-- Step 2: 问题定位  
SELECT 
  date_trunc('hour', created) as problem_hour,
  COUNT(*) as total_queries,
  AVG(elapsed_time.seconds) as avg_runtime,
  COUNT(CASE WHEN state = 'FAILED' THEN 1 END) as failures,
  array_agg(DISTINCT user_name) as active_users
FROM system.runtime.queries  
WHERE created BETWEEN current_timestamp - INTERVAL '2' HOUR 
                  AND current_timestamp - INTERVAL '1' HOUR
GROUP BY 1;

-- Step 3: 深入分析
-- 找出问题时段的具体问题查询
SELECT 
  query_id,
  user_name,
  state, 
  elapsed_time.seconds as runtime,
  left(query, 200) as query_text
FROM system.runtime.queries
WHERE created BETWEEN current_timestamp - INTERVAL '2' HOUR 
                  AND current_timestamp - INTERVAL '1' HOUR
  AND (elapsed_time.seconds > 300 OR state = 'FAILED')
ORDER BY elapsed_time.seconds DESC;

-- Step 4: 根因分析
-- 分析具体查询的执行详情 (需要实际的query_id)
-- SELECT * FROM system.runtime.stages WHERE query_id = 'your_problematic_query_id';
```

### 🎯 最终综合挑战

#### 建立完整的性能监控体系
```sql
-- 创建综合性能仪表板查询
WITH performance_summary AS (
  SELECT 
    date_trunc('day', created) as day,
    COUNT(*) as daily_queries,
    COUNT(CASE WHEN state = 'FINISHED' THEN 1 END) as successful,
    COUNT(CASE WHEN state = 'FAILED' THEN 1 END) as failed,  
    AVG(elapsed_time.seconds) as avg_runtime,
    PERCENTILE(elapsed_time.seconds, 0.95) as p95_runtime,
    COUNT(CASE WHEN elapsed_time.seconds > 300 THEN 1 END) as slow_queries,
    SUM(total_bytes) / 1024.0 / 1024.0 / 1024.0 / 1024.0 as total_data_tb
  FROM system.runtime.queries
  WHERE created >= current_timestamp - INTERVAL '7' DAY
  GROUP BY 1
)
SELECT 
  day,
  daily_queries,
  round(successful * 100.0 / daily_queries, 2) as success_rate_pct,
  avg_runtime,
  p95_runtime,  
  slow_queries,
  total_data_tb,
  CASE 
    WHEN successful * 100.0 / daily_queries < 95 THEN '🔴 Poor'
    WHEN p95_runtime > 300 THEN '🟡 Degraded'
    ELSE '🟢 Healthy'
  END as health_status
FROM performance_summary
ORDER BY day DESC;

-- 🎯 基于此查询结果，设计你的性能告警规则
```

---

## 📋 练习总结与进阶建议

### 🏆 完成检查清单

完成所有练习后，你应该能够：

- [ ] **熟练使用EXPLAIN命令** 分析查询执行计划
- [ ] **理解统计信息的重要性** 并掌握ANALYZE的使用
- [ ] **判断和调整Join策略** 适应不同的数据规模场景
- [ ] **识别动态过滤的使用场景** 并验证其效果
- [ ] **设计有效的分区策略** 并优化分区查询
- [ ] **重写查询以提升性能** 避免常见的反模式
- [ ] **进行端到端的复杂查询优化** 应用综合优化技术
- [ ] **建立性能监控和诊断体系** 支持持续优化

### 🚀 进阶学习方向

#### 1. 高级优化技术
- 物化视图的设计和使用
- 预计算和预聚合策略
- 跨引擎查询优化（联邦查询）
- 自定义UDF的性能优化

#### 2. 大规模生产实践  
- 多租户环境的资源管理
- 容错执行(FTE)的配置和使用
- 安全与权限控制优化
- 集群扩缩容策略

#### 3. 深度技术研究
- 优化器源码分析  
- 新特性和实验性功能测试
- 与其他查询引擎的性能对比
- 贡献开源社区

### 💡 实际应用建议

1. **建立基线**: 在生产环境应用前，先建立性能基线
2. **渐进优化**: 一次只应用一项优化，验证效果后再继续
3. **监控驱动**: 基于监控数据进行优化，而非主观猜测
4. **文档记录**: 记录优化过程和效果，形成最佳实践
5. **持续学习**: Trino发展迅速，保持对新特性的学习

---

**恭喜你完成了Trino优化器的实战练习！通过这些动手实验，你应该已经具备了在实际生产环境中进行Trino性能优化的能力。记住，优化是一个持续的过程，需要结合具体的业务场景和数据特点来应用这些技术。祝你在Trino的优化道路上越走越远！** 🎉
