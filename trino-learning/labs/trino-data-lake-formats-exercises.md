# Trino 数据湖表格式实战实验

## 🎯 实验概述

这套数据湖表格式实验专门配合《Trino 数据湖表格式深度剖析》文档，通过实际操作Apache Iceberg、Delta Lake等现代表格式，帮助你掌握湖仓一体化架构的核心技术。

**前置条件**: 
- 完成Trino基础学习
- 熟悉Parquet/ORC等列式存储格式
- 有权限访问S3或HDFS存储系统
- 基本的Spark环境 (部分实验需要)

---

## 📚 实验目录

1. [实验1: Iceberg表创建与基础操作](#实验1-iceberg表创建与基础操作)
2. [实验2: Delta Lake事务机制验证](#实验2-delta-lake事务机制验证)
3. [实验3: 表格式性能对比测试](#实验3-表格式性能对比测试)
4. [实验4: 时间旅行功能实战](#实验4-时间旅行功能实战)
5. [实验5: 模式演进与兼容性](#实验5-模式演进与兼容性)
6. [实验6: ACID事务与并发控制](#实验6-acid事务与并发控制)
7. [实验7: 表维护与优化策略](#实验7-表维护与优化策略)
8. [实验8: 湖仓一体化架构设计](#实验8-湖仓一体化架构设计)

---

## 实验1: Iceberg表创建与基础操作

### 🎯 学习目标
- 理解Iceberg表的创建和配置过程
- 掌握Iceberg表的元数据结构
- 验证Trino与Iceberg的集成
- 分析Iceberg的存储布局

### 🔬 实验步骤

#### 步骤1: 配置Iceberg连接器
```properties
# config/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://localhost:9083

# S3存储配置
iceberg.catalog.warehouse=s3://trino-iceberg-test/warehouse/
hive.s3.aws-access-key=YOUR_ACCESS_KEY
hive.s3.aws-secret-key=YOUR_SECRET_KEY
hive.s3.region=us-west-2

# Iceberg特性配置
iceberg.delete-as-join-rewrite-enabled=true
iceberg.target-max-file-size=512MB
iceberg.split-size=128MB
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
```

#### 步骤2: 创建Iceberg表并分析元数据
```sql
-- 创建第一个Iceberg表
CREATE TABLE iceberg.test_schema.orders_iceberg (
    orderkey BIGINT,
    custkey BIGINT,
    orderstatus VARCHAR(1),
    totalprice DOUBLE,
    orderdate DATE,
    orderpriority VARCHAR(15),
    clerk VARCHAR(15),
    shippriority INTEGER,
    comment VARCHAR(79)
)
WITH (
    partitioning = ARRAY['orderdate'],
    format = 'PARQUET',
    format_version = 2
);

-- 插入测试数据
INSERT INTO iceberg.test_schema.orders_iceberg
SELECT 
    orderkey, custkey, orderstatus, totalprice, orderdate,
    orderpriority, clerk, shippriority, comment
FROM hive.tpch_sf1.orders
WHERE orderdate >= DATE '1995-01-01';

-- 查看表属性和元数据
SHOW CREATE TABLE iceberg.test_schema.orders_iceberg;

-- 查看Iceberg特有的系统表
SELECT * FROM iceberg.test_schema."orders_iceberg$snapshots" LIMIT 5;
SELECT * FROM iceberg.test_schema."orders_iceberg$manifests" LIMIT 5;
SELECT * FROM iceberg.test_schema."orders_iceberg$files" LIMIT 10;
```

#### 步骤3: 分析Iceberg存储结构
```bash
# 检查S3上的Iceberg表结构
aws s3 ls s3://trino-iceberg-test/warehouse/test_schema/orders_iceberg/ --recursive

# 预期看到类似结构:
# metadata/
#   v1.metadata.json
#   v2.metadata.json  
#   snap-123456789.avro
# data/
#   orderdate=1995-01-01/
#     data-001.parquet
#     data-002.parquet
```

#### 步骤4: 元数据文件内容分析
```python
#!/usr/bin/env python3
import boto3
import json
import avro.schema
import avro.io
import io

def analyze_iceberg_metadata(bucket_name, table_path):
    """分析Iceberg表的元数据文件"""
    s3 = boto3.client('s3')
    
    # 查找最新的元数据文件
    metadata_prefix = f"{table_path}/metadata/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
    
    if 'Contents' not in response:
        print("No metadata files found")
        return
    
    # 找到最新的metadata.json文件
    metadata_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.metadata.json')]
    latest_metadata = max(metadata_files, key=lambda x: x['LastModified'])
    
    print(f"Analyzing metadata file: {latest_metadata['Key']}")
    
    # 读取元数据内容
    obj = s3.get_object(Bucket=bucket_name, Key=latest_metadata['Key'])
    metadata_content = obj['Body'].read().decode('utf-8')
    metadata = json.loads(metadata_content)
    
    # 分析元数据结构
    print("\n=== Table Metadata Analysis ===")
    print(f"Format version: {metadata.get('format-version')}")
    print(f"Table UUID: {metadata.get('table-uuid')}")
    print(f"Current snapshot ID: {metadata.get('current-snapshot-id')}")
    print(f"Schema ID: {metadata.get('current-schema-id')}")
    print(f"Partition spec ID: {metadata.get('default-spec-id')}")
    
    # 分析模式信息
    schema = metadata.get('schemas', [{}])[0]
    print(f"\nSchema fields count: {len(schema.get('fields', []))}")
    for field in schema.get('fields', [])[:5]:  # 显示前5个字段
        print(f"  {field.get('name')}: {field.get('type')} (ID: {field.get('id')})")
    
    # 分析分区规格
    specs = metadata.get('partition-specs', [])
    if specs:
        current_spec = specs[-1]  # 最新分区规格
        print(f"\nPartition spec (ID: {current_spec.get('spec-id')}):")
        for field in current_spec.get('fields', []):
            print(f"  {field}")
    
    # 分析快照信息
    snapshots = metadata.get('snapshots', [])
    print(f"\nSnapshot count: {len(snapshots)}")
    for snapshot in snapshots[-3:]:  # 显示最近3个快照
        print(f"  Snapshot {snapshot.get('snapshot-id')}:")
        print(f"    Timestamp: {snapshot.get('timestamp-ms')}")
        print(f"    Operation: {snapshot.get('operation')}")
        print(f"    Summary: {snapshot.get('summary', {})}")

if __name__ == "__main__":
    analyze_iceberg_metadata("trino-iceberg-test", "warehouse/test_schema/orders_iceberg")
```

### 📊 Iceberg元数据分析结果

| 元数据组件 | 数量 | 大小 | 备注 |
|-----------|------|------|------|
| 元数据文件 |  |  |  |
| 快照数量 |  |  |  |
| Manifest文件 |  |  |  |
| 数据文件 |  |  |  |

---

## 实验2: Delta Lake事务机制验证

### 🎯 学习目标
- 验证Delta Lake的ACID事务特性
- 理解事务日志的工作机制
- 测试并发写入的冲突检测
- 分析Delta表的版本管理

### 🔬 实验步骤

#### 步骤1: 创建Delta表
```sql
-- 在Trino中创建Delta表 (需要Delta连接器)
CREATE TABLE delta.test_schema.customer_delta (
    custkey BIGINT,
    name VARCHAR(25),
    address VARCHAR(40),
    nationkey BIGINT,
    phone VARCHAR(15),
    acctbal DOUBLE,
    mktsegment VARCHAR(10),
    comment VARCHAR(117),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
WITH (
    location = 's3://trino-delta-test/tables/customer_delta/',
    checkpoint_interval = 10
);

-- 插入初始数据
INSERT INTO delta.test_schema.customer_delta
SELECT 
    custkey, name, address, nationkey, phone, acctbal, mktsegment, comment,
    CURRENT_TIMESTAMP as last_updated
FROM hive.tpch_sf1.customer
WHERE custkey <= 50000;
```

#### 步骤2: 分析Delta事务日志
```python
#!/usr/bin/env python3
import boto3
import json
import pandas as pd
from datetime import datetime

def analyze_delta_log(bucket_name, table_path):
    """分析Delta Lake的事务日志"""
    s3 = boto3.client('s3')
    
    # 列出_delta_log目录下的所有文件
    log_prefix = f"{table_path}/_delta_log/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=log_prefix)
    
    if 'Contents' not in response:
        print("No delta log files found")
        return
    
    print("=== Delta Log Analysis ===")
    
    log_files = []
    checkpoint_files = []
    
    for obj in response['Contents']:
        key = obj['Key']
        filename = key.split('/')[-1]
        
        if filename.endswith('.json'):
            # 事务日志文件
            version = int(filename.split('.')[0])
            log_files.append({
                'version': version,
                'key': key,
                'size': obj['Size'],
                'modified': obj['LastModified']
            })
        elif filename.endswith('.checkpoint.parquet'):
            # 检查点文件
            checkpoint_files.append({
                'version': int(filename.split('.')[0]),
                'key': key,
                'size': obj['Size']
            })
    
    print(f"Transaction log files: {len(log_files)}")
    print(f"Checkpoint files: {len(checkpoint_files)}")
    
    # 分析最近的几个事务
    recent_logs = sorted(log_files, key=lambda x: x['version'])[-5:]
    
    print("\n=== Recent Transactions ===")
    for log_file in recent_logs:
        print(f"\nVersion {log_file['version']}:")
        print(f"  Size: {log_file['size']} bytes")
        print(f"  Modified: {log_file['modified']}")
        
        # 读取事务日志内容
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=log_file['key'])
            log_content = obj['Body'].read().decode('utf-8')
            
            # 分析每行操作
            operations = []
            for line in log_content.strip().split('\n'):
                if line.strip():
                    action = json.loads(line)
                    operations.append(action)
            
            print(f"  Operations count: {len(operations)}")
            
            # 统计操作类型
            op_types = {}
            for op in operations:
                for key in op.keys():
                    if key not in ['commitInfo']:  # 跳过commitInfo
                        op_types[key] = op_types.get(key, 0) + 1
            
            print(f"  Operation types: {op_types}")
            
            # 分析commitInfo
            commit_info = next((op['commitInfo'] for op in operations if 'commitInfo' in op), None)
            if commit_info:
                print(f"  Operation: {commit_info.get('operation', 'Unknown')}")
                print(f"  User: {commit_info.get('userName', 'Unknown')}")
                print(f"  Timestamp: {commit_info.get('timestamp')}")
                
        except Exception as e:
            print(f"  Error reading log content: {e}")

if __name__ == "__main__":
    analyze_delta_log("trino-delta-test", "tables/customer_delta")
```

#### 步骤3: 事务冲突测试
```sql
-- 模拟并发事务场景

-- 会话1: 更新高价值客户
UPDATE delta.test_schema.customer_delta 
SET mktsegment = 'VIP', last_updated = CURRENT_TIMESTAMP
WHERE acctbal > 8000;

-- 同时在会话2: 删除低活跃客户
DELETE FROM delta.test_schema.customer_delta 
WHERE acctbal < 1000;

-- 会话3: 插入新客户
INSERT INTO delta.test_schema.customer_delta VALUES
(150001, 'New Customer 1', 'New Address 1', 1, '555-0001', 5000.0, 'BUILDING', 'Test customer', CURRENT_TIMESTAMP),
(150002, 'New Customer 2', 'New Address 2', 2, '555-0002', 7500.0, 'AUTOMOBILE', 'Test customer', CURRENT_TIMESTAMP);

-- 🎯 观察哪些操作成功，哪些因为冲突失败
-- 🎯 分析Delta Lake如何处理这些并发操作
```

### 📊 事务测试结果记录

| 操作类型 | 执行时间 | 成功/失败 | 冲突原因 | 版本号 | 备注 |
|---------|----------|----------|----------|--------|------|
| UPDATE高价值客户 |  |  |  |  |  |
| DELETE低活跃客户 |  |  |  |  |  |
| INSERT新客户 |  |  |  |  |  |

---

## 实验3: 表格式性能对比测试

### 🎯 学习目标
- 对比Iceberg、Delta、Hive表的查询性能
- 理解不同表格式的优化特征
- 分析存储开销和压缩效果
- 验证查询下推的效果差异

### 🔬 实验步骤

#### 步骤1: 创建相同结构的不同格式表
```sql
-- Hive表 (传统格式)
CREATE TABLE hive.test_schema.lineitem_hive (
    orderkey BIGINT,
    partkey BIGINT,
    suppkey BIGINT,
    linenumber INTEGER,
    quantity DOUBLE,
    extendedprice DOUBLE,
    discount DOUBLE,
    tax DOUBLE,
    returnflag VARCHAR(1),
    linestatus VARCHAR(1),
    shipdate DATE,
    commitdate DATE,
    receiptdate DATE,
    shipinstruct VARCHAR(25),
    shipmode VARCHAR(10),
    comment VARCHAR(44)
)
WITH (
    partitioned_by = ARRAY['shipdate'],
    format = 'PARQUET'
);

-- Iceberg表
CREATE TABLE iceberg.test_schema.lineitem_iceberg (
    orderkey BIGINT,
    partkey BIGINT, 
    suppkey BIGINT,
    linenumber INTEGER,
    quantity DOUBLE,
    extendedprice DOUBLE,
    discount DOUBLE,
    tax DOUBLE,
    returnflag VARCHAR(1),
    linestatus VARCHAR(1),
    shipdate DATE,
    commitdate DATE,
    receiptdate DATE,
    shipinstruct VARCHAR(25),
    shipmode VARCHAR(10),
    comment VARCHAR(44)
)
WITH (
    partitioning = ARRAY['bucket(orderkey, 16)', 'day(shipdate)'],
    format = 'PARQUET'
);

-- Delta表 (如果有Delta连接器)
CREATE TABLE delta.test_schema.lineitem_delta (
    -- 相同的列定义
)
WITH (
    location = 's3://trino-delta-test/tables/lineitem_delta/',
    partitioned_by = ARRAY['shipdate']
);
```

#### 步骤2: 插入相同的测试数据
```sql
-- 为每个表格式插入相同的数据
INSERT INTO hive.test_schema.lineitem_hive
SELECT * FROM hive.tpch_sf10.lineitem WHERE shipdate >= DATE '1995-01-01';

INSERT INTO iceberg.test_schema.lineitem_iceberg  
SELECT * FROM hive.tpch_sf10.lineitem WHERE shipdate >= DATE '1995-01-01';

INSERT INTO delta.test_schema.lineitem_delta
SELECT * FROM hive.tpch_sf10.lineitem WHERE shipdate >= DATE '1995-01-01';

-- 收集统计信息
ANALYZE TABLE hive.test_schema.lineitem_hive;
ANALYZE TABLE iceberg.test_schema.lineitem_iceberg;
ANALYZE TABLE delta.test_schema.lineitem_delta;
```

#### 步骤3: 性能基准测试
```sql
-- 测试查询1: 简单过滤和聚合
-- Hive表
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)
SELECT 
    returnflag,
    linestatus,
    COUNT(*) as line_count,
    SUM(quantity) as total_qty,
    SUM(extendedprice) as total_price,
    AVG(extendedprice) as avg_price
FROM hive.test_schema.lineitem_hive
WHERE shipdate >= DATE '1996-01-01' AND shipdate < DATE '1996-04-01'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus;

-- Iceberg表 (相同查询)
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)
SELECT 
    returnflag, linestatus, COUNT(*) as line_count,
    SUM(quantity) as total_qty, SUM(extendedprice) as total_price, AVG(extendedprice) as avg_price
FROM iceberg.test_schema.lineitem_iceberg
WHERE shipdate >= DATE '1996-01-01' AND shipdate < DATE '1996-04-01'
GROUP BY returnflag, linestatus
ORDER BY returnflag, linestatus;

-- 🎯 对比执行计划的差异:
-- - 扫描的文件数量
-- - 分区裁剪效果
-- - 统计信息质量
-- - 查询下推程度
```

#### 步骤4: 自动化性能测试脚本
```python
#!/usr/bin/env python3
import trino
import time
import statistics
from dataclasses import dataclass
from typing import List

@dataclass 
class BenchmarkResult:
    table_format: str
    query_name: str
    execution_time: float
    rows_scanned: int
    rows_returned: int
    peak_memory_mb: float
    files_scanned: int

class TableFormatBenchmark:
    
    def __init__(self):
        self.conn = trino.dbapi.connect(
            host='localhost',
            port=8080,
            user='benchmark'
        )
        
    def benchmark_query(self, query: str, table_format: str, query_name: str) -> BenchmarkResult:
        """执行单个查询基准测试"""
        cursor = self.conn.cursor()
        
        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        execution_time = time.time() - start_time
        
        # 获取查询统计
        stats_query = f"""
        SELECT 
            total_rows,
            processed_rows, 
            peak_user_memory_bytes,
            total_bytes
        FROM system.runtime.queries
        WHERE query_id = '{cursor.query_id}'
        """
        cursor.execute(stats_query)
        stats = cursor.fetchone()
        
        cursor.close()
        
        return BenchmarkResult(
            table_format=table_format,
            query_name=query_name,
            execution_time=execution_time,
            rows_scanned=stats[0] if stats else 0,
            rows_returned=len(result),
            peak_memory_mb=(stats[2] / 1024 / 1024) if stats else 0,
            files_scanned=0  # 需要从EXPLAIN结果中解析
        )
    
    def run_comprehensive_benchmark(self) -> List[BenchmarkResult]:
        """运行全面的性能基准测试"""
        results = []
        
        # 定义测试查询集
        queries = {
            'point_query': {
                'hive': "SELECT * FROM hive.test_schema.lineitem_hive WHERE orderkey = 12345",
                'iceberg': "SELECT * FROM iceberg.test_schema.lineitem_iceberg WHERE orderkey = 12345",
                'delta': "SELECT * FROM delta.test_schema.lineitem_delta WHERE orderkey = 12345"
            },
            'range_scan': {
                'hive': "SELECT COUNT(*) FROM hive.test_schema.lineitem_hive WHERE shipdate BETWEEN DATE '1996-01-01' AND DATE '1996-03-31'",
                'iceberg': "SELECT COUNT(*) FROM iceberg.test_schema.lineitem_iceberg WHERE shipdate BETWEEN DATE '1996-01-01' AND DATE '1996-03-31'",
                'delta': "SELECT COUNT(*) FROM delta.test_schema.lineitem_delta WHERE shipdate BETWEEN DATE '1996-01-01' AND DATE '1996-03-31'"
            },
            'aggregation': {
                'hive': "SELECT suppkey, SUM(extendedprice) as total FROM hive.test_schema.lineitem_hive GROUP BY suppkey",
                'iceberg': "SELECT suppkey, SUM(extendedprice) as total FROM iceberg.test_schema.lineitem_iceberg GROUP BY suppkey",
                'delta': "SELECT suppkey, SUM(extendedprice) as total FROM delta.test_schema.lineitem_delta GROUP BY suppkey"
            }
        }
        
        # 执行基准测试
        for query_name, format_queries in queries.items():
            print(f"Running benchmark for: {query_name}")
            
            for table_format, query in format_queries.items():
                print(f"  Testing {table_format} format...")
                
                try:
                    result = self.benchmark_query(query, table_format, query_name)
                    results.append(result)
                    
                    print(f"    Execution time: {result.execution_time:.2f}s")
                    print(f"    Rows scanned: {result.rows_scanned:,}")
                    print(f"    Peak memory: {result.peak_memory_mb:.1f}MB")
                    
                except Exception as e:
                    print(f"    Error: {e}")
        
        return results
    
    def generate_performance_report(self, results: List[BenchmarkResult]):
        """生成性能对比报告"""
        print("\n" + "="*60)
        print("PERFORMANCE COMPARISON REPORT")
        print("="*60)
        
        # 按查询类型分组
        by_query = {}
        for result in results:
            if result.query_name not in by_query:
                by_query[result.query_name] = {}
            by_query[result.query_name][result.table_format] = result
        
        # 生成对比表格
        for query_name, formats in by_query.items():
            print(f"\n{query_name.upper()} Performance:")
            print("-" * 40)
            
            baseline_time = formats.get('hive', {}).execution_time if 'hive' in formats else None
            
            for format_name, result in formats.items():
                improvement = ""
                if baseline_time and result.execution_time != baseline_time:
                    ratio = baseline_time / result.execution_time
                    improvement = f"({ratio:.1f}x {'faster' if ratio > 1 else 'slower'})"
                
                print(f"{format_name:>8}: {result.execution_time:6.2f}s  {improvement}")

if __name__ == "__main__":
    benchmark = TableFormatBenchmark()
    results = benchmark.run_comprehensive_benchmark()
    benchmark.generate_performance_report(results)
```

### 📊 性能对比结果

| 查询类型 | Hive耗时 | Iceberg耗时 | Delta耗时 | 最佳格式 | 性能提升 |
|---------|----------|-------------|----------|----------|----------|
| 点查询 |  |  |  |  |  |
| 范围扫描 |  |  |  |  |  |
| 聚合查询 |  |  |  |  |  |
| Join查询 |  |  |  |  |  |

---

## 实验4: 时间旅行功能实战

### 🎯 学习目标
- 验证Iceberg和Delta的时间旅行功能
- 理解快照和版本的概念差异
- 测试历史数据查询的性能
- 分析时间旅行的存储开销

### 🔬 实验步骤

#### 步骤1: 创建时间序列数据
```sql
-- 创建模拟时间序列更新的场景
-- 首次插入
INSERT INTO iceberg.test_schema.customer_delta
SELECT 
    custkey, name, address, nationkey, phone, acctbal, mktsegment, comment,
    TIMESTAMP '2023-01-01 00:00:00' as last_updated
FROM hive.tpch_sf1.customer WHERE custkey <= 1000;

-- 模拟后续更新操作 (创建多个版本)
UPDATE iceberg.test_schema.customer_delta 
SET acctbal = acctbal * 1.1, last_updated = TIMESTAMP '2023-02-01 00:00:00'
WHERE mktsegment = 'BUILDING';

UPDATE iceberg.test_schema.customer_delta
SET acctbal = acctbal * 0.95, last_updated = TIMESTAMP '2023-03-01 00:00:00' 
WHERE mktsegment = 'AUTOMOBILE';

-- 添加新客户
INSERT INTO iceberg.test_schema.customer_delta VALUES
(1001, 'Time Travel Customer', 'Test Address', 1, '555-1001', 10000.0, 'MACHINERY', 'Test', TIMESTAMP '2023-04-01 00:00:00');

-- 删除一些客户
DELETE FROM iceberg.test_schema.customer_delta 
WHERE acctbal < 0 OR custkey IN (999, 998, 997);
```

#### 步骤2: Iceberg时间旅行查询
```sql
-- 查看所有可用的快照
SELECT * FROM iceberg.test_schema."customer_delta$snapshots" 
ORDER BY committed_at;

-- 基于快照ID的时间旅行
SELECT snapshot_id FROM iceberg.test_schema."customer_delta$snapshots" 
ORDER BY committed_at LIMIT 3;

-- 使用第一个快照的数据
SELECT COUNT(*), AVG(acctbal) as avg_balance
FROM iceberg.test_schema.customer_delta FOR VERSION AS OF 12345678901234;  -- 替换为实际snapshot_id

-- 基于时间戳的时间旅行  
SELECT COUNT(*), AVG(acctbal) as avg_balance
FROM iceberg.test_schema.customer_delta FOR TIMESTAMP AS OF TIMESTAMP '2023-02-15 00:00:00';

-- 对比当前版本
SELECT COUNT(*), AVG(acctbal) as avg_balance  
FROM iceberg.test_schema.customer_delta;

-- 🎯 分析结果差异，验证时间旅行的正确性
```

#### 步骤3: 增量查询实战
```sql
-- Iceberg增量查询 (查看两个快照之间的变化)
SELECT * FROM TABLE(iceberg.system.table_changes(
    'test_schema', 'customer_delta',
    12345678901234,  -- from_snapshot_id
    23456789012345   -- to_snapshot_id  
));

-- 分析增量数据的特征
WITH incremental_changes AS (
  SELECT * FROM TABLE(iceberg.system.table_changes(
    'test_schema', 'customer_delta', 
    12345678901234, 23456789012345
  ))
)
SELECT 
  change_type,
  COUNT(*) as change_count,
  COUNT(DISTINCT custkey) as affected_customers,
  SUM(CASE WHEN change_type = 'INSERT' THEN 1 ELSE 0 END) as inserts,
  SUM(CASE WHEN change_type = 'DELETE' THEN 1 ELSE 0 END) as deletes,
  SUM(CASE WHEN change_type = 'UPDATE_BEFORE' THEN 1 ELSE 0 END) as updates
FROM incremental_changes
GROUP BY change_type;
```

### 📊 时间旅行性能分析

| 查询类型 | 当前版本耗时 | 历史版本耗时 | 性能差异 | 存储开销 | 备注 |
|---------|-------------|-------------|----------|----------|------|
| 点查询 |  |  |  |  |  |
| 聚合查询 |  |  |  |  |  |
| 增量查询 |  |  |  |  |  |

---

## 实验5: 模式演进与兼容性

### 🎯 学习目标
- 验证不同表格式的模式演进能力
- 理解向前和向后兼容性
- 测试复杂模式变更的处理
- 分析模式演进对查询的影响

### 🔬 实验步骤

#### 步骤1: Iceberg模式演进测试
```sql
-- 初始表结构
CREATE TABLE iceberg.test_schema.evolving_table (
    id BIGINT,
    name VARCHAR(100),
    created_date DATE
);

-- 插入初始数据
INSERT INTO iceberg.test_schema.evolving_table VALUES
(1, 'Record 1', DATE '2023-01-01'),
(2, 'Record 2', DATE '2023-01-02'),
(3, 'Record 3', DATE '2023-01-03');

-- 模式演进1: 添加新列
ALTER TABLE iceberg.test_schema.evolving_table 
ADD COLUMN email VARCHAR(200);

-- 验证新列的默认值处理
SELECT * FROM iceberg.test_schema.evolving_table;

-- 插入包含新列的数据
INSERT INTO iceberg.test_schema.evolving_table VALUES
(4, 'Record 4', DATE '2023-01-04', 'test4@example.com'),
(5, 'Record 5', DATE '2023-01-05', 'test5@example.com');

-- 模式演进2: 修改列类型 (支持的类型提升)
ALTER TABLE iceberg.test_schema.evolving_table 
ALTER COLUMN id SET DATA TYPE BIGINT;  -- int → bigint

-- 模式演进3: 重命名列
ALTER TABLE iceberg.test_schema.evolving_table 
RENAME COLUMN created_date TO creation_date;

-- 验证模式演进后的查询兼容性
SELECT id, name, creation_date, email FROM iceberg.test_schema.evolving_table;
```

#### 步骤2: 跨版本兼容性测试
```sql
-- 使用旧模式查询新数据 (向后兼容)
SELECT id, name, created_date  -- 使用旧列名
FROM iceberg.test_schema.evolving_table FOR VERSION AS OF <old_snapshot_id>;

-- 使用新模式查询所有数据 (向前兼容)
SELECT id, name, creation_date, email
FROM iceberg.test_schema.evolving_table;

-- 🎯 验证兼容性:
-- - 旧查询是否仍能正常工作？
-- - 新列在旧数据中如何处理？
-- - 类型提升是否透明？
```

### 📊 模式演进兼容性测试结果

| 演进操作 | Iceberg | Delta | Hive | 兼容性影响 |
|---------|---------|-------|------|------------|
| ADD COLUMN |  |  |  |  |
| DROP COLUMN |  |  |  |  |
| RENAME COLUMN |  |  |  |  |
| 类型提升(int→bigint) |  |  |  |  |
| 嵌套结构变更 |  |  |  |  |

---

## 实验6-8: 其他高级实验

由于篇幅限制，提供其余实验的大纲：

### 实验6: ACID事务与并发控制
- 模拟多客户端并发写入场景
- 测试事务隔离级别
- 验证乐观并发控制机制
- 分析冲突解决策略

### 实验7: 表维护与优化策略
- 实施自动化表维护流程
- 测试小文件压缩效果  
- 验证过期快照清理机制
- 分析存储成本优化

### 实验8: 湖仓一体化架构设计
- 设计端到端的湖仓架构
- 集成流式和批量处理
- 实现统一的元数据管理
- 建立数据治理体系

---

## 🎯 实验总结与进阶

完成这些数据湖表格式实验后，你将：

### ✅ **掌握现代数据湖技术**
- 深入理解Iceberg、Delta、Hudi的技术原理
- 具备选择和配置合适表格式的能力
- 掌握表格式的性能调优技巧

### ✅ **建立湖仓一体化视野**  
- 理解湖仓一体化的技术路径
- 具备设计现代数据架构的能力
- 掌握跨引擎数据访问的技术方案

### ✅ **获得前沿技术优势**
- 数据湖表格式是数据领域的前沿技术
- ACID事务能力是现代数据平台的标配
- 时间旅行和增量处理是差异化能力

### 🚀 **职业发展价值**
1. **数据湖架构师**: 设计企业级数据湖平台
2. **湖仓技术专家**: 引领组织的技术演进  
3. **开源技术贡献者**: 参与前沿开源项目
4. **技术咨询顾问**: 帮助企业选择最优技术方案

通过掌握数据湖表格式技术，你已经站在了数据存储技术的最前沿！🌟
