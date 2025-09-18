# Trino FTE 容错机制实战实验

## 🎯 实验概述

这套FTE容错实验专门配合《Trino FTE容错与外部Exchange架构深度剖析》文档，通过实际的容错场景模拟和配置实践，帮助你掌握Trino FTE的核心机制和企业级部署技能。

**前置条件**: 
- 完成基础优化和执行器学习
- 熟悉Trino集群部署和配置
- 有权限访问外部存储系统(S3/HDFS)

---

## 📚 实验目录

1. [实验1: FTE环境搭建与配置](#实验1-fte环境搭建与配置)
2. [实验2: 外部Exchange存储验证](#实验2-外部exchange存储验证)
3. [实验3: 故障注入与恢复测试](#实验3-故障注入与恢复测试)
4. [实验4: 性能开销测量与对比](#实验4-性能开销测量与对比)
5. [实验5: 推测执行机制验证](#实验5-推测执行机制验证)
6. [实验6: 大规模ETL容错实战](#实验6-大规模etl容错实战)
7. [实验7: FTE监控与告警系统](#实验7-fte监控与告警系统)
8. [实验8: 成本优化与容量规划](#实验8-成本优化与容量规划)

---

## 实验1: FTE环境搭建与配置

### 🎯 学习目标
- 搭建支持FTE的Trino集群环境
- 配置外部Exchange存储系统
- 验证FTE功能的正确启用
- 理解FTE配置参数的影响

### 🔬 实验步骤

#### 步骤1: 准备外部存储
```bash
# S3存储配置 (AWS环境)
aws s3 mb s3://trino-fte-exchange-test --region us-west-2

# 配置S3生命周期策略
cat > lifecycle-policy.json << EOF
{
    "Rules": [
        {
            "ID": "TrinoFTECleanup",
            "Status": "Enabled",
            "Filter": {"Prefix": "exchange-data/"},
            "Expiration": {"Days": 7},
            "Transitions": [
                {
                    "Days": 1,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 3,
                    "StorageClass": "GLACIER"
                }
            ]
        }
    ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
  --bucket trino-fte-exchange-test \
  --lifecycle-configuration file://lifecycle-policy.json
```

#### 步骤2: 配置Coordinator FTE参数
```properties
# config/config.properties
# 基础配置
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
query.max-memory=50GB
query.max-memory-per-node=8GB

# 启用FTE
fault-tolerant-execution-enabled=true
fault-tolerant-execution-task-memory=4GB
fault-tolerant-execution-max-task-split-count=256
fault-tolerant-execution-target-task-input-size=2GB
fault-tolerant-execution-target-task-split-count=64

# Exchange配置
exchange.base-directories=s3://trino-fte-exchange-test/exchange-data/
exchange.compression-enabled=true
exchange.encryption-enabled=false
exchange.max-buffer-size=1GB
exchange.sink.buffer-pool-min-size=128MB
exchange.sink.buffers-per-partition=2

# 重试策略
fault-tolerant-execution-task-retry-attempts=3
fault-tolerant-execution-task-retry-delay=10s
fault-tolerant-execution-retry-policy=TASK

# 推测执行
fault-tolerant-execution-speculative-execution-enabled=true
fault-tolerant-execution-speculative-execution-threshold=60s
```

#### 步骤3: 配置Worker FTE参数
```properties
# config/config.properties (Worker节点)
coordinator=false
http-server.http.port=8080
query.max-memory-per-node=8GB

# 与Coordinator相同的FTE配置
fault-tolerant-execution-enabled=true
fault-tolerant-execution-task-memory=4GB
exchange.base-directories=s3://trino-fte-exchange-test/exchange-data/
exchange.compression-enabled=true
```

#### 步骤4: S3访问配置
```properties
# config/catalog/hive.properties
connector.name=hive
hive.s3.aws-access-key=YOUR_ACCESS_KEY
hive.s3.aws-secret-key=YOUR_SECRET_KEY
hive.s3.region=us-west-2
hive.s3.endpoint=https://s3.us-west-2.amazonaws.com

# 或使用IAM角色 (推荐)
hive.s3.use-instance-credentials=true
```

### 📊 验证配置

#### 验证1: 检查FTE状态
```sql
-- 连接Trino并检查FTE配置
SHOW SESSION LIKE 'fault_tolerant%';

-- 预期输出应包含:
-- fault_tolerant_execution_enabled | true
-- fault_tolerant_execution_task_memory | 4GB
-- fault_tolerant_execution_max_task_split_count | 256
```

#### 验证2: 测试外部Exchange
```sql
-- 运行简单查询测试FTE
SELECT 
    orderkey,
    custkey,
    totalprice,
    COUNT(*) OVER (PARTITION BY custkey) as customer_order_count
FROM orders 
WHERE orderdate >= DATE '2023-01-01'
LIMIT 1000;

-- 检查是否产生了Exchange数据
-- 在S3控制台查看 s3://trino-fte-exchange-test/exchange-data/
```

### 📋 实验记录表

| 配置项 | 设置值 | 验证结果 | 备注 |
|--------|--------|----------|------|
| FTE启用状态 |  |  |  |
| Exchange存储 |  |  |  |
| 任务内存限制 |  |  |  |
| 推测执行 |  |  |  |

---

## 实验2: 外部Exchange存储验证

### 🎯 学习目标
- 理解外部Exchange的数据组织方式
- 验证数据压缩和序列化机制
- 测试不同存储后端的性能差异
- 分析存储开销和成本影响

### 🔬 实验步骤

#### 步骤1: Exchange数据结构分析
```bash
# 创建分析工具脚本
cat > analyze_exchange_data.py << 'EOF'
#!/usr/bin/env python3
import boto3
import json
from datetime import datetime

def analyze_exchange_storage(bucket_name, prefix):
    s3 = boto3.client('s3')
    
    # 列出所有Exchange相关对象
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' not in response:
        print("No exchange data found")
        return
    
    total_size = 0
    file_count = 0
    file_types = {}
    
    for obj in response['Contents']:
        key = obj['Key']
        size = obj['Size']
        modified = obj['LastModified']
        
        total_size += size
        file_count += 1
        
        # 分析文件类型
        if key.endswith('.metadata'):
            file_types['metadata'] = file_types.get('metadata', 0) + 1
        elif 'part-' in key:
            file_types['data'] = file_types.get('data', 0) + 1
        else:
            file_types['other'] = file_types.get('other', 0) + 1
        
        print(f"File: {key}")
        print(f"  Size: {size:,} bytes ({size/1024/1024:.2f} MB)")
        print(f"  Modified: {modified}")
        print()
    
    print("=== Exchange Storage Summary ===")
    print(f"Total files: {file_count}")
    print(f"Total size: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    print(f"File types: {file_types}")
    
    # 分析元数据文件
    for obj in response['Contents']:
        if obj['Key'].endswith('.metadata'):
            analyze_metadata_file(bucket_name, obj['Key'])
            break

def analyze_metadata_file(bucket_name, key):
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        metadata_content = response['Body'].read().decode('utf-8')
        metadata = json.loads(metadata_content)
        
        print("\n=== Exchange Metadata Analysis ===")
        print(f"Partition count: {len(metadata.get('partitions', []))}")
        
        for i, partition in enumerate(metadata.get('partitions', [])):
            print(f"Partition {i}:")
            print(f"  Object key: {partition.get('objectKey', 'N/A')}")
            print(f"  Row count: {partition.get('rowCount', 0):,}")
            print(f"  Data size: {partition.get('dataSize', 0):,} bytes")
        
    except Exception as e:
        print(f"Failed to analyze metadata: {e}")

if __name__ == "__main__":
    analyze_exchange_storage("trino-fte-exchange-test", "exchange-data/")
EOF

chmod +x analyze_exchange_data.py
```

#### 步骤2: 触发Exchange数据生成
```sql
-- 执行一个会产生大量中间结果的查询
WITH customer_orders AS (
  SELECT 
    c.custkey,
    c.name,
    c.mktsegment,
    o.orderkey,
    o.totalprice,
    o.orderdate,
    ROW_NUMBER() OVER (PARTITION BY c.custkey ORDER BY o.orderdate DESC) as order_rank
  FROM customer c
  JOIN orders o ON c.custkey = o.custkey
  WHERE o.orderdate >= DATE '2023-01-01'
),
customer_summary AS (
  SELECT 
    custkey,
    name,
    mktsegment,
    COUNT(*) as total_orders,
    SUM(totalprice) as total_spent,
    AVG(totalprice) as avg_order_value,
    MAX(CASE WHEN order_rank = 1 THEN orderdate END) as last_order_date
  FROM customer_orders
  GROUP BY custkey, name, mktsegment
)
SELECT 
  mktsegment,
  COUNT(*) as customer_count,
  SUM(total_orders) as total_orders,
  SUM(total_spent) as segment_revenue,
  AVG(avg_order_value) as avg_order_value,
  PERCENTILE(total_spent, 0.5) as median_customer_value,
  PERCENTILE(total_spent, 0.95) as p95_customer_value
FROM customer_summary
GROUP BY mktsegment
ORDER BY segment_revenue DESC;
```

#### 步骤3: 分析Exchange数据
```bash
# 运行分析脚本
python3 analyze_exchange_data.py

# 查看压缩效果
aws s3 ls s3://trino-fte-exchange-test/exchange-data/ --recursive --human-readable | head -20
```

### 📊 Exchange存储分析结果

| 指标 | 值 | 备注 |
|------|----|----|
| 原始数据大小 |  |  |
| 压缩后大小 |  |  |
| 压缩率 |  |  |
| 分区数量 |  |  |
| 元数据开销 |  |  |
| 存储成本/小时 |  |  |

---

## 实验3: 故障注入与恢复测试

### 🎯 学习目标
- 模拟各种故障场景验证FTE恢复能力
- 理解不同故障类型的恢复策略
- 测量故障恢复的时间和成本
- 验证数据一致性保证

### 🔬 实验步骤

#### 步骤1: 准备故障注入工具
```bash
# 创建故障注入脚本
cat > fault_injection.py << 'EOF'
#!/usr/bin/env python3
import subprocess
import time
import random
import sys
from typing import List

class FaultInjector:
    def __init__(self, worker_nodes: List[str]):
        self.worker_nodes = worker_nodes
        
    def kill_random_worker(self) -> str:
        """随机杀死一个Worker进程"""
        node = random.choice(self.worker_nodes)
        print(f"Killing Trino worker on {node}")
        
        # SSH到节点并杀死Trino进程
        cmd = f"ssh {node} 'sudo pkill -f trino-server'"
        subprocess.run(cmd, shell=True)
        
        return node
        
    def restart_worker(self, node: str):
        """重启指定节点的Worker"""
        print(f"Restarting Trino worker on {node}")
        cmd = f"ssh {node} 'sudo systemctl start trino'"
        subprocess.run(cmd, shell=True)
        
    def simulate_network_partition(self, node: str, duration: int):
        """模拟网络分区"""
        print(f"Simulating network partition on {node} for {duration}s")
        
        # 阻塞到Coordinator的连接
        block_cmd = f"ssh {node} 'sudo iptables -A INPUT -p tcp --dport 8080 -j DROP'"
        subprocess.run(block_cmd, shell=True)
        
        time.sleep(duration)
        
        # 恢复网络连接
        restore_cmd = f"ssh {node} 'sudo iptables -D INPUT -p tcp --dport 8080 -j DROP'"
        subprocess.run(restore_cmd, shell=True)
        
    def simulate_storage_failure(self, duration: int):
        """模拟存储故障"""
        print(f"Simulating S3 access failure for {duration}s")
        
        # 通过修改S3凭证模拟存储不可用
        # 注意：这需要谨慎操作，确保可以恢复
        
    def run_fault_scenario(self, scenario: str):
        """运行特定的故障场景"""
        if scenario == "worker_failure":
            failed_node = self.kill_random_worker()
            time.sleep(10)  # 等待故障检测
            return {"type": "worker_failure", "node": failed_node}
            
        elif scenario == "network_partition":
            node = random.choice(self.worker_nodes)
            self.simulate_network_partition(node, 30)
            return {"type": "network_partition", "node": node}
            
        # 其他故障场景...

if __name__ == "__main__":
    # 替换为实际的Worker节点地址
    workers = ["worker1.example.com", "worker2.example.com", "worker3.example.com"]
    injector = FaultInjector(workers)
    
    if len(sys.argv) > 1:
        scenario = sys.argv[1]
        result = injector.run_fault_scenario(scenario)
        print(f"Fault injection result: {result}")
    else:
        print("Usage: python3 fault_injection.py <scenario>")
        print("Scenarios: worker_failure, network_partition")
EOF

chmod +x fault_injection.py
```

#### 步骤2: 故障恢复测试用例

##### 测试用例1: Worker节点故障
```sql
-- 启动一个长时间运行的查询
SELECT 
    c.mktsegment,
    EXTRACT(YEAR FROM o.orderdate) as order_year,
    COUNT(*) as order_count,
    SUM(o.totalprice) as total_revenue,
    AVG(o.totalprice) as avg_order_value,
    STDDEV(o.totalprice) as revenue_stddev,
    COUNT(DISTINCT c.custkey) as unique_customers,
    COUNT(DISTINCT o.orderkey) as unique_orders
FROM customer c
JOIN orders o ON c.custkey = o.custkey
JOIN lineitem l ON o.orderkey = l.orderkey
WHERE o.orderdate >= DATE '2020-01-01' 
  AND o.orderdate < DATE '2024-01-01'
GROUP BY 
    c.mktsegment, 
    EXTRACT(YEAR FROM o.orderdate)
ORDER BY 
    order_year, 
    total_revenue DESC;
```

```bash
# 在查询运行过程中注入Worker故障
python3 fault_injection.py worker_failure
```

##### 测试用例2: 网络分区故障
```sql
-- 启动另一个复杂查询
WITH monthly_customer_behavior AS (
  SELECT 
    c.custkey,
    c.name,
    c.mktsegment,
    DATE_TRUNC('month', o.orderdate) as order_month,
    COUNT(*) as monthly_orders,
    SUM(o.totalprice) as monthly_spend,
    COUNT(DISTINCT l.partkey) as distinct_parts_ordered
  FROM customer c
  JOIN orders o ON c.custkey = o.custkey
  JOIN lineitem l ON o.orderkey = l.orderkey
  WHERE o.orderdate >= DATE '2023-01-01'
  GROUP BY c.custkey, c.name, c.mktsegment, DATE_TRUNC('month', o.orderdate)
)
SELECT 
  mktsegment,
  order_month,
  COUNT(DISTINCT custkey) as active_customers,
  SUM(monthly_orders) as total_orders,
  SUM(monthly_spend) as total_revenue,
  AVG(monthly_spend) as avg_customer_spend,
  PERCENTILE(monthly_spend, 0.5) as median_spend,
  PERCENTILE(monthly_spend, 0.95) as p95_spend
FROM monthly_customer_behavior
GROUP BY mktsegment, order_month
ORDER BY order_month, total_revenue DESC;
```

### 📊 故障恢复测试结果

| 故障类型 | 检测时间 | 恢复时间 | 查询成功 | 数据一致性 | 备注 |
|---------|----------|----------|----------|------------|------|
| Worker故障 |  |  |  |  |  |
| 网络分区 |  |  |  |  |  |
| 存储故障 |  |  |  |  |  |
| 协调器故障 |  |  |  |  |  |

---

## 实验4: 性能开销测量与对比

### 🎯 学习目标
- 量化FTE相对传统模式的性能开销
- 分析不同工作负载下的开销差异
- 理解开销的来源和优化方向
- 建立ROI评估模型

### 🔬 实验步骤

#### 步骤1: 性能测试框架搭建
```python
#!/usr/bin/env python3
import time
import statistics
from dataclasses import dataclass
from typing import List, Dict
import trino

@dataclass
class QueryResult:
    query_id: str
    execution_time: float
    cpu_time: float
    peak_memory: int
    processed_bytes: int
    network_bytes: int
    success: bool
    error_message: str = ""

class PerformanceBenchmark:
    def __init__(self, trino_host: str, trino_port: int):
        self.trino_host = trino_host
        self.trino_port = trino_port
        
    def create_connection(self, fte_enabled: bool):
        """创建Trino连接"""
        session_properties = {}
        if fte_enabled:
            session_properties['fault_tolerant_execution_enabled'] = 'true'
        else:
            session_properties['fault_tolerant_execution_enabled'] = 'false'
            
        return trino.dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user='test_user',
            catalog='hive',
            schema='default',
            session_properties=session_properties
        )
    
    def execute_query_with_metrics(self, query: str, fte_enabled: bool) -> QueryResult:
        """执行查询并收集性能指标"""
        conn = self.create_connection(fte_enabled)
        cursor = conn.cursor()
        
        start_time = time.time()
        
        try:
            cursor.execute(query)
            result = cursor.fetchall()  # 确保查询完全执行
            
            execution_time = time.time() - start_time
            
            # 获取查询统计信息
            stats_query = f"""
            SELECT 
                elapsed_time_millis,
                total_cpu_time_millis,
                peak_user_memory_bytes,
                total_bytes,
                output_data_size_bytes
            FROM system.runtime.queries 
            WHERE query_id = '{cursor.query_id}'
            """
            
            cursor.execute(stats_query)
            stats = cursor.fetchone()
            
            return QueryResult(
                query_id=cursor.query_id,
                execution_time=execution_time,
                cpu_time=stats[1] / 1000.0 if stats else 0,
                peak_memory=stats[2] if stats else 0,
                processed_bytes=stats[3] if stats else 0,
                network_bytes=stats[4] if stats else 0,
                success=True
            )
            
        except Exception as e:
            return QueryResult(
                query_id="",
                execution_time=time.time() - start_time,
                cpu_time=0,
                peak_memory=0,
                processed_bytes=0,
                network_bytes=0,
                success=False,
                error_message=str(e)
            )
        finally:
            cursor.close()
            conn.close()
    
    def run_benchmark_suite(self, queries: List[str], iterations: int = 3) -> Dict:
        """运行完整的基准测试套件"""
        results = {
            'traditional': [],
            'fte': []
        }
        
        for query in queries:
            print(f"Testing query: {query[:50]}...")
            
            # 传统模式测试
            traditional_results = []
            for i in range(iterations):
                print(f"  Traditional mode iteration {i+1}/{iterations}")
                result = self.execute_query_with_metrics(query, fte_enabled=False)
                traditional_results.append(result)
            
            # FTE模式测试  
            fte_results = []
            for i in range(iterations):
                print(f"  FTE mode iteration {i+1}/{iterations}")
                result = self.execute_query_with_metrics(query, fte_enabled=True)
                fte_results.append(result)
                
            results['traditional'].append(traditional_results)
            results['fte'].append(fte_results)
            
        return results
    
    def analyze_performance_impact(self, results: Dict):
        """分析性能影响"""
        print("\n=== Performance Impact Analysis ===")
        
        for i, (trad_results, fte_results) in enumerate(zip(results['traditional'], results['fte'])):
            print(f"\nQuery {i+1} Analysis:")
            
            # 计算平均指标
            trad_avg_time = statistics.mean([r.execution_time for r in trad_results if r.success])
            fte_avg_time = statistics.mean([r.execution_time for r in fte_results if r.success])
            
            trad_avg_memory = statistics.mean([r.peak_memory for r in trad_results if r.success])
            fte_avg_memory = statistics.mean([r.peak_memory for r in fte_results if r.success])
            
            # 计算开销比例
            time_overhead = ((fte_avg_time - trad_avg_time) / trad_avg_time) * 100
            memory_overhead = ((fte_avg_memory - trad_avg_memory) / trad_avg_memory) * 100
            
            print(f"  Execution Time: {trad_avg_time:.2f}s → {fte_avg_time:.2f}s ({time_overhead:+.1f}%)")
            print(f"  Peak Memory: {trad_avg_memory/1024/1024:.1f}MB → {fte_avg_memory/1024/1024:.1f}MB ({memory_overhead:+.1f}%)")
            
            # 成功率对比
            trad_success_rate = sum(1 for r in trad_results if r.success) / len(trad_results)
            fte_success_rate = sum(1 for r in fte_results if r.success) / len(fte_results)
            
            print(f"  Success Rate: {trad_success_rate:.1%} → {fte_success_rate:.1%}")

# 测试查询集合
TEST_QUERIES = [
    # 短查询 (预期开销高)
    """
    SELECT mktsegment, COUNT(*) as customer_count
    FROM customer
    WHERE acctbal > 5000
    GROUP BY mktsegment
    ORDER BY customer_count DESC
    """,
    
    # 中等查询 (平衡开销)
    """
    SELECT 
        c.mktsegment,
        EXTRACT(YEAR FROM o.orderdate) as year,
        COUNT(*) as order_count,
        SUM(o.totalprice) as total_revenue
    FROM customer c
    JOIN orders o ON c.custkey = o.custkey
    WHERE o.orderdate >= DATE '2022-01-01'
    GROUP BY c.mktsegment, EXTRACT(YEAR FROM o.orderdate)
    ORDER BY year, total_revenue DESC
    """,
    
    # 长查询 (预期开销低)
    """
    SELECT 
        c.mktsegment,
        o.orderpriority,
        EXTRACT(YEAR FROM o.orderdate) as year,
        EXTRACT(QUARTER FROM o.orderdate) as quarter,
        COUNT(DISTINCT c.custkey) as unique_customers,
        COUNT(*) as total_orders,
        SUM(o.totalprice) as total_revenue,
        AVG(o.totalprice) as avg_order_value,
        COUNT(DISTINCT l.partkey) as unique_parts
    FROM customer c
    JOIN orders o ON c.custkey = o.custkey  
    JOIN lineitem l ON o.orderkey = l.orderkey
    WHERE o.orderdate >= DATE '2020-01-01'
      AND o.orderdate < DATE '2024-01-01'
    GROUP BY 
        c.mktsegment,
        o.orderpriority, 
        EXTRACT(YEAR FROM o.orderdate),
        EXTRACT(QUARTER FROM o.orderdate)
    ORDER BY year, quarter, total_revenue DESC
    """
]

if __name__ == "__main__":
    benchmark = PerformanceBenchmark("localhost", 8080)
    results = benchmark.run_benchmark_suite(TEST_QUERIES, iterations=3)
    benchmark.analyze_performance_impact(results)
```

### 📊 性能开销对比分析

| 查询类型 | 传统模式耗时 | FTE模式耗时 | 时间开销 | 内存开销 | 成功率提升 | ROI评估 |
|---------|-------------|-------------|----------|----------|------------|---------|
| 短查询(<5min) |  |  |  |  |  |  |
| 中查询(5-60min) |  |  |  |  |  |  |
| 长查询(1hr+) |  |  |  |  |  |  |

---

## 实验5: 推测执行机制验证

### 🎯 学习目标
- 理解推测执行的触发条件和机制
- 验证推测执行对性能的改善效果
- 分析推测执行的资源开销
- 掌握推测执行的调优参数

### 🔬 实验步骤

#### 步骤1: 配置推测执行参数
```properties
# 调整coordinator配置启用更积极的推测执行
fault-tolerant-execution-speculative-execution-enabled=true
fault-tolerant-execution-speculative-execution-threshold=30s
fault-tolerant-execution-speculative-execution-min-task-split-count=4

# 调整推测执行的触发条件
query.low-memory-killer.delay=30s
task.max-worker-threads=200
```

#### 步骤2: 创建慢任务模拟环境
```sql
-- 创建数据倾斜场景来触发推测执行
-- 先创建一个有数据倾斜的测试表
CREATE TABLE skewed_orders AS
SELECT 
    CASE 
        WHEN random() < 0.1 THEN 1  -- 10%的数据集中在custkey=1
        WHEN random() < 0.2 THEN 2  -- 10%的数据集中在custkey=2  
        ELSE CAST(random() * 100000 AS INTEGER) + 3
    END as custkey,
    orderkey,
    totalprice,
    orderdate,
    orderpriority
FROM orders
WHERE orderdate >= DATE '2023-01-01';

-- 收集统计信息
ANALYZE TABLE skewed_orders;

-- 验证数据倾斜
SELECT 
    custkey,
    COUNT(*) as order_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM skewed_orders
GROUP BY custkey
ORDER BY order_count DESC
LIMIT 10;
```

#### 步骤3: 推测执行监控查询
```python
#!/usr/bin/env python3
import trino
import time
import threading
from collections import defaultdict

class SpeculativeExecutionMonitor:
    def __init__(self, trino_host, trino_port):
        self.trino_host = trino_host
        self.trino_port = trino_port
        self.monitoring = False
        self.query_stats = defaultdict(list)
        
    def monitor_query_execution(self, query_id: str):
        """监控特定查询的执行情况"""
        conn = trino.dbapi.connect(
            host=self.trino_host,
            port=self.trino_port,
            user='monitor'
        )
        cursor = conn.cursor()
        
        while self.monitoring:
            try:
                # 查询任务执行状态
                stats_query = f"""
                SELECT 
                    task_id,
                    state,
                    total_scheduled_time_millis,
                    total_cpu_time_millis,
                    total_blocked_time_millis,
                    peak_user_memory_bytes,
                    speculative,
                    created,
                    end_time
                FROM system.runtime.tasks
                WHERE query_id = '{query_id}'
                ORDER BY created
                """
                
                cursor.execute(stats_query)
                tasks = cursor.fetchall()
                
                current_time = time.time()
                snapshot = {
                    'timestamp': current_time,
                    'tasks': []
                }
                
                for task in tasks:
                    task_info = {
                        'task_id': task[0],
                        'state': task[1], 
                        'total_time_ms': task[2],
                        'cpu_time_ms': task[3],
                        'blocked_time_ms': task[4],
                        'peak_memory_bytes': task[5],
                        'is_speculative': task[6],
                        'created': task[7],
                        'end_time': task[8]
                    }
                    snapshot['tasks'].append(task_info)
                
                self.query_stats[query_id].append(snapshot)
                
                # 检查是否有推测任务启动
                speculative_tasks = [t for t in tasks if t[6]]  # speculative = true
                if speculative_tasks:
                    print(f"[{current_time:.1f}] Detected {len(speculative_tasks)} speculative tasks for query {query_id}")
                
                time.sleep(2)  # 每2秒采样一次
                
            except Exception as e:
                print(f"Monitoring error: {e}")
                break
                
        cursor.close()
        conn.close()
    
    def start_monitoring(self, query_id: str):
        """开始监控"""
        self.monitoring = True
        monitor_thread = threading.Thread(
            target=self.monitor_query_execution, 
            args=(query_id,)
        )
        monitor_thread.start()
        return monitor_thread
    
    def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
    
    def analyze_speculative_execution(self, query_id: str):
        """分析推测执行的效果"""
        snapshots = self.query_stats[query_id]
        if not snapshots:
            print("No monitoring data available")
            return
        
        print(f"\n=== Speculative Execution Analysis for {query_id} ===")
        
        # 分析任务完成时间分布
        final_snapshot = snapshots[-1]
        completed_tasks = [t for t in final_snapshot['tasks'] if t['state'] == 'FINISHED']
        speculative_tasks = [t for t in completed_tasks if t['is_speculative']]
        regular_tasks = [t for t in completed_tasks if not t['is_speculative']]
        
        if speculative_tasks:
            print(f"Regular tasks: {len(regular_tasks)}")
            print(f"Speculative tasks: {len(speculative_tasks)}")
            
            # 计算平均执行时间
            if regular_tasks:
                avg_regular_time = sum(t['total_time_ms'] for t in regular_tasks) / len(regular_tasks)
                print(f"Average regular task time: {avg_regular_time:.0f}ms")
            
            if speculative_tasks:
                avg_speculative_time = sum(t['total_time_ms'] for t in speculative_tasks) / len(speculative_tasks)
                print(f"Average speculative task time: {avg_speculative_time:.0f}ms")
                
                # 计算推测执行节省的时间
                if regular_tasks and speculative_tasks:
                    time_savings = avg_regular_time - avg_speculative_time
                    print(f"Time savings from speculation: {time_savings:.0f}ms ({time_savings/avg_regular_time*100:.1f}%)")
        else:
            print("No speculative tasks detected")
        
        # 分析推测执行的触发时机
        self._analyze_speculation_triggers(snapshots)
    
    def _analyze_speculation_triggers(self, snapshots):
        """分析推测执行的触发时机"""
        print("\n--- Speculation Trigger Analysis ---")
        
        for i, snapshot in enumerate(snapshots):
            speculative_count = sum(1 for t in snapshot['tasks'] if t['is_speculative'])
            if speculative_count > 0:
                print(f"Snapshot {i}: {speculative_count} speculative tasks detected")
                
                # 分析触发推测执行时的任务状态分布
                states = {}
                for task in snapshot['tasks']:
                    state = task['state']
                    states[state] = states.get(state, 0) + 1
                
                print(f"  Task states: {states}")

# 使用监控工具
if __name__ == "__main__":
    monitor = SpeculativeExecutionMonitor("localhost", 8080)
    
    # 启动监控
    query_id = "your_query_id_here"  # 需要替换为实际的query_id
    monitor_thread = monitor.start_monitoring(query_id)
    
    # 监控一段时间后停止
    time.sleep(300)  # 监控5分钟
    monitor.stop_monitoring()
    monitor_thread.join()
    
    # 分析结果
    monitor.analyze_speculative_execution(query_id)
```

### 📊 推测执行效果分析

| 场景 | 慢任务数量 | 推测任务数量 | 时间节省 | 资源开销 | 成功率 |
|------|------------|-------------|----------|----------|--------|
| 数据倾斜 |  |  |  |  |  |
| 节点性能差异 |  |  |  |  |  |
| 网络抖动 |  |  |  |  |  |

---

## 实验6-8: 其他高级实验

### 实验6: 大规模ETL容错实战
- 设计24小时连续运行的ETL流水线
- 模拟多种故障组合场景
- 验证数据完整性和一致性
- 测量总体TCO改善

### 实验7: FTE监控与告警系统  
- 搭建Prometheus+Grafana监控
- 配置关键指标告警规则
- 实现自动化故障响应
- 建立SLA监控体系

### 实验8: 成本优化与容量规划
- 分析FTE的存储和计算成本
- 建立容量预测模型
- 优化数据生命周期策略
- 制定成本控制策略

---

## 🎯 实验总结与进阶

完成这些FTE实验后，你将：

### ✅ **掌握企业级容错能力**
- 理解FTE在大规模生产环境的价值
- 具备配置和调优FTE系统的技能
- 建立完整的故障响应和恢复机制

### ✅ **建立可靠性工程思维**
- 从系统可靠性角度思考架构设计
- 掌握故障注入和混沌工程实践
- 具备ROI驱动的技术决策能力

### ✅ **获得竞争优势**
- FTE是Trino的差异化特性
- 企业级容错能力是核心竞争力
- 可靠性专家在市场上稀缺且高值

### 🚀 **应用场景**
1. **关键ETL流水线**: 确保数据处理的可靠完成
2. **大规模分析平台**: 提供企业级SLA保证
3. **多云混合架构**: 跨环境的容错执行
4. **成本敏感场景**: 避免重复计算的巨大浪费

通过掌握FTE技术，你已经具备了构建和运维企业级可靠查询平台的核心能力！这项技能将让你在数据工程领域脱颖而出！🌟
