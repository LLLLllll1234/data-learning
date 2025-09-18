# Trino Performance Tuning Scripts 使用指南

这个目录包含了一套完整的Trino性能分析和调优工具，帮助你监控、分析和优化Trino集群的性能。

## 📁 文件结构

```
scripts/
├── trino-performance-tuning.sql    # SQL查询脚本集合
├── trino_performance_analyzer.py   # Python自动化分析工具
├── config_template.yaml           # 配置文件模板
├── requirements.txt               # Python依赖包列表
└── README.md                     # 本文档
```

## 🚀 快速开始

### 1. 环境准备

```bash
# 安装Python依赖
pip install -r requirements.txt

# 复制配置文件模板
cp config_template.yaml config.yaml

# 根据实际环境修改配置
vim config.yaml
```

### 2. 基本使用

#### SQL脚本方式 (手动分析)

```sql
-- 连接到Trino并执行相关SQL查询
-- 复制trino-performance-tuning.sql中的查询到Trino客户端执行

-- 例如：查看当前运行的查询
SELECT 
    query_id, state, user_name, 
    date_diff('second', created, now()) as runtime_seconds
FROM system.runtime.queries
WHERE state = 'RUNNING'
ORDER BY created DESC;
```

#### Python脚本方式 (自动化分析)

```bash
# 生成性能报告
python trino_performance_analyzer.py --report --days 7

# 创建可视化仪表板
python trino_performance_analyzer.py --dashboard --days 7

# 分析查询模式
python trino_performance_analyzer.py --analyze-patterns --days 3

# 使用自定义配置文件
python trino_performance_analyzer.py --config config.yaml --report
```

## 📊 功能特性

### SQL脚本功能

#### 1. 查询性能分析
- 查看当前运行查询状态
- 分析最近完成查询的性能统计
- 识别查询失败原因和模式
- 监控长时间排队的查询

#### 2. 统计信息维护
- 检查表统计信息完整性和新鲜度
- 分析列统计信息质量
- 自动生成ANALYZE语句脚本
- 识别需要重新收集统计的大表

#### 3. Join性能优化
- 识别可能需要广播Join优化的查询
- 分析Join数据倾斜问题
- 评估Join策略选择效果

#### 4. 资源使用分析
- 内存使用趋势分析
- 按用户分析资源使用模式
- 集群负载趋势监控

#### 5. 慢查询诊断
- 识别和分类慢查询
- 分析查询Stage执行情况
- 定位性能瓶颈类型

#### 6. 优化建议生成
- 针对不同workload的参数调优建议
- 表设计优化建议
- 自动化优化策略推荐

### Python脚本功能

#### 1. 自动化性能报告
- 生成全面的性能分析报告
- 支持JSON和HTML格式导出
- 包含优化建议和行动项

#### 2. 可视化仪表板
- 交互式性能趋势图表
- 多维度资源使用监控
- 实时性能指标展示

#### 3. 查询模式分析
- 识别高频查询模式
- 分析查询性能变化
- 优化潜力评估

#### 4. 智能优化建议
- 基于历史数据的智能分析
- 自动生成针对性优化建议
- 优先级排序和影响评估

## 💡 使用场景

### 日常监控 (推荐每日执行)

```bash
# 生成每日性能报告
python trino_performance_analyzer.py --report --days 1 --format html

# 检查统计信息健康度
python trino_performance_analyzer.py --report | grep -A 10 "stats_health"
```

相关SQL查询:
```sql
-- 查看昨天的查询性能概况 (1.2)
-- 检查内存使用趋势 (4.1)  
-- 检查统计信息完整性 (2.1)
```

### 周度分析 (推荐每周执行)

```bash  
# 生成周度性能分析报告
python trino_performance_analyzer.py --report --dashboard --days 7

# 分析查询模式变化
python trino_performance_analyzer.py --analyze-patterns --days 7
```

相关SQL查询:
```sql
-- 分析Join性能模式 (3.2)
-- 识别慢查询模式 (5.1)
-- 分析表访问模式 (8.1)
```

### 问题诊断 (按需执行)

```bash
# 诊断当前性能问题
python trino_performance_analyzer.py --report --days 1 --threshold 60

# 分析特定时间段的问题
python trino_performance_analyzer.py --dashboard --days 2
```

相关SQL查询:
```sql
-- 查看当前运行状态 (1.1)
-- 分析具体慢查询的Stage (5.2)  
-- 查看失败原因分布 (1.3)
```

### 容量规划 (推荐每月执行)

```bash
# 生成月度资源使用分析
python trino_performance_analyzer.py --report --days 30
```

相关SQL查询:
```sql
-- 按用户分析资源使用模式 (4.2)
-- 集群负载趋势分析 (4.3)
-- 节点健康状态检查 (9.1)
```

## 🔧 配置说明

### config.yaml 关键配置项

```yaml
# Trino连接配置
trino:
  host: your-trino-coordinator      # 修改为实际的coordinator地址
  port: 8080                       # 修改为实际端口
  user: your_username              # 修改为实际用户名

# 分析阈值配置  
analysis:
  slow_query_threshold: 300        # 慢查询定义阈值，建议根据实际SLA调整
  memory_threshold_gb: 10          # 内存告警阈值，建议根据集群规模调整
  failure_rate_threshold: 0.05     # 失败率告警阈值，建议保持5%以下

# 报告配置
reporting:
  days_lookback: 7                 # 默认分析回溯天数，可根据需要调整
```

### 环境变量配置 (可选)

```bash
# 设置Trino连接环境变量
export TRINO_HOST=your-coordinator-host
export TRINO_PORT=8080
export TRINO_USER=your-username
export TRINO_PASSWORD=your-password
```

## 📈 报告解读

### 性能报告关键指标

#### 基础统计部分
- **total_queries**: 总查询数量
- **success_rate**: 成功率，应保持在95%以上
- **avg_runtime_seconds**: 平均运行时间
- **p95_runtime_seconds**: 95分位运行时间，SLA关键指标
- **total_processed_tb**: 总处理数据量
- **avg_memory_gb**: 平均内存使用

#### 慢查询分析部分
- **slow_query_count**: 慢查询数量
- **avg_slow_runtime**: 慢查询平均运行时间
- **top_slow_queries**: 最慢的查询列表

#### 优化建议部分
- **category**: 建议类别 (stability/performance/resource/statistics)
- **priority**: 优先级 (high/medium/low)
- **recommendation**: 具体建议内容
- **action**: 可执行的操作命令

### 仪表板图表解读

1. **查询数量趋势**: 反映系统负载变化
2. **运行时间趋势**: 反映性能稳定性
3. **内存使用趋势**: 反映资源压力
4. **失败率趋势**: 反映系统健康度
5. **95分位运行时间**: 反映用户体验
6. **查询数量分布**: 反映使用模式

## ⚠️ 注意事项

### 权限要求
- 需要能够访问 `system.runtime.*` 表的权限
- 需要能够访问 `system.metadata.*` 表的权限
- 如果要执行ANALYZE语句，需要相应的表权限

### 性能考虑
- 分析脚本本身会产生查询负载，建议在低峰时期运行
- 对于大型集群，建议适当调整查询的时间范围
- 统计信息相关的查询可能较慢，请耐心等待

### 数据保留
- Trino的系统表数据有保留期限制，历史数据可能不完整
- 建议定期导出重要的性能数据用于长期分析
- 可以考虑将分析结果存储到外部数据仓库

## 🛠️ 故障排除

### 常见问题

#### 1. 连接失败
```bash
# 检查网络连通性
telnet your-coordinator-host 8080

# 检查认证配置
curl -u username:password http://your-coordinator-host:8080/v1/info
```

#### 2. 权限不足
```sql
-- 检查当前用户权限
SHOW GRANTS;

-- 请求管理员授予system表访问权限
```

#### 3. Python依赖问题
```bash
# 升级pip
pip install --upgrade pip

# 强制重新安装依赖
pip install --force-reinstall -r requirements.txt
```

#### 4. 配置文件问题
```bash
# 验证YAML语法
python -c "import yaml; yaml.safe_load(open('config.yaml'))"

# 使用默认配置测试
python trino_performance_analyzer.py --report
```

## 📞 获取帮助

如果遇到问题或需要支持，请：

1. 检查日志文件 `trino_analyzer.log` 中的错误信息
2. 确认配置文件格式和内容正确
3. 验证Trino集群连接和权限
4. 查看本文档的故障排除部分

---

**提示**: 这些工具旨在帮助你更好地理解和优化Trino性能。建议从基础的SQL查询开始，逐步熟悉各项功能后再使用高级的Python分析工具。
