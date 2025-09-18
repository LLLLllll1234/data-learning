-- ============================================================================
-- Trino Performance Tuning & Analysis Scripts Collection
-- ============================================================================
-- 这个SQL脚本集合提供了一套完整的Trino性能分析和调优工具
-- 使用方法：根据需要复制相应的SQL语句到Trino客户端执行

-- ============================================================================
-- 1. 查询性能分析
-- ============================================================================

-- 1.1 查看当前运行的查询
SELECT 
    query_id,
    state,
    user_name,
    source,
    query,
    date_diff('second', created, now()) as runtime_seconds,
    queued_time.seconds as queued_seconds,
    analysis_time.seconds as analysis_seconds,  
    planning_time.seconds as planning_seconds,
    total_bytes,
    total_rows,
    peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0 as peak_memory_gb
FROM system.runtime.queries
WHERE state = 'RUNNING'
ORDER BY created DESC;

-- 1.2 查看最近完成的查询性能统计
SELECT 
    query_id,
    state,
    user_name,
    left(query, 100) as query_preview,
    elapsed_time.seconds as total_seconds,
    queued_time.seconds as queued_seconds,
    planning_time.seconds as planning_seconds,
    analysis_time.seconds as analysis_seconds,
    execution_time.seconds as execution_seconds,
    total_bytes / 1024.0 / 1024.0 / 1024.0 as total_gb,
    total_rows / 1000000.0 as total_million_rows,
    peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0 as peak_memory_gb,
    peak_total_memory_bytes / 1024.0 / 1024.0 / 1024.0 as peak_total_memory_gb,
    cumulative_user_memory / 1024.0 / 1024.0 / 1024.0 as cumulative_memory_gb
FROM system.runtime.queries
WHERE created >= current_timestamp - INTERVAL '1' HOUR
  AND state IN ('FINISHED', 'FAILED')
ORDER BY elapsed_time.seconds DESC
LIMIT 20;

-- 1.3 查看查询失败统计  
SELECT 
    error_type,
    COUNT(*) as failure_count,
    array_agg(DISTINCT user_name) as affected_users,
    min(created) as first_occurrence,
    max(created) as last_occurrence
FROM system.runtime.queries
WHERE state = 'FAILED'
  AND created >= current_timestamp - INTERVAL '24' HOUR
GROUP BY error_type
ORDER BY failure_count DESC;

-- 1.4 分析长时间排队的查询
SELECT 
    query_id,
    user_name,
    left(query, 150) as query_preview,
    queued_time.seconds as queued_seconds,
    elapsed_time.seconds as total_seconds,
    resource_group_name,
    state
FROM system.runtime.queries
WHERE queued_time.seconds > 30
  AND created >= current_timestamp - INTERVAL '6' HOUR
ORDER BY queued_time.seconds DESC;

-- ============================================================================
-- 2. 统计信息分析与维护
-- ============================================================================

-- 2.1 检查表统计信息的完整性和新鲜度
SELECT 
    table_catalog,
    table_schema,
    table_name,
    row_count,
    data_size / 1024.0 / 1024.0 / 1024.0 as data_size_gb,
    CASE 
        WHEN row_count IS NULL THEN '❌ Missing'
        WHEN row_count = 0 THEN '⚠️ Empty or Stale'
        ELSE '✅ Available'
    END as stats_status
FROM system.metadata.table_statistics
WHERE table_schema NOT IN ('information_schema', 'system')
ORDER BY 
    CASE WHEN row_count IS NULL THEN 0 ELSE 1 END,
    data_size DESC;

-- 2.2 检查列统计信息质量
SELECT 
    table_schema,
    table_name,
    column_name,
    data_type,
    distinct_values_count,
    nulls_fraction,
    data_size / 1024.0 / 1024.0 as data_size_mb,
    CASE 
        WHEN distinct_values_count IS NULL THEN '❌ Missing NDV'
        WHEN distinct_values_count = 0 THEN '⚠️ Zero NDV'
        WHEN nulls_fraction IS NULL THEN '⚠️ Missing Null Info'
        ELSE '✅ Complete'
    END as stats_quality
FROM system.metadata.column_statistics
WHERE table_schema NOT IN ('information_schema', 'system')
  AND (distinct_values_count IS NULL OR distinct_values_count = 0 OR nulls_fraction IS NULL)
ORDER BY table_schema, table_name, column_name;

-- 2.3 生成ANALYZE语句脚本
SELECT 
    'ANALYZE TABLE ' || table_catalog || '.' || table_schema || '.' || table_name || ';' as analyze_command
FROM system.metadata.table_statistics
WHERE table_schema NOT IN ('information_schema', 'system')
  AND (row_count IS NULL OR row_count = 0)
ORDER BY table_schema, table_name;

-- 2.4 识别需要重新收集统计的大表
SELECT 
    table_catalog || '.' || table_schema || '.' || table_name as full_table_name,
    row_count,
    data_size / 1024.0 / 1024.0 / 1024.0 as data_size_gb,
    'ANALYZE TABLE ' || table_catalog || '.' || table_schema || '.' || table_name || 
    ' WITH (sample_percentage = ' || 
    CASE 
        WHEN data_size > 100 * 1024 * 1024 * 1024 THEN '1.0'  -- 100GB+ : 1%采样
        WHEN data_size > 10 * 1024 * 1024 * 1024 THEN '5.0'   -- 10GB+ : 5%采样  
        ELSE '10.0'  -- 10GB- : 10%采样
    END || ');' as recommended_analyze
FROM system.metadata.table_statistics  
WHERE table_schema NOT IN ('information_schema', 'system')
  AND data_size > 1024 * 1024 * 1024  -- 只分析1GB以上的表
  AND (row_count IS NULL OR row_count = 0)
ORDER BY data_size DESC;

-- ============================================================================
-- 3. Join性能优化分析
-- ============================================================================

-- 3.1 识别可能需要广播Join优化的查询模式
-- 注意：这需要在实际执行计划中查看，这里提供分析思路
WITH frequent_joins AS (
  SELECT 
    user_name,
    regexp_extract(lower(query), '(from\s+\w+\.\w+\.\w+).*?(join\s+\w+\.\w+\.\w+)', 2) as joined_table,
    COUNT(*) as join_frequency,
    AVG(elapsed_time.seconds) as avg_seconds,
    AVG(total_bytes) as avg_bytes
  FROM system.runtime.queries
  WHERE query LIKE '%JOIN%'
    AND state = 'FINISHED' 
    AND created >= current_timestamp - INTERVAL '7' DAY
  GROUP BY 1, 2
)
SELECT 
  user_name,
  joined_table,
  join_frequency,
  avg_seconds,
  avg_bytes / 1024.0 / 1024.0 / 1024.0 as avg_gb,
  CASE 
    WHEN avg_seconds > 60 AND avg_gb < 1 THEN '🎯 Consider Broadcast Join'
    WHEN avg_seconds > 300 THEN '⚡ Needs Optimization'
    ELSE '✅ Performance OK'
  END as optimization_hint
FROM frequent_joins
WHERE join_frequency >= 5
ORDER BY avg_seconds DESC, join_frequency DESC;

-- 3.2 分析Join倾斜问题的查询
-- 查看执行时间方差较大的重复查询模式
WITH query_patterns AS (
  SELECT 
    regexp_replace(
      regexp_replace(query, '\d+', 'N'),  -- 数字替换为N
      '''[^'']*''', '''X'''               -- 字符串替换为X
    ) as query_pattern,
    elapsed_time.seconds as seconds,
    total_bytes,
    user_name
  FROM system.runtime.queries
  WHERE state = 'FINISHED'
    AND created >= current_timestamp - INTERVAL '3' DAY
    AND query LIKE '%JOIN%'
),
pattern_stats AS (
  SELECT 
    query_pattern,
    COUNT(*) as execution_count,
    AVG(seconds) as avg_seconds,
    STDDEV(seconds) as stddev_seconds,
    MIN(seconds) as min_seconds,
    MAX(seconds) as max_seconds
  FROM query_patterns
  GROUP BY query_pattern
  HAVING COUNT(*) >= 3
)
SELECT 
  left(query_pattern, 200) as pattern_preview,
  execution_count,
  avg_seconds,
  stddev_seconds,
  min_seconds,
  max_seconds,
  max_seconds / NULLIF(min_seconds, 0) as performance_ratio,
  CASE 
    WHEN stddev_seconds > avg_seconds * 0.5 THEN '⚠️ High Variance - Check for Skew'
    WHEN max_seconds / NULLIF(min_seconds, 0) > 5 THEN '🔍 Investigate Data Skew'
    ELSE '✅ Consistent Performance'
  END as skew_indicator
FROM pattern_stats
WHERE stddev_seconds IS NOT NULL
ORDER BY stddev_seconds / NULLIF(avg_seconds, 0) DESC;

-- ============================================================================
-- 4. 资源使用分析
-- ============================================================================

-- 4.1 内存使用分析
SELECT 
    date_trunc('hour', created) as hour,
    COUNT(*) as query_count,
    AVG(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0) as avg_memory_gb,
    PERCENTILE(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0, 0.95) as p95_memory_gb,
    MAX(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0) as max_memory_gb,
    COUNT(CASE WHEN error_code = 'EXCEEDED_MEMORY_LIMIT' THEN 1 END) as oom_failures
FROM system.runtime.queries
WHERE created >= current_timestamp - INTERVAL '24' HOUR
GROUP BY 1
ORDER BY 1 DESC;

-- 4.2 按用户分析资源使用模式
SELECT 
    user_name,
    COUNT(*) as total_queries,
    COUNT(CASE WHEN state = 'FINISHED' THEN 1 END) as successful_queries,
    COUNT(CASE WHEN state = 'FAILED' THEN 1 END) as failed_queries,
    AVG(elapsed_time.seconds) as avg_runtime_seconds,
    SUM(total_bytes) / 1024.0 / 1024.0 / 1024.0 / 1024.0 as total_processed_tb,
    AVG(peak_user_memory_bytes) / 1024.0 / 1024.0 / 1024.0 as avg_memory_gb,
    MAX(peak_user_memory_bytes) / 1024.0 / 1024.0 / 1024.0 as max_memory_gb
FROM system.runtime.queries
WHERE created >= current_timestamp - INTERVAL '7' DAY
GROUP BY user_name
HAVING COUNT(*) >= 10
ORDER BY total_processed_tb DESC;

-- 4.3 集群负载趋势分析
SELECT 
    date_trunc('hour', created) as hour,
    COUNT(*) as total_queries,
    COUNT(CASE WHEN state = 'RUNNING' THEN 1 END) as running_queries,
    COUNT(CASE WHEN state = 'QUEUED' THEN 1 END) as queued_queries,
    AVG(queued_time.seconds) as avg_queue_time,
    PERCENTILE(queued_time.seconds, 0.95) as p95_queue_time,
    SUM(total_bytes) / 1024.0 / 1024.0 / 1024.0 / 1024.0 as total_data_tb
FROM system.runtime.queries  
WHERE created >= current_timestamp - INTERVAL '48' HOUR
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================================================
-- 5. 慢查询诊断
-- ============================================================================

-- 5.1 识别慢查询模式
WITH slow_queries AS (
  SELECT 
    query_id,
    user_name,
    query,
    elapsed_time.seconds as total_seconds,
    queued_time.seconds as queued_seconds,
    planning_time.seconds as planning_seconds,
    analysis_time.seconds as analysis_seconds,
    execution_time.seconds as execution_seconds,
    total_bytes / 1024.0 / 1024.0 / 1024.0 as processed_gb,
    total_rows / 1000000.0 as processed_million_rows,
    peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0 as peak_memory_gb
  FROM system.runtime.queries
  WHERE state = 'FINISHED'
    AND elapsed_time.seconds > 60  -- 超过1分钟的查询
    AND created >= current_timestamp - INTERVAL '24' HOUR
)
SELECT 
  query_id,
  user_name,
  left(query, 100) as query_preview,
  total_seconds,
  planning_seconds / total_seconds * 100 as planning_pct,
  execution_seconds / total_seconds * 100 as execution_pct,
  processed_gb,
  processed_million_rows,
  processed_gb / NULLIF(total_seconds, 0) as throughput_gb_per_sec,
  peak_memory_gb,
  CASE 
    WHEN planning_seconds / total_seconds > 0.5 THEN '🧠 Planning Bottleneck'
    WHEN processed_gb / NULLIF(total_seconds, 0) < 0.1 THEN '💿 IO Bottleneck'  
    WHEN peak_memory_gb > 50 THEN '💾 Memory Intensive'
    ELSE '⚡ CPU Intensive'
  END as bottleneck_type
FROM slow_queries
ORDER BY total_seconds DESC;

-- 5.2 查看具体慢查询的Stage分析
-- 注意：需要替换具体的query_id
SELECT 
    stage_id,
    state,
    total_tasks,
    running_tasks, 
    completed_tasks,
    total_splits,
    queued_splits,
    running_splits,
    wall_time.seconds as wall_seconds,
    total_cpu_time.seconds as cpu_seconds,
    raw_input_data_size / 1024.0 / 1024.0 as input_mb,
    processed_input_data_size / 1024.0 / 1024.0 as processed_mb,
    output_data_size / 1024.0 / 1024.0 as output_mb
FROM system.runtime.stages
WHERE query_id = '<YOUR_QUERY_ID_HERE>'  -- 替换为实际的query_id
ORDER BY stage_id;

-- ============================================================================  
-- 6. 会话参数优化建议
-- ============================================================================

-- 6.1 生成针对不同workload的参数调优建议
WITH workload_analysis AS (
  SELECT 
    user_name,
    COUNT(*) as query_count,
    AVG(elapsed_time.seconds) as avg_runtime,
    AVG(total_bytes / 1024.0 / 1024.0 / 1024.0) as avg_data_gb,
    AVG(peak_user_memory_bytes / 1024.0 / 1024.0 / 1024.0) as avg_memory_gb,
    COUNT(CASE WHEN query LIKE '%JOIN%' THEN 1 END) as join_queries,
    COUNT(CASE WHEN query LIKE '%GROUP BY%' THEN 1 END) as agg_queries,
    COUNT(CASE WHEN query LIKE '%ORDER BY%' THEN 1 END) as sort_queries
  FROM system.runtime.queries
  WHERE created >= current_timestamp - INTERVAL '7' DAY
    AND state = 'FINISHED'
  GROUP BY user_name
  HAVING COUNT(*) >= 20
)
SELECT 
  user_name,
  query_count,
  avg_runtime,
  avg_data_gb,
  avg_memory_gb,
  join_queries,
  CASE 
    WHEN avg_data_gb < 1 AND join_queries > query_count * 0.5 THEN
      'SET SESSION broadcast_join_threshold = ''200MB''; -- 适合小数据Join'
    WHEN avg_memory_gb > 20 THEN
      'SET SESSION query_max_memory_per_node = ''8GB''; -- 控制内存使用'
    WHEN avg_runtime > 300 THEN  
      'SET SESSION enable_dynamic_filtering = true; -- 启用动态过滤'
    ELSE 'Current settings likely optimal'
  END as tuning_recommendation
FROM workload_analysis
ORDER BY avg_runtime DESC;

-- ============================================================================
-- 7. 动态过滤效果分析  
-- ============================================================================

-- 7.1 查看动态过滤的使用情况 (需要启用event listener)
-- 注意：这个查询可能需要根据你的监控系统调整
SELECT 
    date_trunc('day', created) as day,
    COUNT(*) as total_queries,
    COUNT(CASE WHEN query LIKE '%DynamicFilter%' THEN 1 END) as dynamic_filter_queries,
    AVG(CASE WHEN query LIKE '%DynamicFilter%' THEN elapsed_time.seconds END) as avg_df_runtime,
    AVG(CASE WHEN query NOT LIKE '%DynamicFilter%' THEN elapsed_time.seconds END) as avg_regular_runtime
FROM system.runtime.queries
WHERE created >= current_timestamp - INTERVAL '30' DAY
  AND state = 'FINISHED'
  AND query LIKE '%JOIN%'
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================================================
-- 8. 表设计优化建议
-- ============================================================================

-- 8.1 识别经常全表扫描的大表
WITH table_access_patterns AS (
  SELECT 
    regexp_extract(query, 'FROM\s+(\w+\.\w+\.\w+)', 1) as table_name,
    COUNT(*) as scan_frequency,
    AVG(elapsed_time.seconds) as avg_scan_time,
    AVG(total_bytes / 1024.0 / 1024.0 / 1024.0) as avg_scanned_gb,
    COUNT(CASE WHEN query NOT LIKE '%WHERE%' THEN 1 END) as full_scans
  FROM system.runtime.queries
  WHERE query LIKE '%SELECT%'
    AND query LIKE '%FROM%'
    AND created >= current_timestamp - INTERVAL '7' DAY
    AND state = 'FINISHED'
  GROUP BY 1
  HAVING table_name IS NOT NULL
),
table_stats AS (
  SELECT 
    table_catalog || '.' || table_schema || '.' || table_name as full_name,
    data_size / 1024.0 / 1024.0 / 1024.0 as table_size_gb
  FROM system.metadata.table_statistics
)
SELECT 
  tap.table_name,
  ts.table_size_gb,
  tap.scan_frequency,
  tap.avg_scan_time,
  tap.avg_scanned_gb,
  tap.full_scans,
  tap.full_scans * 100.0 / tap.scan_frequency as full_scan_ratio,
  CASE 
    WHEN ts.table_size_gb > 10 AND tap.full_scans > tap.scan_frequency * 0.3 THEN
      '📊 Consider partitioning by frequently filtered columns'
    WHEN tap.avg_scan_time > 300 AND ts.table_size_gb > 50 THEN
      '🗂️ Consider columnar format (Parquet/ORC) and compression'
    WHEN tap.scan_frequency > 100 AND tap.avg_scan_time > 60 THEN
      '🚀 Consider pre-aggregation or materialized views'
    ELSE '✅ Table access pattern looks reasonable'
  END as optimization_suggestion  
FROM table_access_patterns tap
LEFT JOIN table_stats ts ON tap.table_name = ts.full_name
WHERE ts.table_size_gb IS NOT NULL
ORDER BY tap.full_scans DESC, ts.table_size_gb DESC;

-- ============================================================================
-- 9. 集群健康度监控
-- ============================================================================

-- 9.1 节点健康状态检查
-- 注意：这个查询需要有适当的权限访问system.runtime.nodes
SELECT 
    node_id,
    http_uri,
    node_version,
    coordinator,
    state,
    age(now(), last_request_time) as last_seen,
    recent_failures,
    recent_successes  
FROM system.runtime.nodes
ORDER BY coordinator DESC, state, node_id;

-- 9.2 资源组使用统计  
SELECT 
    name as resource_group,
    state,
    queued_queries,
    running_queries, 
    total_queued_time.seconds / 3600.0 as total_queued_hours,
    cpu_usage_millis / 1000.0 / 3600.0 as cpu_hours
FROM system.runtime.resource_groups
WHERE queued_queries > 0 OR running_queries > 0
ORDER BY queued_queries DESC, running_queries DESC;

-- ============================================================================
-- 使用说明和最佳实践
-- ============================================================================

/*
使用建议：

1. 日常监控（每日执行）：
   - 运行 1.2 查看当天查询性能概况
   - 运行 4.1 检查内存使用趋势
   - 运行 2.1 检查统计信息完整性

2. 周度分析（每周执行）：
   - 运行 3.2 分析Join性能模式
   - 运行 5.1 识别慢查询模式  
   - 运行 8.1 分析表访问模式

3. 按需诊断（问题出现时）：
   - 运行 1.1 查看当前运行状态
   - 运行 5.2 分析具体慢查询的Stage
   - 运行 1.3 查看失败原因分布

4. 优化建议应用：
   - 根据 2.3/2.4 的输出执行ANALYZE语句
   - 参考 6.1 调整会话参数  
   - 根据 8.1 考虑表设计优化

注意事项：
- 某些查询需要替换具体的参数（如query_id）
- 统计信息查询可能需要相应的权限
- 建议在低峰时期运行资源密集型的分析查询
- 可以根据实际环境调整时间间隔和阈值
*/
