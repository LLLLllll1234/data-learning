# Trino 优化器 & 代价模型深度剖析

## 📚 目录
1. [优化器架构概览](#1-优化器架构概览)
2. [代价模型原理](#2-代价模型原理)
3. [统计信息系统](#3-统计信息系统)
4. [关键优化规则](#4-关键优化规则)
5. [Join优化策略](#5-join优化策略)
6. [动态过滤机制](#6-动态过滤机制)
7. [实践案例分析](#7-实践案例分析)
8. [性能调优指南](#8-性能调优指南)

---

## 1. 优化器架构概览

### 1.1 优化器组件结构

```
Query → Parse → Analyze → Plan → Optimize → Execute
                    ↓         ↓        ↓
                 Metadata   Rules   CBO
                 Catalog   Engine  Stats
```

### 1.2 优化阶段划分

```java
// 伪代码：Trino优化流程
public class SqlQueryExecution {
    
    public Plan optimizeQuery(Statement statement) {
        // 1. 语法分析和语义检查
        AnalyzedStatement analyzed = analyzer.analyze(statement);
        
        // 2. 逻辑计划生成
        LogicalPlan logicalPlan = logicalPlanner.plan(analyzed);
        
        // 3. 规则优化（Rule-Based Optimization）
        LogicalPlan optimizedLogical = ruleOptimizer.optimize(logicalPlan);
        
        // 4. 代价优化（Cost-Based Optimization）
        LogicalPlan costOptimized = costOptimizer.optimize(optimizedLogical);
        
        // 5. 物理计划生成
        PhysicalPlan physicalPlan = physicalPlanner.plan(costOptimized);
        
        return physicalPlan;
    }
}
```

### 1.3 优化器核心接口

```java
// 优化规则接口
public interface Rule<T extends PlanNode> {
    Optional<PlanNode> apply(T node, Captures captures, Context context);
    Pattern<T> getPattern();
}

// 代价评估接口  
public interface CostCalculator {
    PlanCostEstimate calculateCost(PlanNode node, StatsProvider statsProvider);
}

// 统计提供接口
public interface StatsProvider {
    PlanNodeStatsEstimate getStats(PlanNode node);
}
```

---

## 2. 代价模型原理

### 2.1 代价构成要素

Trino的代价模型主要包含以下维度：

```java
public class PlanCostEstimate {
    private final double cpuCost;      // CPU计算代价
    private final double memoryCost;   // 内存使用代价  
    private final double networkCost;  // 网络传输代价
    private final double ioRate;       // IO速率代价
    
    // 代价计算公式
    public double getTotalCost() {
        return cpuCost + memoryCost + networkCost + ioRate;
    }
}
```

### 2.2 基础代价计算

#### 表扫描代价
```java
public class TableScanCostCalculator {
    
    public PlanCostEstimate calculateScanCost(TableScanNode scan, TableStatistics stats) {
        // 行数估计
        double rows = stats.getRowCount().getValue();
        
        // 列数和平均行大小
        double avgRowSize = calculateAverageRowSize(scan.getOutputSymbols(), stats);
        
        // IO代价 = 行数 × 平均行大小 × IO代价系数
        double ioCost = rows * avgRowSize * IO_COST_COEFFICIENT;
        
        // CPU代价 = 行数 × CPU处理系数  
        double cpuCost = rows * CPU_COST_COEFFICIENT;
        
        return PlanCostEstimate.builder()
            .setCpuCost(cpuCost)
            .setMaxMemory(0) // 扫描不占用显著内存
            .setNetworkCost(0) // 本地扫描无网络开销
            .setIoRate(ioCost)
            .build();
    }
}
```

#### Join代价估算
```java
public class JoinCostCalculator {
    
    public PlanCostEstimate calculateHashJoinCost(
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            List<Symbol> joinKeys) {
        
        double leftRows = leftStats.getOutputRowCount();
        double rightRows = rightStats.getOutputRowCount();
        
        // 构建哈希表代价 (右表)
        double buildCost = rightRows * HASH_BUILD_COST_PER_ROW;
        
        // 探测代价 (左表) 
        double probeCost = leftRows * HASH_PROBE_COST_PER_ROW;
        
        // 输出行数估算 (基于连接选择性)
        double selectivity = estimateJoinSelectivity(leftStats, rightStats, joinKeys);
        double outputRows = leftRows * rightRows * selectivity;
        
        // 内存需求 = 右表大小 (哈希表)
        double rightDataSize = rightRows * calculateAverageRowSize(rightStats);
        double memoryRequirement = rightDataSize * HASH_TABLE_OVERHEAD_FACTOR;
        
        // 网络代价 = 数据重分区成本 (如果需要)
        double networkCost = calculateRepartitionCost(leftStats, rightStats);
        
        return PlanCostEstimate.builder()
            .setCpuCost(buildCost + probeCost)
            .setMaxMemory(memoryRequirement)
            .setNetworkCost(networkCost)
            .build();
    }
    
    // Join选择性估算
    private double estimateJoinSelectivity(
            PlanNodeStatsEstimate left, 
            PlanNodeStatsEstimate right,
            List<Symbol> joinKeys) {
        
        if (joinKeys.isEmpty()) {
            return 1.0; // 笛卡尔积
        }
        
        double selectivity = 1.0;
        for (Symbol key : joinKeys) {
            // 基于NDV估算单个键的选择性
            double leftNdv = left.getSymbolStatistics(key).getDistinctValuesCount();
            double rightNdv = right.getSymbolStatistics(key).getDistinctValuesCount();
            
            // 选择性 = 1 / max(leftNDV, rightNDV)
            double keySelectivity = 1.0 / Math.max(leftNdv, rightNdv);
            selectivity *= keySelectivity;
        }
        
        return selectivity;
    }
}
```

### 2.3 代价模型配置参数

```properties
# Trino代价模型核心参数
optimizer.cpu-cost-weight=75.0
optimizer.memory-cost-weight=10.0  
optimizer.network-cost-weight=15.0
optimizer.io-cost-weight=1.0

# Join策略阈值
optimizer.broadcast-join-threshold=10MB
optimizer.max-reordered-joins=9
optimizer.max-rewritten-joins=8

# 统计相关
optimizer.default-filter-factor=0.9
optimizer.unknown-correlation-coefficient=0.5
```

---

## 3. 统计信息系统

### 3.1 统计信息类型

```java
public class TableStatistics {
    private final Estimate rowCount;                    // 总行数
    private final Map<String, ColumnStatistics> columnStatistics; // 列统计
    
    public static class ColumnStatistics {
        private final Estimate distinctValuesCount;     // 不同值数量 (NDV)
        private final Estimate nullsCount;             // 空值数量
        private final Optional<Object> min;            // 最小值
        private final Optional<Object> max;            // 最大值
        private final Estimate averageRowSize;         // 平均行大小
        private final Optional<Histogram> histogram;   // 直方图 (可选)
    }
}
```

### 3.2 统计信息收集

#### ANALYZE语句
```sql
-- 收集表统计
ANALYZE TABLE orders;

-- 收集特定列统计
ANALYZE TABLE orders (orderkey, totalprice, orderdate);

-- 指定采样率
ANALYZE TABLE orders WITH (sample_percentage = 5.0);
```

#### 统计信息查询
```sql
-- 查看表统计
SHOW STATS FOR orders;

-- 结果示例
┌─────────────────┬────────────────┬──────────────┬─────────┬─────────┬──────────────┐
│   column_name   │  data_size     │ distinct_cnt │  nulls  │   min   │     max      │
├─────────────────┼────────────────┼──────────────┼─────────┼─────────┼──────────────┤
│ orderkey        │ 60000000       │ 15000000     │ 0       │ 1       │ 60000000     │
│ custkey         │ 60000000       │ 9999977      │ 0       │ 1       │ 15000000     │
│ orderdate       │ 240000000      │ 2406         │ 0       │ 1992... │ 1998...      │
│ totalprice      │ 120000000      │ 14996357     │ 0       │ 811.73  │ 591036.15    │
└─────────────────┴────────────────┴──────────────┴─────────┴─────────┴──────────────┘
```

### 3.3 统计信息在优化中的应用

```java
public class FilterStatsCalculator {
    
    // 基于统计计算过滤后的行数
    public PlanNodeStatsEstimate calculateFilterStats(
            PlanNodeStatsEstimate inputStats,
            Expression predicate) {
        
        double inputRows = inputStats.getOutputRowCount();
        double selectivity = estimateSelectivity(predicate, inputStats);
        double outputRows = inputRows * selectivity;
        
        return PlanNodeStatsEstimate.builder()
            .setOutputRowCount(outputRows)
            .addSymbolStatistics(calculateFilteredSymbolStats(inputStats, predicate))
            .build();
    }
    
    private double estimateSelectivity(Expression predicate, PlanNodeStatsEstimate stats) {
        if (predicate instanceof ComparisonExpression) {
            ComparisonExpression comparison = (ComparisonExpression) predicate;
            
            // 等值过滤: selectivity = 1/NDV
            if (comparison.getOperator() == EQUAL) {
                Symbol symbol = extractSymbol(comparison.getLeft());
                double ndv = stats.getSymbolStatistics(symbol).getDistinctValuesCount();
                return 1.0 / Math.max(ndv, 1.0);
            }
            
            // 范围过滤: 基于min/max计算
            if (comparison.getOperator() == LESS_THAN || 
                comparison.getOperator() == GREATER_THAN) {
                return estimateRangeSelectivity(comparison, stats);
            }
        }
        
        // 默认选择性
        return DEFAULT_FILTER_SELECTIVITY;
    }
}
```

---

## 4. 关键优化规则

### 4.1 规则优化框架

```java
public class RuleBasedOptimizer {
    
    private final List<Rule<?>> rules = ImmutableList.of(
        // 逻辑简化规则
        new RemoveRedundantIdentityProjections(),
        new PruneUnreferencedOutputs(),
        new MergeFilters(),
        new SimplifyExpressions(),
        
        // 下推规则  
        new PredicatePushDown(),
        new ProjectionPushDown(),
        new LimitPushDown(),
        new TopNPushDown(),
        
        // 连接优化规则
        new ReorderJoins(),
        new ReplaceRedundantJoinWithSource(),
        new TransformCorrelatedScalarSubquery(),
        
        // 聚合优化规则
        new PushPartialAggregationThroughJoin(),
        new PruneCountAggregationOverScalar(),
        new OptimizeDuplicateInsensitiveJoins()
    );
    
    public PlanNode optimize(PlanNode plan, Session session, PlanNodeIdAllocator idAllocator) {
        IterativeOptimizer iterativeOptimizer = new IterativeOptimizer(
            rules,
            session,
            idAllocator,
            new MemoizingStatsProvider(statsCalculator, session),
            new CostCalculator(statsCalculator));
            
        return iterativeOptimizer.optimize(plan);
    }
}
```

### 4.2 谓词下推规则详解

```java
public class PredicatePushDown implements Rule<FilterNode> {
    
    @Override
    public Optional<PlanNode> apply(FilterNode filter, Captures captures, Context context) {
        PlanNode source = filter.getSource();
        Expression predicate = filter.getPredicate();
        
        // 尝试将谓词下推到不同类型的算子
        if (source instanceof JoinNode) {
            return pushDownToJoin(filter, (JoinNode) source, predicate, context);
        } else if (source instanceof TableScanNode) {
            return pushDownToTableScan(filter, (TableScanNode) source, predicate, context);
        } else if (source instanceof ProjectNode) {
            return pushDownThroughProject(filter, (ProjectNode) source, predicate, context);
        }
        
        return Optional.empty();
    }
    
    private Optional<PlanNode> pushDownToJoin(
            FilterNode filter, 
            JoinNode join, 
            Expression predicate,
            Context context) {
        
        List<Expression> leftPredicates = new ArrayList<>();
        List<Expression> rightPredicates = new ArrayList<>();
        List<Expression> joinPredicates = new ArrayList<>();
        
        // 分解复合谓词
        for (Expression conjunct : extractConjuncts(predicate)) {
            Set<Symbol> predicateSymbols = extractUnique(conjunct);
            
            if (join.getLeft().getOutputSymbols().containsAll(predicateSymbols)) {
                // 谓词只依赖左表，可以下推到左表
                leftPredicates.add(conjunct);
            } else if (join.getRight().getOutputSymbols().containsAll(predicateSymbols)) {
                // 谓词只依赖右表，可以下推到右表  
                rightPredicates.add(conjunct);
            } else {
                // 跨表谓词，保留在Join层面
                joinPredicates.add(conjunct);
            }
        }
        
        // 构建新的计划
        PlanNode left = join.getLeft();
        PlanNode right = join.getRight();
        
        if (!leftPredicates.isEmpty()) {
            left = new FilterNode(context.getIdAllocator().getNextId(), 
                                left, 
                                combineConjuncts(leftPredicates));
        }
        
        if (!rightPredicates.isEmpty()) {
            right = new FilterNode(context.getIdAllocator().getNextId(),
                                 right,
                                 combineConjuncts(rightPredicates));
        }
        
        JoinNode newJoin = new JoinNode(
            join.getId(),
            join.getType(),
            left,
            right,
            join.getCriteria(),
            join.getLeftHashSymbol(),
            join.getRightHashSymbol(),
            join.getFilter());
            
        if (joinPredicates.isEmpty()) {
            return Optional.of(newJoin);
        } else {
            return Optional.of(new FilterNode(
                filter.getId(),
                newJoin,
                combineConjuncts(joinPredicates)));
        }
    }
}
```

### 4.3 投影下推规则

```java
public class ProjectionPushDown implements Rule<ProjectNode> {
    
    @Override 
    public Optional<PlanNode> apply(ProjectNode project, Captures captures, Context context) {
        
        // 只保留需要的列，移除冗余投影
        Set<Symbol> requiredInputs = extractInputSymbols(project.getAssignments());
        
        if (project.getSource() instanceof TableScanNode) {
            return pushProjectionToTableScan(project, requiredInputs, context);
        }
        
        return Optional.empty();
    }
    
    private Optional<PlanNode> pushProjectionToTableScan(
            ProjectNode project,
            Set<Symbol> requiredSymbols,
            Context context) {
        
        TableScanNode tableScan = (TableScanNode) project.getSource();
        
        // 计算需要从表中读取的列
        Set<ColumnHandle> requiredColumns = requiredSymbols.stream()
            .map(symbol -> tableScan.getAssignments().get(symbol))
            .collect(toImmutableSet());
            
        if (requiredColumns.equals(tableScan.getAssignments().values())) {
            // 已经是最少的列集合
            return Optional.empty();
        }
        
        // 创建裁剪后的表扫描
        Map<Symbol, ColumnHandle> prunedAssignments = tableScan.getAssignments().entrySet()
            .stream()
            .filter(entry -> requiredColumns.contains(entry.getValue()))
            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
            
        TableScanNode prunedScan = new TableScanNode(
            tableScan.getId(),
            tableScan.getTable(),
            ImmutableList.copyOf(prunedAssignments.keySet()),
            prunedAssignments,
            tableScan.getPredicate(),
            tableScan.getEnforcedConstraint(),
            tableScan.getStatistics());
        
        return Optional.of(new ProjectNode(project.getId(), prunedScan, project.getAssignments()));
    }
}
```

---

## 5. Join优化策略

### 5.1 Join重排序算法

```java
public class JoinReorderingOptimizer {
    
    public PlanNode reorderJoins(JoinNode joinTree, Context context) {
        // 1. 提取Join图
        JoinGraph joinGraph = buildJoinGraph(joinTree);
        
        // 2. 基于CBO搜索最优连接顺序
        JoinEnumeration enumeration = new JoinEnumeration(
            joinGraph,
            context.getStatsProvider(),
            context.getCostCalculator());
            
        return enumeration.chooseJoinOrder();
    }
    
    private static class JoinEnumeration {
        
        public PlanNode chooseJoinOrder() {
            // 动态规划搜索最优Join顺序
            Map<Set<Integer>, JoinPlan> bestPlans = new HashMap<>();
            
            // 初始化：单表计划
            for (int i = 0; i < relations.size(); i++) {
                Set<Integer> singleton = ImmutableSet.of(i);
                PlanNode relation = relations.get(i);
                double cost = costCalculator.calculateCost(relation).getTotalCost();
                bestPlans.put(singleton, new JoinPlan(relation, cost));
            }
            
            // 动态规划：枚举所有可能的子集组合
            for (int size = 2; size <= relations.size(); size++) {
                for (Set<Integer> subset : Sets.combinations(IntStream.range(0, relations.size())
                        .boxed().collect(toSet()), size)) {
                    
                    JoinPlan bestPlan = findBestJoinPlan(subset, bestPlans);
                    bestPlans.put(subset, bestPlan);
                }
            }
            
            Set<Integer> allRelations = IntStream.range(0, relations.size())
                .boxed().collect(toSet());
            return bestPlans.get(allRelations).getPlan();
        }
        
        private JoinPlan findBestJoinPlan(Set<Integer> relations, 
                                        Map<Set<Integer>, JoinPlan> bestPlans) {
            JoinPlan bestPlan = null;
            double bestCost = Double.POSITIVE_INFINITY;
            
            // 尝试所有可能的二元分割
            for (Set<Integer> left : properSubsets(relations)) {
                Set<Integer> right = Sets.difference(relations, left);
                
                if (right.isEmpty() || !canJoin(left, right)) {
                    continue;
                }
                
                JoinPlan leftPlan = bestPlans.get(left);
                JoinPlan rightPlan = bestPlans.get(right);
                
                // 尝试不同的join策略
                for (JoinDistributionType distributionType : 
                     Arrays.asList(PARTITIONED, BROADCAST)) {
                    
                    PlanNode joinPlan = createJoin(
                        leftPlan.getPlan(), 
                        rightPlan.getPlan(),
                        getJoinCriteria(left, right),
                        distributionType);
                        
                    double cost = costCalculator.calculateCost(joinPlan).getTotalCost();
                    
                    if (cost < bestCost) {
                        bestCost = cost;
                        bestPlan = new JoinPlan(joinPlan, cost);
                    }
                }
            }
            
            return bestPlan;
        }
    }
}
```

### 5.2 Join策略选择

```java
public class JoinDistributionTypeSelector {
    
    public JoinDistributionType selectDistributionType(
            PlanNode left, 
            PlanNode right, 
            List<JoinNode.EquiJoinClause> criteria,
            Session session) {
        
        PlanNodeStatsEstimate leftStats = statsProvider.getStats(left);
        PlanNodeStatsEstimate rightStats = statsProvider.getStats(right);
        
        // 广播Join阈值检查
        double broadcastThreshold = session.getSystemProperty(
            BROADCAST_JOIN_THRESHOLD, DataSize.class).toBytes();
            
        double rightDataSize = calculateDataSize(rightStats);
        
        if (rightDataSize <= broadcastThreshold) {
            // 右表足够小，使用广播Join
            return BROADCAST;
        }
        
        // 计算两种策略的代价
        double broadcastCost = calculateBroadcastJoinCost(leftStats, rightStats);
        double partitionedCost = calculatePartitionedJoinCost(leftStats, rightStats, criteria);
        
        return broadcastCost < partitionedCost ? BROADCAST : PARTITIONED;
    }
    
    private double calculateBroadcastJoinCost(
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats) {
        
        double leftRows = leftStats.getOutputRowCount();  
        double rightRows = rightStats.getOutputRowCount();
        double rightDataSize = calculateDataSize(rightStats);
        
        // 网络代价：右表广播到所有节点
        int workerCount = nodeManager.getRequiredWorkerNodes().size();
        double networkCost = rightDataSize * workerCount * NETWORK_COST_MULTIPLIER;
        
        // 构建哈希表代价
        double buildCost = rightRows * HASH_BUILD_COST_PER_ROW * workerCount;
        
        // 探测代价
        double probeCost = leftRows * HASH_PROBE_COST_PER_ROW;
        
        return networkCost + buildCost + probeCost;
    }
    
    private double calculatePartitionedJoinCost(
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            List<JoinNode.EquiJoinClause> criteria) {
        
        double leftRows = leftStats.getOutputRowCount();
        double rightRows = rightStats.getOutputRowCount();
        double leftDataSize = calculateDataSize(leftStats);
        double rightDataSize = calculateDataSize(rightStats);
        
        // 重分区网络代价：假设数据均匀分布
        double repartitionCost = (leftDataSize + rightDataSize) * NETWORK_COST_MULTIPLIER;
        
        // 本地Join代价
        double avgLeftRowsPerNode = leftRows / getWorkerCount();
        double avgRightRowsPerNode = rightRows / getWorkerCount();
        
        double buildCost = avgRightRowsPerNode * HASH_BUILD_COST_PER_ROW;
        double probeCost = avgLeftRowsPerNode * HASH_PROBE_COST_PER_ROW;
        
        return repartitionCost + buildCost + probeCost;
    }
}
```

---

## 6. 动态过滤机制

### 6.1 动态过滤原理

动态过滤是Trino的一项关键优化技术，在Join执行时生成过滤条件，动态地减少另一侧表的扫描量。

```java
public class DynamicFilterPlanner {
    
    // 为Join计划动态过滤  
    public PlanNode planDynamicFilters(JoinNode joinNode, Context context) {
        
        if (!isDynamicFilteringEnabled(context.getSession())) {
            return joinNode;
        }
        
        List<DynamicFilterId> dynamicFilters = new ArrayList<>();
        
        // 为每个等值连接条件创建动态过滤
        for (JoinNode.EquiJoinClause clause : joinNode.getCriteria()) {
            Symbol probeSymbol = clause.getLeft();  // 左表探测列
            Symbol buildSymbol = clause.getRight(); // 右表构建列
            
            // 创建动态过滤ID
            DynamicFilterId filterId = new DynamicFilterId("DF_" + context.getIdAllocator().getNextId());
            dynamicFilters.add(filterId);
            
            // 在右表构建端创建动态过滤源
            PlanNode rightSource = addDynamicFilterSource(
                joinNode.getRight(), 
                buildSymbol, 
                filterId, 
                context);
            
            // 在左表探测端应用动态过滤
            PlanNode leftSource = addDynamicFilterConsumer(
                joinNode.getLeft(),
                probeSymbol,
                filterId,
                context);
                
            joinNode = joinNode.withSources(leftSource, rightSource);
        }
        
        return joinNode;
    }
    
    private PlanNode addDynamicFilterSource(
            PlanNode source, 
            Symbol buildSymbol,
            DynamicFilterId filterId,
            Context context) {
        
        // 在构建端收集唯一值或创建BloomFilter
        return new DynamicFilterSourceNode(
            context.getIdAllocator().getNextId(),
            source,
            buildSymbol,
            filterId,
            DynamicFilterSourceNode.FilterType.BLOOM_FILTER); // 或 MIN_MAX, IN_LIST
    }
    
    private PlanNode addDynamicFilterConsumer(
            PlanNode source,
            Symbol probeSymbol, 
            DynamicFilterId filterId,
            Context context) {
        
        // 递归查找TableScan节点应用动态过滤
        return source.accept(new DynamicFilterInjector(probeSymbol, filterId), context);
    }
}

// 动态过滤注入器
class DynamicFilterInjector extends SimplePlanRewriter<Context> {
    private final Symbol targetSymbol;
    private final DynamicFilterId filterId;
    
    @Override
    public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context) {
        
        // 检查是否包含目标列
        if (!node.getOutputSymbols().contains(targetSymbol)) {
            return node;
        }
        
        // 创建动态过滤谓词
        Expression dynamicFilter = new DynamicFilterExpression(filterId, targetSymbol);
        
        // 将动态过滤添加到表扫描的谓词中
        Expression combinedPredicate = combineConjuncts(
            node.getPredicate().orElse(BooleanLiteral.TRUE_LITERAL),
            dynamicFilter);
        
        return new TableScanNode(
            node.getId(),
            node.getTable(),
            node.getOutputSymbols(),
            node.getAssignments(),
            Optional.of(combinedPredicate),
            node.getEnforcedConstraint(),
            node.getStatistics());
    }
}
```

### 6.2 动态过滤类型

```java
public enum DynamicFilterType {
    
    // 范围过滤：min <= value <= max
    MIN_MAX {
        @Override
        public Predicate<Object> createFilter(Set<Object> values) {
            Object min = Collections.min((Collection) values);
            Object max = Collections.max((Collection) values);
            return value -> compare(min, value) <= 0 && compare(value, max) <= 0;
        }
    },
    
    // 集合过滤：value IN (set)  
    IN_LIST {
        @Override
        public Predicate<Object> createFilter(Set<Object> values) {
            return values::contains;
        }
    },
    
    // 布隆过滤：可能包含的值
    BLOOM_FILTER {
        @Override  
        public Predicate<Object> createFilter(Set<Object> values) {
            BloomFilter<Object> filter = BloomFilter.create(
                Funnels.unencodedChars(), 
                values.size(),
                0.01); // 1% false positive rate
                
            values.forEach(filter::put);
            return filter::mightContain;
        }
    };
    
    public abstract Predicate<Object> createFilter(Set<Object> values);
}
```

### 6.3 动态过滤执行时序

```
1. Join开始执行
2. 右表(build side)开始扫描和构建哈希表
3. 同时收集join key的统计信息(min/max/distinct values)
4. 当收集到足够信息时，创建动态过滤器
5. 将过滤器推送到左表(probe side)的TableScan
6. 左表应用动态过滤，跳过大量不匹配的数据
7. 继续正常的Join处理流程
```

---

## 7. 实践案例分析

### 7.1 复杂查询优化案例

假设有以下查询：

```sql
SELECT 
    o.orderkey,
    o.orderdate,
    c.name as customer_name,
    SUM(l.extendedprice * (1 - l.discount)) as revenue
FROM orders o
JOIN customer c ON o.custkey = c.custkey  
JOIN lineitem l ON o.orderkey = l.orderkey
WHERE o.orderdate >= DATE '1995-01-01'
  AND o.orderdate < DATE '1996-01-01'  
  AND c.mktsegment = 'BUILDING'
GROUP BY o.orderkey, o.orderdate, c.name
ORDER BY revenue DESC
LIMIT 10;
```

#### 优化前的执行计划
```
- Limit[10]
  - Sort[revenue DESC]  
    - Aggregate[GROUP BY orderkey,orderdate,name | SUM(revenue)]
      - Join[o.orderkey = l.orderkey]
        - Join[o.custkey = c.custkey]
          - TableScan[orders] - Filters: orderdate >= '1995-01-01' AND orderdate < '1996-01-01'
          - TableScan[customer] - Filters: mktsegment = 'BUILDING'  
        - TableScan[lineitem]
```

#### 优化后的执行计划
```
- Limit[10]
  - Sort[revenue DESC] - 应用TopN优化
    - Aggregate[GROUP BY orderkey,orderdate,name | SUM(revenue)]  
      - Join[o.orderkey = l.orderkey] - 使用动态过滤
        - Join[o.custkey = c.custkey] - 广播小表customer
          - TableScan[orders] - 应用分区裁剪和列裁剪
          - TableScan[customer] - 谓词下推: mktsegment = 'BUILDING'
        - TableScan[lineitem] - 动态过滤: orderkey IN (filtered_orders)
```

#### 关键优化点分析

1. **谓词下推**: `mktsegment = 'BUILDING'` 下推到customer表扫描
2. **分区裁剪**: 基于orderdate过滤orders表的分区  
3. **列裁剪**: 只读取查询中需要的列
4. **Join策略选择**: customer表较小，选择broadcast join
5. **动态过滤**: 从filtered orders生成动态过滤器，应用到lineitem扫描
6. **TopN优化**: LIMIT + ORDER BY 合并为TopN算子

### 7.2 使用EXPLAIN分析优化效果

```sql
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) 
SELECT ...;
```

重点关注以下指标：
- **CPU成本和内存成本**
- **网络传输量** (Exchange节点)
- **扫描行数vs输出行数** (过滤效果) 
- **Join策略** (Broadcast vs Partitioned)
- **动态过滤应用** (DynamicFilterSource/Consumer节点)

---

## 8. 性能调优指南

### 8.1 统计信息维护

```sql
-- 定期更新表统计 
ANALYZE TABLE large_table;

-- 针对性更新高基数列
ANALYZE TABLE large_table (high_cardinality_column);

-- 检查统计信息是否及时
SELECT 
    table_name,
    column_name, 
    data_size,
    distinct_values_count,
    nulls_count
FROM information_schema.column_statistics 
WHERE schema_name = 'your_schema'
ORDER BY table_name, column_name;
```

### 8.2 会话级优化参数

```sql
-- 启用动态过滤
SET SESSION enable_dynamic_filtering = true;

-- 调整Join策略阈值
SET SESSION join_distribution_type = 'BROADCAST'; 
SET SESSION join_reordering_strategy = 'AUTOMATIC';

-- 内存和并发控制
SET SESSION query_max_memory = '10GB';
SET SESSION query_max_memory_per_node = '2GB';

-- 启用代价优化器
SET SESSION optimizer_use_histograms = true;
SET SESSION use_legacy_scheduler = false;
```

### 8.3 表设计最佳实践

```sql
-- 合理的分区策略
CREATE TABLE events (
    event_id BIGINT,
    event_time TIMESTAMP,
    user_id BIGINT,
    event_type VARCHAR(50)
)
WITH (
    partitioned_by = ARRAY['year', 'month', 'day'],
    format = 'PARQUET'
);

-- 选择合适的bucketing
CREATE TABLE user_profiles (
    user_id BIGINT,
    profile_data JSON  
)
WITH (
    bucketed_by = ARRAY['user_id'],
    bucket_count = 64,
    format = 'PARQUET'
);
```

### 8.4 查询优化Checklist

- [ ] **统计信息是否最新**：定期ANALYZE关键表
- [ ] **分区策略是否合理**：基于查询pattern设计分区
- [ ] **Join顺序是否优化**：小表在右，高选择性条件优先
- [ ] **是否启用动态过滤**：对于大表Join特别有效
- [ ] **列裁剪和谓词下推**：只读必要的列和行
- [ ] **数据倾斜处理**：检查Join key的分布均匀性
- [ ] **资源配置调优**：合理设置内存和并发参数

---

## 📋 总结

Trino的优化器是一个复杂而强大的系统，通过：

1. **多阶段优化流水线**：规则优化 → 代价优化 → 物理规划
2. **精确的代价模型**：基于统计信息的CPU/内存/网络/IO代价估算  
3. **丰富的优化规则**：谓词下推、投影下推、Join重排序等
4. **动态运行时优化**：动态过滤、自适应查询执行
5. **可配置的调优参数**：支持不同workload的优化策略

掌握这些原理和实践技巧，能够帮助你：
- **理解查询执行计划**，识别性能瓶颈
- **调优复杂查询**，获得数量级的性能提升  
- **设计高效的数据模型**，配合优化器发挥最佳效果
- **监控和诊断**生产环境的查询性能问题

希望这份深度资料能够帮助你全面掌握Trino优化器的核心机制！🚀
