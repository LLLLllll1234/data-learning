# Trino 执行器源码级深度剖析

## 📚 目录

1. [执行引擎架构总览](#1-执行引擎架构总览)
2. [Page数据结构深度解析](#2-page数据结构深度解析)
3. [算子(Operator)实现机理](#3-算子operator实现机理)
4. [Exchange系统源码分析](#4-exchange系统源码分析)
5. [内存管理与背压机制](#5-内存管理与背压机制)
6. [Driver与Pipeline执行](#6-driver与pipeline执行)
7. [调度器与任务管理](#7-调度器与任务管理)
8. [性能关键路径分析](#8-性能关键路径分析)

---

## 1. 执行引擎架构总览

### 1.1 核心组件关系图

```chart
Query Execution Architecture:

┌─────────────────────────────────────────────────────────────┐
│                    Coordinator                               │
├─────────────────────────────────────────────────────────────┤
│  QueryExecution                                             │
│  ├── StageExecution (ROOT)                                  │
│  ├── StageExecution (INTERMEDIATE)                          │
│  └── StageExecution (SOURCE)                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Worker Nodes                             │
├─────────────────────────────────────────────────────────────┤
│  TaskExecution                                              │
│  ├── Driver 1 ──► Pipeline ──► [Op1] → [Op2] → [Op3]      │
│  ├── Driver 2 ──► Pipeline ──► [Op1] → [Op2] → [Op3]      │
│  └── Driver N ──► Pipeline ──► [Op1] → [Op2] → [Op3]      │
│                                   │                         │
│                                   ▼                         │
│  LocalExchange / RemoteExchange                             │
│  ├── Producer: PageBuffer                                  │
│  └── Consumer: PageBuffer                                  │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 执行模型核心抽象

```java
// 核心接口定义 (简化版源码)
public interface Operator extends AutoCloseable {
    // 获取下一批数据页
    Page getOutput();
    
    // 是否需要更多输入数据
    boolean needsInput();
    
    // 添加输入数据页
    void addInput(Page page);
    
    // 标记输入结束
    void finish();
    
    // 是否已完成处理
    boolean isFinished();
    
    // 当前状态 (需要输入/有输出/阻塞/完成)
    ListenableFuture<?> isBlocked();
}
```

### 1.3 数据流动模型

```java
// 执行器的Pull模型实现
public class Driver {
    private final List<Operator> operators;
    private final OperatorFactory outputFactory;
    
    // 核心执行循环
    public ListenableFuture<?> process() {
        // 从最后一个算子开始拉取数据
        Operator outputOperator = operators.get(operators.size() - 1);
        
        while (!outputOperator.isFinished()) {
            // 检查是否被阻塞
            if (outputOperator.isBlocked().isDone()) {
                // 拉取输出页
                Page outputPage = outputOperator.getOutput();
                if (outputPage != null) {
                    // 将页发送给下游
                    sendToDownstream(outputPage);
                }
            } else {
                // 等待解除阻塞
                return outputOperator.isBlocked();
            }
        }
        return Futures.immediateFuture(null);
    }
}
```

---

## 2. Page数据结构深度解析

### 2.1 Page的内存布局

```java
// Page是Trino执行器的核心数据结构
public class Page {
    private final Block[] blocks;           // 列存储块数组
    private final int positionCount;        // 行数
    private final long sizeInBytes;         // 总字节大小
    private final long retainedSizeInBytes; // 保留内存大小
    
    public Page(Block... blocks) {
        this.blocks = requireNonNull(blocks, "blocks is null");
        this.positionCount = blocks.length == 0 ? 0 : blocks[0].getPositionCount();
        
        // 计算总大小
        long sizeInBytes = 0;
        long retainedSizeInBytes = 0;
        for (Block block : blocks) {
            sizeInBytes += block.getSizeInBytes();
            retainedSizeInBytes += block.getRetainedSizeInBytes();
        }
        this.sizeInBytes = sizeInBytes;
        this.retainedSizeInBytes = retainedSizeInBytes;
    }
    
    // 关键方法：获取指定列的块
    public Block getBlock(int channel) {
        return blocks[channel];
    }
    
    // 创建子页面 (用于分区和过滤)
    public Page getRegion(int positionOffset, int length) {
        Block[] regionBlocks = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            regionBlocks[i] = blocks[i].getRegion(positionOffset, length);
        }
        return new Page(regionBlocks);
    }
}
```

### 2.2 Block数据结构实现

```java
// Block是列存储的基本单元
public abstract class Block {
    // 获取位置i的值是否为null
    public abstract boolean isNull(int position);
    
    // 获取位置i的字节表示
    public abstract Slice getSlice(int position, int offset, int length);
    
    // 获取位置i的长整型值
    public abstract long getLong(int position, int offset);
    
    // 获取位置i的双精度值  
    public abstract double getDouble(int position, int offset);
    
    // 创建区域视图 (零拷贝切片)
    public abstract Block getRegion(int positionOffset, int length);
    
    // 拷贝位置到BlockBuilder (用于输出构建)
    public abstract void writePositionTo(int position, BlockBuilder blockBuilder);
}

// 具体实现：变长字符串Block
public class VariableWidthBlock extends AbstractBlock {
    private final Slice slice;              // 实际数据存储
    private final int[] offsets;            // 每行的偏移量
    private final boolean[] nulls;          // null标记数组
    
    @Override
    public Slice getSlice(int position, int offset, int length) {
        checkReadablePosition(position);
        int positionOffset = offsets[position];
        int positionLength = offsets[position + 1] - positionOffset;
        return slice.slice(positionOffset + offset, length);
    }
    
    // 高效的区域切片实现 (零拷贝)
    @Override  
    public Block getRegion(int positionOffset, int length) {
        int startOffset = offsets[positionOffset];
        int endOffset = offsets[positionOffset + length];
        
        return new VariableWidthBlock(
            length,
            slice.slice(startOffset, endOffset - startOffset),
            Arrays.copyOfRange(offsets, positionOffset, positionOffset + length + 1),
            nulls == null ? null : Arrays.copyOfRange(nulls, positionOffset, positionOffset + length)
        );
    }
}
```

### 2.3 列式数据的向量化处理

```java
// 向量化表达式求值示例
public class VectorizedExpressionEvaluator {
    
    // 向量化的加法操作
    public Block evaluateAdd(Block leftBlock, Block rightBlock) {
        int positionCount = leftBlock.getPositionCount();
        BlockBuilder resultBuilder = BIGINT.createBlockBuilder(null, positionCount);
        
        // 批量处理，利用CPU缓存
        for (int i = 0; i < positionCount; i++) {
            if (leftBlock.isNull(i) || rightBlock.isNull(i)) {
                resultBuilder.appendNull();
            } else {
                long left = BIGINT.getLong(leftBlock, i);
                long right = BIGINT.getLong(rightBlock, i);
                BIGINT.writeLong(resultBuilder, left + right);
            }
        }
        return resultBuilder.build();
    }
    
    // SIMD优化的批量操作 (概念示例)
    public Block evaluateAddSIMD(Block leftBlock, Block rightBlock) {
        // 实际实现会使用JIT编译器的向量化优化
        // 或者调用native代码进行SIMD加速
        return evaluateWithVectorIntrinsics(leftBlock, rightBlock, Long::sum);
    }
}
```

---

## 3. 算子(Operator)实现机理

### 3.1 TableScan算子实现

```java
// 表扫描算子的核心实现
public class TableScanOperator implements Operator {
    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final PageSourceProvider pageSourceProvider;
    private final TableHandle table;
    
    private PageSource pageSource;
    private boolean finished;
    
    @Override
    public Page getOutput() {
        if (finished) {
            return null;
        }
        
        if (pageSource == null) {
            // 延迟初始化页面源
            pageSource = pageSourceProvider.createPageSource(table);
        }
        
        // 从连接器获取下一页数据
        Page page = pageSource.getNextPage();
        if (page == null) {
            finished = true;
            return null;
        }
        
        // 更新算子统计
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        
        return page;
    }
    
    @Override
    public ListenableFuture<?> isBlocked() {
        if (pageSource != null) {
            return pageSource.isBlocked();
        }
        return Futures.immediateFuture(null);
    }
}
```

### 3.2 HashJoin算子实现深度解析

```java
// Hash Join算子的复杂实现
public class HashJoinOperator implements Operator {
    private enum State {
        CONSUMING_BUILD_INPUT,    // 消费构建端输入
        LOOKUP_SOURCE_BUILT,      // 查找结构已构建
        CONSUMING_PROBE_INPUT,    // 消费探测端输入
        FINISHED                  // 完成
    }
    
    private final OperatorContext operatorContext;
    private final List<Type> buildTypes;
    private final List<Type> probeTypes;
    private final JoinBridge joinBridge;
    private final PageBuilder pageBuilder;
    
    private State state = State.CONSUMING_BUILD_INPUT;
    private LookupSource lookupSource;
    private JoinProbe probe;
    
    @Override
    public Page getOutput() {
        switch (state) {
            case CONSUMING_BUILD_INPUT:
                return null; // 还在构建哈希表阶段
                
            case LOOKUP_SOURCE_BUILT:
                // 哈希表构建完成，开始探测
                lookupSource = joinBridge.getLookupSource();
                probe = new JoinProbe(lookupSource, probeTypes);
                state = State.CONSUMING_PROBE_INPUT;
                return null;
                
            case CONSUMING_PROBE_INPUT:
                return processProbeInput();
                
            case FINISHED:
                return null;
        }
        throw new IllegalStateException("Unknown state: " + state);
    }
    
    private Page processProbeInput() {
        if (!probe.hasOutput()) {
            // 没有输出，需要更多探测输入或已完成
            return null;
        }
        
        pageBuilder.reset();
        
        // 批量处理Join匹配
        while (!pageBuilder.isFull() && probe.hasOutput()) {
            // 获取下一个匹配的位置对
            JoinProbe.ProbePosition probePosition = probe.getOutput();
            
            // 写入探测侧数据
            for (int channel = 0; channel < probeTypes.size(); channel++) {
                Type type = probeTypes.get(channel);
                Block probeBlock = probePosition.getProbeBlock(channel);
                type.appendTo(probeBlock, probePosition.getProbePosition(), 
                             pageBuilder.getBlockBuilder(channel));
            }
            
            // 写入构建侧数据
            int buildPosition = probePosition.getBuildPosition();
            for (int channel = 0; channel < buildTypes.size(); channel++) {
                Type type = buildTypes.get(channel);
                Block buildBlock = lookupSource.getBuildBlock(channel);
                type.appendTo(buildBlock, buildPosition, 
                             pageBuilder.getBlockBuilder(probeTypes.size() + channel));
            }
            
            pageBuilder.declarePosition();
        }
        
        if (pageBuilder.isEmpty()) {
            return null;
        }
        
        return pageBuilder.build();
    }
    
    @Override
    public void addInput(Page page) {
        switch (state) {
            case CONSUMING_BUILD_INPUT:
                // 将页面添加到哈希表构建
                joinBridge.addBuildPage(page);
                break;
                
            case CONSUMING_PROBE_INPUT:
                // 将页面用于探测
                probe.addProbePage(page);
                break;
                
            default:
                throw new IllegalStateException("Cannot add input in state: " + state);
        }
    }
}
```

### 3.3 聚合算子的内存管理

```java
// 聚合算子的高级实现
public class HashAggregationOperator implements Operator {
    private final GroupByHash groupByHash;          // 分组哈希表
    private final List<Aggregator> aggregators;    // 聚合函数列表
    private final AggregationMemoryContext memoryContext;
    
    private boolean inputFinished;
    private Iterator<Page> outputIterator;
    
    @Override
    public Page getOutput() {
        if (outputIterator == null) {
            if (!inputFinished) {
                return null; // 还在消费输入
            }
            
            // 输入完成，开始生成输出
            outputIterator = buildOutputIterator();
        }
        
        if (outputIterator.hasNext()) {
            return outputIterator.next();
        }
        
        return null; // 输出完成
    }
    
    @Override
    public void addInput(Page page) {
        // 更新内存使用
        long previousMemorySize = groupByHash.getEstimatedSize();
        
        // 处理分组
        GroupByIdBlock groupIds = groupByHash.getGroupIds(page);
        
        // 更新聚合状态
        for (int i = 0; i < aggregators.size(); i++) {
            aggregators.get(i).processPage(groupIds, page);
        }
        
        // 检查内存限制
        long currentMemorySize = groupByHash.getEstimatedSize();
        long memoryIncrease = currentMemorySize - previousMemorySize;
        
        if (memoryIncrease > 0) {
            memoryContext.setBytes(currentMemorySize);
            
            // 如果内存使用过高，触发溢写
            if (currentMemorySize > getMaxPartialMemory()) {
                triggerSpill();
            }
        }
    }
    
    // 内存溢写机制
    private void triggerSpill() {
        // 将当前哈希表内容写入磁盘
        SpillableHashAggregationBuilder spillBuilder = 
            new SpillableHashAggregationBuilder(
                groupByHash.getGroupCount(),
                aggregators.stream()
                    .map(Aggregator::createPartialAggregation)
                    .collect(toList()));
                    
        // 创建溢写文件
        spillBuilder.spillToDisk(operatorContext.getSpillContext());
        
        // 重置内存状态
        groupByHash.clear();
        memoryContext.setBytes(0);
    }
}
```

---

## 4. Exchange系统源码分析

### 4.1 Exchange架构设计

```java
// Exchange系统的核心抽象
public interface ExchangeClient {
    // 获取下一批数据
    ListenableFuture<Page> getNextPage();
    
    // 是否完成
    boolean isFinished();
    
    // 关闭连接
    void close();
    
    // 获取缓冲的数据量
    DataSize getBufferedBytes();
}

// 远程Exchange客户端实现
public class HttpExchangeClient implements ExchangeClient {
    private final URI location;                    // 远程位置
    private final HttpClient httpClient;           // HTTP客户端
    private final DataStreamProvider dataProvider; // 数据流提供者
    private final Queue<Page> pageBuffer;          // 页面缓冲区
    private final AtomicLong bufferedBytes;        // 缓冲字节数
    
    @Override
    public ListenableFuture<Page> getNextPage() {
        // 如果缓冲区有数据，直接返回
        if (!pageBuffer.isEmpty()) {
            Page page = pageBuffer.poll();
            bufferedBytes.addAndGet(-page.getSizeInBytes());
            return Futures.immediateFuture(page);
        }
        
        // 异步获取更多数据
        return Futures.transform(
            fetchMoreData(),
            this::processReceivedData,
            MoreExecutors.directExecutor()
        );
    }
    
    private ListenableFuture<SliceInput> fetchMoreData() {
        // 发起HTTP请求获取数据
        HttpUriBuilder uriBuilder = uriBuilderFrom(location);
        URI uri = uriBuilder.build();
        
        Request request = Request.Builder()
            .setUri(uri)
            .setHeader(CONTENT_TYPE, PRESTO_PAGES)
            .build();
            
        return httpClient.executeAsync(request, 
            createPageResponseHandler());
    }
    
    private Page processReceivedData(SliceInput input) {
        if (input == null) {
            return null; // 数据结束
        }
        
        // 反序列化页面数据
        Page page = pagesSerde.deserialize(input);
        bufferedBytes.addAndGet(page.getSizeInBytes());
        
        return page;
    }
}
```

### 4.2 本地Exchange实现

```java
// 本地Exchange用于同一节点内的数据传输
public class LocalExchange {
    private final List<LocalExchangeBuffer> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final AtomicBoolean noMoreProducers;
    
    // 生产者接口
    public interface LocalExchangeProducer {
        void addPage(Page page);
        void finish();
    }
    
    // 消费者接口  
    public interface LocalExchangeConsumer {
        ListenableFuture<Page> getNextPage();
        boolean isFinished();
    }
    
    // 创建生产者
    public LocalExchangeProducer createProducer() {
        return new LocalExchangeProducer() {
            @Override
            public void addPage(Page page) {
                // 根据分区函数决定目标buffer
                int partition = getPartition(page);
                buffers.get(partition).addPage(page);
                
                // 更新内存使用
                memoryManager.updateMemoryUsage(page.getSizeInBytes());
            }
            
            @Override
            public void finish() {
                for (LocalExchangeBuffer buffer : buffers) {
                    buffer.setNoMorePages();
                }
            }
        };
    }
    
    // 创建消费者
    public LocalExchangeConsumer createConsumer(int partition) {
        return new LocalExchangeConsumer() {
            private final LocalExchangeBuffer buffer = buffers.get(partition);
            
            @Override
            public ListenableFuture<Page> getNextPage() {
                return buffer.getNextPage();
            }
            
            @Override
            public boolean isFinished() {
                return buffer.isFinished();
            }
        };
    }
}

// 本地Exchange缓冲区实现
public class LocalExchangeBuffer {
    private final Queue<Page> pages = new ArrayDeque<>();
    private final AtomicLong bufferedBytes = new AtomicLong();
    private final SettableFuture<Void> blocked = SettableFuture.create();
    private volatile boolean noMorePages;
    
    public void addPage(Page page) {
        synchronized (this) {
            pages.add(page);
            bufferedBytes.addAndGet(page.getSizeInBytes());
            
            // 唤醒等待的消费者
            if (!blocked.isDone()) {
                blocked.set(null);
            }
        }
    }
    
    public ListenableFuture<Page> getNextPage() {
        synchronized (this) {
            if (!pages.isEmpty()) {
                Page page = pages.poll();
                bufferedBytes.addAndGet(-page.getSizeInBytes());
                return Futures.immediateFuture(page);
            }
            
            if (noMorePages) {
                return Futures.immediateFuture(null);
            }
            
            // 等待新数据到达
            return Futures.transform(blocked, input -> {
                synchronized (this) {
                    if (!pages.isEmpty()) {
                        Page page = pages.poll();
                        bufferedBytes.addAndGet(-page.getSizeInBytes());
                        return page;
                    }
                    return null;
                }
            }, MoreExecutors.directExecutor());
        }
    }
}
```

### 4.3 背压机制实现

```java
// 背压控制的Exchange实现
public class BackpressureAwareExchange {
    private final ExchangeClient client;
    private final AtomicLong pendingBytes = new AtomicLong();
    private final long maxPendingBytes;
    private volatile SettableFuture<Void> backpressureFuture;
    
    public ListenableFuture<Page> getNextPage() {
        // 检查背压状态
        if (pendingBytes.get() > maxPendingBytes) {
            if (backpressureFuture == null || backpressureFuture.isDone()) {
                backpressureFuture = SettableFuture.create();
            }
            
            // 返回阻塞的Future，暂停数据消费
            return backpressureFuture.thenCompose(ignored -> 
                client.getNextPage());
        }
        
        // 正常获取数据
        return Futures.transform(
            client.getNextPage(),
            this::handlePageReceived,
            MoreExecutors.directExecutor()
        );
    }
    
    private Page handlePageReceived(Page page) {
        if (page != null) {
            long pageSize = page.getSizeInBytes();
            long newPendingBytes = pendingBytes.addAndGet(pageSize);
            
            // 检查是否需要启动背压
            if (newPendingBytes > maxPendingBytes && 
                (backpressureFuture == null || backpressureFuture.isDone())) {
                backpressureFuture = SettableFuture.create();
            }
        }
        return page;
    }
    
    // 当下游处理完页面时调用
    public void pageProcessed(Page page) {
        long newPendingBytes = pendingBytes.addAndGet(-page.getSizeInBytes());
        
        // 如果背压状态解除，通知继续
        if (newPendingBytes <= maxPendingBytes && 
            backpressureFuture != null && !backpressureFuture.isDone()) {
            backpressureFuture.set(null);
        }
    }
}
```

---

## 5. 内存管理与背压机制

### 5.1 内存上下文层次结构

```java
// 内存管理的层次结构
public class QueryMemoryContext {
    private final MemoryPool memoryPool;
    private final AtomicLong userMemory = new AtomicLong();
    private final AtomicLong systemMemory = new AtomicLong();
    private final Map<String, TaskMemoryContext> taskContexts = new ConcurrentHashMap<>();
    
    // 为任务创建内存上下文
    public TaskMemoryContext addTaskMemoryContext(String taskId) {
        TaskMemoryContext taskContext = new TaskMemoryContext(this, taskId);
        taskContexts.put(taskId, taskContext);
        return taskContext;
    }
    
    // 更新用户内存使用
    public void updateUserMemory(long bytes) {
        long newUserMemory = userMemory.addAndGet(bytes);
        
        // 检查是否超过查询级别的内存限制
        if (newUserMemory > getMaxQueryMemory()) {
            throw new PrestoException(EXCEEDED_MEMORY_LIMIT, 
                "Query exceeded per-query memory limit");
        }
        
        // 通知内存池更新
        memoryPool.reserve(bytes);
    }
}

// 任务级别的内存上下文
public class TaskMemoryContext {
    private final QueryMemoryContext queryContext;
    private final String taskId;
    private final AtomicLong localUserMemory = new AtomicLong();
    private final Map<String, OperatorMemoryContext> operatorContexts = new ConcurrentHashMap<>();
    
    public OperatorMemoryContext addOperatorMemoryContext(String operatorId) {
        return operatorContexts.computeIfAbsent(operatorId, 
            id -> new OperatorMemoryContext(this, id));
    }
    
    public void updateMemory(long bytes) {
        localUserMemory.addAndGet(bytes);
        queryContext.updateUserMemory(bytes);
    }
}

// 算子级别的内存上下文
public class OperatorMemoryContext {
    private final TaskMemoryContext taskContext;
    private final String operatorId;
    private final AtomicLong userMemory = new AtomicLong();
    private final AtomicLong systemMemory = new AtomicLong();
    
    // 可撤销内存分配 (用于溢写)
    private final AtomicLong revocableMemory = new AtomicLong();
    private final AtomicReference<MemoryRevocationRequestListener> revocationListener = 
        new AtomicReference<>();
    
    public void setBytes(long bytes) {
        long delta = bytes - userMemory.getAndSet(bytes);
        if (delta != 0) {
            taskContext.updateMemory(delta);
        }
    }
    
    // 分配可撤销内存 (用于Hash表等可溢写的数据结构)
    public void setRevocableBytes(long bytes) {
        long delta = bytes - revocableMemory.getAndSet(bytes);
        if (delta != 0) {
            // 可撤销内存不计入查询限制，但会触发溢写
            checkForMemoryRevocation();
        }
    }
    
    private void checkForMemoryRevocation() {
        MemoryRevocationRequestListener listener = revocationListener.get();
        if (listener != null && shouldRevokeMemory()) {
            // 请求内存撤销 (触发溢写)
            listener.requestMemoryRevocation();
        }
    }
}
```

### 5.2 内存池管理

```java
// 内存池的实现
public class MemoryPool {
    private final long maxBytes;
    private final AtomicLong reservedBytes = new AtomicLong();
    private final Map<String, Long> queryMemoryReservations = new ConcurrentHashMap<>();
    
    // 内存预留
    public void reserve(String queryId, long bytes) {
        long newReservation = queryMemoryReservations.merge(queryId, bytes, Long::sum);
        long newTotal = reservedBytes.addAndGet(bytes);
        
        // 检查内存池限制
        if (newTotal > maxBytes) {
            // 尝试回收内存
            tryReclaimMemory();
            
            // 如果还是超限，抛出异常
            if (reservedBytes.get() > maxBytes) {
                reservedBytes.addAndGet(-bytes);
                queryMemoryReservations.merge(queryId, -bytes, Long::sum);
                throw new PrestoException(CLUSTER_OUT_OF_MEMORY, 
                    "Cluster is out of memory");
            }
        }
    }
    
    // 内存回收策略
    private void tryReclaimMemory() {
        // 1. 请求可撤销内存释放 (溢写)
        requestMemoryRevocation();
        
        // 2. 终止内存使用最多的查询
        killLargestQuery();
        
        // 3. 强制垃圾回收 (最后手段)
        System.gc();
    }
    
    private void requestMemoryRevocation() {
        // 向所有使用可撤销内存的算子发送撤销请求
        for (QueryMemoryContext queryContext : activeQueries.values()) {
            queryContext.requestMemoryRevocation();
        }
    }
}
```

### 5.3 溢写机制实现

```java
// 支持溢写的Hash表实现
public class SpillableHashAggregationBuilder {
    private final GroupByHash groupByHash;
    private final List<Aggregator> aggregators;
    private final SpillContext spillContext;
    private final List<SpilledPartition> spilledPartitions = new ArrayList<>();
    
    public void processPage(Page page) {
        // 正常处理页面
        GroupByIdBlock groupIds = groupByHash.getGroupIds(page);
        
        for (Aggregator aggregator : aggregators) {
            aggregator.processPage(groupIds, page);
        }
        
        // 检查内存使用，如果过高则溢写
        if (getMemoryUsage() > getSpillThreshold()) {
            spillToDisk();
        }
    }
    
    private void spillToDisk() {
        try {
            // 创建溢写文件
            SpillWriter spillWriter = spillContext.createSpillWriter();
            
            // 将哈希表内容写入磁盘
            Iterator<Page> pages = buildOutputPages();
            while (pages.hasNext()) {
                Page page = pages.next();
                spillWriter.write(page);
            }
            spillWriter.close();
            
            // 记录溢写分区
            spilledPartitions.add(new SpilledPartition(spillWriter.getFile()));
            
            // 清空内存中的数据
            groupByHash.clear();
            for (Aggregator aggregator : aggregators) {
                aggregator.reset();
            }
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to spill to disk", e);
        }
    }
    
    // 合并内存和磁盘数据生成最终结果
    public Iterator<Page> buildFinalResult() {
        List<Iterator<Page>> iterators = new ArrayList<>();
        
        // 添加内存中的数据
        if (!groupByHash.isEmpty()) {
            iterators.add(buildOutputPages());
        }
        
        // 添加磁盘中的数据
        for (SpilledPartition partition : spilledPartitions) {
            iterators.add(partition.getPages());
        }
        
        // 合并所有迭代器
        return new MergingIterator<>(iterators, this::mergeSortedPages);
    }
}
```

---

## 6. Driver与Pipeline执行

### 6.1 Driver执行模型

```java
// Driver是Pipeline执行的核心
public class Driver {
    private final DriverContext driverContext;
    private final List<Operator> operators;
    private final Map<String, OperatorStats> operatorStats;
    private final AtomicReference<State> state = new AtomicReference<>(State.ALIVE);
    
    enum State {
        ALIVE,      // 正在执行
        NEED_MORE_INPUT,  // 需要更多输入
        BLOCKED,    // 被阻塞
        FINISHED    // 已完成
    }
    
    // 核心处理循环
    public ListenableFuture<?> process() {
        try {
            long startTime = System.nanoTime();
            
            while (true) {
                State currentState = processInternal();
                
                switch (currentState) {
                    case ALIVE:
                        // 继续处理
                        if (System.nanoTime() - startTime > MAX_PROCESS_TIME_NANOS) {
                            // 避免占用线程太长时间，让出执行
                            return Futures.immediateFuture(null);
                        }
                        break;
                        
                    case NEED_MORE_INPUT:
                        // 需要等待更多输入
                        return Futures.immediateFuture(null);
                        
                    case BLOCKED:
                        // 被阻塞，返回阻塞Future
                        return getBlockedFuture();
                        
                    case FINISHED:
                        // 处理完成
                        return Futures.immediateFuture(null);
                }
            }
        } catch (Exception e) {
            driverContext.failed(e);
            throw e;
        }
    }
    
    private State processInternal() {
        // 从输出算子开始拉取数据
        Operator outputOperator = operators.get(operators.size() - 1);
        
        // 检查输出算子是否被阻塞
        if (!outputOperator.isBlocked().isDone()) {
            return State.BLOCKED;
        }
        
        // 尝试获取输出
        Page outputPage = outputOperator.getOutput();
        if (outputPage != null) {
            // 将输出发送给下游
            driverContext.addOutputPage(outputPage);
            return State.ALIVE;
        }
        
        // 输出算子已完成
        if (outputOperator.isFinished()) {
            return State.FINISHED;
        }
        
        // 输出算子需要更多输入，检查输入算子
        return processInputOperators();
    }
    
    private State processInputOperators() {
        // 反向遍历算子链，确保数据流动
        for (int i = operators.size() - 2; i >= 0; i--) {
            Operator operator = operators.get(i);
            Operator downstream = operators.get(i + 1);
            
            // 如果下游需要输入且当前算子有输出
            if (downstream.needsInput() && !operator.isBlocked().isDone()) {
                return State.BLOCKED;
            }
            
            if (downstream.needsInput()) {
                Page page = operator.getOutput();
                if (page != null) {
                    downstream.addInput(page);
                    return State.ALIVE;
                }
                
                if (operator.isFinished()) {
                    downstream.finish();
                } else {
                    return State.NEED_MORE_INPUT;
                }
            }
        }
        
        return State.ALIVE;
    }
}
```

### 6.2 Pipeline并行执行

```java
// TaskExecutor管理多个Driver的并行执行
public class TaskExecutor {
    private final ExecutorService executor;
    private final List<DriverRunner> drivers = new ArrayList<>();
    private final AtomicInteger runningDrivers = new AtomicInteger();
    
    // Driver包装器，处理异步执行
    private class DriverRunner implements Runnable {
        private final Driver driver;
        private final AtomicReference<ListenableFuture<?>> blocked = 
            new AtomicReference<>(Futures.immediateFuture(null));
        
        public DriverRunner(Driver driver) {
            this.driver = driver;
        }
        
        @Override
        public void run() {
            try {
                // 执行Driver处理循环
                ListenableFuture<?> future = driver.process();
                
                if (future.isDone()) {
                    // 立即完成，重新调度
                    if (!driver.isFinished()) {
                        executor.execute(this);
                    } else {
                        runningDrivers.decrementAndGet();
                    }
                } else {
                    // 异步等待，完成后重新调度
                    blocked.set(future);
                    future.addListener(() -> {
                        if (!driver.isFinished()) {
                            executor.execute(this);
                        } else {
                            runningDrivers.decrementAndGet();
                        }
                    }, executor);
                }
                
            } catch (Exception e) {
                driver.failed(e);
                runningDrivers.decrementAndGet();
            }
        }
    }
    
    public void addDriver(Driver driver) {
        DriverRunner runner = new DriverRunner(driver);
        drivers.add(runner);
        runningDrivers.incrementAndGet();
        executor.execute(runner);
    }
    
    public void waitForCompletion() throws InterruptedException {
        while (runningDrivers.get() > 0) {
            Thread.sleep(100);
        }
    }
}
```

---

## 7. 调度器与任务管理

### 7.1 分片调度机制

```java
// 分片调度器负责将分片分配给Tasks
public class SplitScheduler {
    private final PlanNodeId sourceId;
    private final ConnectorSplitSource splitSource;
    private final List<RemoteTask> tasks;
    private final Queue<Split> pendingSplits = new ArrayDeque<>();
    private final AtomicInteger assignedSplits = new AtomicInteger();
    
    // 调度分片到任务
    public void scheduleSplits() {
        // 从连接器获取更多分片
        CompletableFuture<ConnectorSplitBatch> future = splitSource.getNextBatch(1000);
        
        future.thenAccept(batch -> {
            // 将新分片添加到待处理队列
            for (ConnectorSplit split : batch.getSplits()) {
                pendingSplits.add(new Split(sourceId, split));
            }
            
            // 分配分片到任务
            assignSplitsToTasks();
            
            if (!batch.isLastBatch()) {
                // 继续获取更多分片
                scheduleSplits();
            }
        });
    }
    
    private void assignSplitsToTasks() {
        // 轮询分配策略
        int taskIndex = 0;
        
        while (!pendingSplits.isEmpty()) {
            Split split = pendingSplits.poll();
            RemoteTask task = tasks.get(taskIndex % tasks.size());
            
            // 考虑数据本地性
            RemoteTask preferredTask = findPreferredTask(split);
            if (preferredTask != null) {
                task = preferredTask;
            }
            
            // 分配分片到任务
            task.addSplits(ImmutableList.of(split));
            assignedSplits.incrementAndGet();
            
            taskIndex++;
        }
    }
    
    private RemoteTask findPreferredTask(Split split) {
        // 基于分片的主机信息选择本地任务
        List<HostAddress> hosts = split.getConnectorSplit().getAddresses();
        if (hosts.isEmpty()) {
            return null;
        }
        
        for (RemoteTask task : tasks) {
            if (hosts.contains(task.getNodeId().getHostAddress())) {
                return task;
            }
        }
        
        return null;
    }
}
```

### 7.2 自适应任务调度

```java
// 自适应调度器根据性能反馈调整并行度
public class AdaptiveTaskScheduler {
    private final ScheduledExecutorService scheduler;
    private final Map<StageId, StageExecution> stages = new ConcurrentHashMap<>();
    private final AtomicReference<SchedulingPolicy> policy = 
        new AtomicReference<>(SchedulingPolicy.UNIFORM);
    
    enum SchedulingPolicy {
        UNIFORM,        // 均匀分配
        PERFORMANCE,    // 基于性能分配
        LOCALITY        // 基于数据本地性分配
    }
    
    public void startAdaptiveScheduling() {
        scheduler.scheduleWithFixedDelay(
            this::adjustScheduling, 
            10, 10, TimeUnit.SECONDS
        );
    }
    
    private void adjustScheduling() {
        for (StageExecution stage : stages.values()) {
            StageStats stats = stage.getStageStats();
            
            // 分析任务性能差异
            double performanceVariance = calculatePerformanceVariance(stats);
            
            if (performanceVariance > HIGH_VARIANCE_THRESHOLD) {
                // 性能差异大，重新平衡任务
                rebalanceTasks(stage);
            }
            
            // 检查是否需要调整并行度
            if (stats.getAverageQueueTime() > QUEUE_TIME_THRESHOLD) {
                // 队列时间过长，增加并行度
                scaleUp(stage);
            } else if (stats.getCpuUtilization() < LOW_CPU_THRESHOLD) {
                // CPU利用率低，减少并行度
                scaleDown(stage);
            }
        }
    }
    
    private void rebalanceTasks(StageExecution stage) {
        List<RemoteTask> tasks = stage.getAllTasks();
        List<TaskStats> taskStats = tasks.stream()
            .map(RemoteTask::getTaskStats)
            .collect(toList());
            
        // 识别慢任务
        double avgProcessingTime = taskStats.stream()
            .mapToDouble(TaskStats::getProcessingTime)
            .average()
            .orElse(0.0);
            
        List<RemoteTask> slowTasks = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            if (taskStats.get(i).getProcessingTime() > avgProcessingTime * 1.5) {
                slowTasks.add(tasks.get(i));
            }
        }
        
        // 将慢任务的分片重新分配
        for (RemoteTask slowTask : slowTasks) {
            redistributeSplits(slowTask, tasks);
        }
    }
    
    private void redistributeSplits(RemoteTask slowTask, List<RemoteTask> allTasks) {
        // 获取慢任务的待处理分片
        List<Split> pendingSplits = slowTask.getPendingSplits();
        
        // 取消慢任务的部分分片
        int splitsToRedistribute = Math.min(pendingSplits.size() / 2, 10);
        List<Split> splitsToMove = pendingSplits.subList(0, splitsToRedistribute);
        
        slowTask.removeSplits(splitsToMove);
        
        // 重新分配到其他任务
        for (Split split : splitsToMove) {
            RemoteTask targetTask = selectBestTask(allTasks, split);
            targetTask.addSplits(ImmutableList.of(split));
        }
    }
}
```

---

## 8. 性能关键路径分析

### 8.1 CPU热点识别

```java
// 性能分析器用于识别CPU热点
public class TrinoProfiler {
    private final Map<String, Long> operatorCpuTime = new ConcurrentHashMap<>();
    private final Map<String, Long> operatorWallTime = new ConcurrentHashMap<>();
    private final ThreadLocal<String> currentOperator = new ThreadLocal<>();
    
    // 在算子执行前后测量时间
    public void recordOperatorTime(String operatorId, Runnable operation) {
        long startCpu = getCurrentThreadCpuTime();
        long startWall = System.nanoTime();
        
        currentOperator.set(operatorId);
        try {
            operation.run();
        } finally {
            currentOperator.remove();
            
            long cpuTime = getCurrentThreadCpuTime() - startCpu;
            long wallTime = System.nanoTime() - startWall;
            
            operatorCpuTime.merge(operatorId, cpuTime, Long::sum);
            operatorWallTime.merge(operatorId, wallTime, Long::sum);
        }
    }
    
    // 生成性能报告
    public PerformanceReport generateReport() {
        Map<String, OperatorProfile> profiles = new HashMap<>();
        
        for (String operatorId : operatorCpuTime.keySet()) {
            long cpu = operatorCpuTime.getOrDefault(operatorId, 0L);
            long wall = operatorWallTime.getOrDefault(operatorId, 0L);
            
            double cpuUtilization = wall > 0 ? (double) cpu / wall : 0.0;
            
            profiles.put(operatorId, new OperatorProfile(
                operatorId, cpu, wall, cpuUtilization));
        }
        
        return new PerformanceReport(profiles);
    }
    
    // 实时监控CPU热点
    public void startContinuousMonitoring() {
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        
        monitor.scheduleAtFixedRate(() -> {
            // 识别CPU使用率最高的算子
            String hottestOperator = operatorCpuTime.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("UNKNOWN");
                
            long hotCpuTime = operatorCpuTime.getOrDefault(hottestOperator, 0L);
            
            if (hotCpuTime > HIGH_CPU_THRESHOLD) {
                // 触发优化建议
                generateOptimizationSuggestion(hottestOperator);
            }
            
        }, 0, 30, TimeUnit.SECONDS);
    }
}
```

### 8.2 内存分配分析

```java
// 内存分配跟踪器
public class MemoryAllocationTracker {
    private final Map<String, AllocationStats> allocationsByOperator = new ConcurrentHashMap<>();
    private final AtomicLong totalAllocations = new AtomicLong();
    private final AtomicLong totalDeallocations = new AtomicLong();
    
    static class AllocationStats {
        final AtomicLong allocations = new AtomicLong();
        final AtomicLong deallocations = new AtomicLong();
        final AtomicLong peakMemory = new AtomicLong();
        final AtomicLong currentMemory = new AtomicLong();
        
        void recordAllocation(long bytes) {
            allocations.addAndGet(bytes);
            long current = currentMemory.addAndGet(bytes);
            
            // 更新峰值内存
            long peak = peakMemory.get();
            while (current > peak && !peakMemory.compareAndSet(peak, current)) {
                peak = peakMemory.get();
            }
        }
        
        void recordDeallocation(long bytes) {
            deallocations.addAndGet(bytes);
            currentMemory.addAndGet(-bytes);
        }
    }
    
    public void recordAllocation(String operatorId, long bytes) {
        AllocationStats stats = allocationsByOperator.computeIfAbsent(
            operatorId, k -> new AllocationStats());
        
        stats.recordAllocation(bytes);
        totalAllocations.addAndGet(bytes);
    }
    
    public void recordDeallocation(String operatorId, long bytes) {
        AllocationStats stats = allocationsByOperator.get(operatorId);
        if (stats != null) {
            stats.recordDeallocation(bytes);
            totalDeallocations.addAndGet(bytes);
        }
    }
    
    // 生成内存使用报告
    public MemoryReport generateMemoryReport() {
        Map<String, OperatorMemoryUsage> usage = new HashMap<>();
        
        for (Map.Entry<String, AllocationStats> entry : allocationsByOperator.entrySet()) {
            String operatorId = entry.getKey();
            AllocationStats stats = entry.getValue();
            
            usage.put(operatorId, new OperatorMemoryUsage(
                operatorId,
                stats.allocations.get(),
                stats.deallocations.get(), 
                stats.peakMemory.get(),
                stats.currentMemory.get()
            ));
        }
        
        return new MemoryReport(
            totalAllocations.get(),
            totalDeallocations.get(),
            usage
        );
    }
    
    // 检测内存泄漏
    public List<String> detectMemoryLeaks() {
        List<String> leakyOperators = new ArrayList<>();
        
        for (Map.Entry<String, AllocationStats> entry : allocationsByOperator.entrySet()) {
            AllocationStats stats = entry.getValue();
            long allocated = stats.allocations.get();
            long deallocated = stats.deallocations.get();
            
            // 如果分配远大于释放，可能存在内存泄漏
            if (allocated > 0 && deallocated < allocated * 0.8) {
                leakyOperators.add(entry.getKey());
            }
        }
        
        return leakyOperators;
    }
}
```

### 8.3 IO性能分析

```java
// IO性能分析器
public class IOPerformanceAnalyzer {
    private final Map<String, IOStats> ioStatsByConnector = new ConcurrentHashMap<>();
    
    static class IOStats {
        final AtomicLong bytesRead = new AtomicLong();
        final AtomicLong bytesWritten = new AtomicLong();
        final AtomicLong readOperations = new AtomicLong();
        final AtomicLong writeOperations = new AtomicLong();
        final AtomicLong readTime = new AtomicLong(); // nanoseconds
        final AtomicLong writeTime = new AtomicLong(); // nanoseconds
        
        double getReadThroughput() {
            long time = readTime.get();
            return time > 0 ? (double) bytesRead.get() / time * 1_000_000_000 : 0.0; // bytes/sec
        }
        
        double getWriteThroughput() {
            long time = writeTime.get();
            return time > 0 ? (double) bytesWritten.get() / time * 1_000_000_000 : 0.0; // bytes/sec
        }
    }
    
    public void recordRead(String connector, long bytes, long durationNanos) {
        IOStats stats = ioStatsByConnector.computeIfAbsent(connector, k -> new IOStats());
        stats.bytesRead.addAndGet(bytes);
        stats.readOperations.incrementAndGet();
        stats.readTime.addAndGet(durationNanos);
    }
    
    public void recordWrite(String connector, long bytes, long durationNanos) {
        IOStats stats = ioStatsByConnector.computeIfAbsent(connector, k -> new IOStats());
        stats.bytesWritten.addAndGet(bytes);
        stats.writeOperations.incrementAndGet();
        stats.writeTime.addAndGet(durationNanos);
    }
    
    // 生成IO性能报告
    public IOPerformanceReport generateReport() {
        Map<String, ConnectorIOPerformance> performance = new HashMap<>();
        
        for (Map.Entry<String, IOStats> entry : ioStatsByConnector.entrySet()) {
            String connector = entry.getKey();
            IOStats stats = entry.getValue();
            
            performance.put(connector, new ConnectorIOPerformance(
                connector,
                stats.bytesRead.get(),
                stats.bytesWritten.get(),
                stats.readOperations.get(),
                stats.writeOperations.get(),
                stats.getReadThroughput(),
                stats.getWriteThroughput()
            ));
        }
        
        return new IOPerformanceReport(performance);
    }
    
    // 识别IO瓶颈
    public List<String> identifyIOBottlenecks() {
        List<String> bottlenecks = new ArrayList<>();
        
        for (Map.Entry<String, IOStats> entry : ioStatsByConnector.entrySet()) {
            String connector = entry.getKey();
            IOStats stats = entry.getValue();
            
            double readThroughput = stats.getReadThroughput();
            double writeThroughput = stats.getWriteThroughput();
            
            // 检查是否低于预期吞吐量阈值
            if (readThroughput < MIN_READ_THROUGHPUT || 
                writeThroughput < MIN_WRITE_THROUGHPUT) {
                bottlenecks.add(connector);
            }
        }
        
        return bottlenecks;
    }
}
```

---

## 📋 总结与调试指南

### 🔍 常用调试技术

#### 1. 执行计划分析

```sql
-- 查看详细的算子统计
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) SELECT ...;

-- 分析实际执行统计 
EXPLAIN ANALYZE SELECT ...;
```

#### 2. 运行时监控

```java
// 监控Driver执行状态
SELECT 
    stage_id,
    task_id,
    driver_id,
    total_scheduled_time,
    total_cpu_time,
    total_blocked_time
FROM system.runtime.tasks 
WHERE query_id = 'your_query_id';
```

#### 3. 内存使用分析

```java
// 查看内存使用详情
SELECT 
    task_id,
    peak_user_memory_reservation,
    cumulative_user_memory,
    user_memory_reservation
FROM system.runtime.tasks
WHERE query_id = 'your_query_id'
ORDER BY peak_user_memory_reservation DESC;
```

### ⚡ 性能优化要点

#### 1. **Page大小优化**

- 默认1MB，可根据网络和内存情况调整
- 过大影响延迟，过小影响吞吐量

#### 2. **算子并行度调整**

- `task.concurrency`: 每个Task的Driver数量
- `node-scheduler.max-splits-per-node`: 每节点最大Split数

#### 3. **内存配置优化**

- `query.max-memory-per-node`: 查询在单节点的内存限制
- `memory.heap-headroom-per-node`: 为系统预留的堆内存

#### 4. **Exchange调优**

- `exchange.max-buffer-size`: Exchange缓冲区大小
- `sink.max-buffer-size`: 输出缓冲区大小

### 🎯 故障排除流程

1. **性能问题**: 分析EXPLAIN ANALYZE输出，识别慢算子
2. **内存问题**: 检查内存使用统计，寻找内存泄漏算子  
3. **网络问题**: 监控Exchange传输量，检查网络瓶颈
4. **CPU问题**: 使用性能分析器识别CPU热点函数

通过深入理解Trino执行器的源码实现，你现在具备了从底层优化查询性能的能力！这些知识将帮助你：

- **精准定位性能瓶颈**的根本原因
- **深度调优**执行器参数和算法
- **贡献代码**到Trino开源社区
- **设计高性能**的自定义算子和连接器

继续探索Trino的源码世界，你会发现更多优化的奥秘！🚀
