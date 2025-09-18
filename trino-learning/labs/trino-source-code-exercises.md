# Trino 源码调试与执行器实验

## 🎯 实验概述

这套源码级实验专门配合《Trino 执行器源码级深度剖析》文档，通过实际的调试和代码分析，帮助你深入理解Trino执行引擎的内部工作机制。

**前置条件**: 
- 完成基础优化学习
- 熟悉Java编程和调试工具
- 本地或远程可访问的Trino环境

---

## 📚 实验目录

1. [实验1: Page数据结构探索](#实验1-page数据结构探索)
2. [实验2: 算子执行流程追踪](#实验2-算子执行流程追踪)
3. [实验3: Exchange数据传输分析](#实验3-exchange数据传输分析)
4. [实验4: 内存管理机制验证](#实验4-内存管理机制验证)
5. [实验5: 背压机制模拟](#实验5-背压机制模拟)
6. [实验6: 性能热点分析](#实验6-性能热点分析)
7. [实验7: 自定义算子开发](#实验7-自定义算子开发)
8. [实验8: 源码级性能调优](#实验8-源码级性能调优)

---

## 实验1: Page数据结构探索

### 🎯 学习目标
- 理解Page和Block的内存布局
- 掌握列式数据的存储和访问机制
- 观察零拷贝技术的实际效果
- 分析不同数据类型的存储优化

### 🔬 实验步骤

#### 步骤1: Page结构分析工具
```java
// 创建Page分析工具类
public class PageAnalyzer {
    
    public static void analyzePage(Page page) {
        System.out.println("=== Page Analysis ===");
        System.out.println("Position Count: " + page.getPositionCount());
        System.out.println("Channel Count: " + page.getChannelCount());
        System.out.println("Size in Bytes: " + page.getSizeInBytes());
        System.out.println("Retained Size: " + page.getRetainedSizeInBytes());
        
        // 分析每个Block
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            analyzeBlock(i, block);
        }
    }
    
    private static void analyzeBlock(int channel, Block block) {
        System.out.println("\n--- Block " + channel + " ---");
        System.out.println("Type: " + block.getClass().getSimpleName());
        System.out.println("Position Count: " + block.getPositionCount());
        System.out.println("Size in Bytes: " + block.getSizeInBytes());
        System.out.println("Retained Size: " + block.getRetainedSizeInBytes());
        
        // 分析null值分布
        int nullCount = 0;
        for (int pos = 0; pos < Math.min(block.getPositionCount(), 10); pos++) {
            if (block.isNull(pos)) {
                nullCount++;
            }
        }
        System.out.println("Null Count (first 10): " + nullCount);
        
        // 显示前几个值
        System.out.println("Sample values:");
        for (int pos = 0; pos < Math.min(block.getPositionCount(), 5); pos++) {
            if (!block.isNull(pos)) {
                System.out.println("  [" + pos + "]: " + extractValue(block, pos));
            } else {
                System.out.println("  [" + pos + "]: NULL");
            }
        }
    }
    
    private static Object extractValue(Block block, int position) {
        // 根据Block类型提取值 (简化实现)
        if (block instanceof IntArrayBlock) {
            return ((IntArrayBlock) block).getInt(position);
        } else if (block instanceof LongArrayBlock) {
            return ((LongArrayBlock) block).getLong(position);
        } else if (block instanceof VariableWidthBlock) {
            return ((VariableWidthBlock) block).getSlice(position, 0, 
                ((VariableWidthBlock) block).getSliceLength(position)).toStringUtf8();
        }
        return "Unknown type";
    }
}
```

#### 步骤2: 创建测试数据并分析
```sql
-- 在Trino中执行，观察不同查询的Page特征

-- 测试1: 整数列
SELECT 
    orderkey, 
    custkey, 
    totalprice
FROM orders_test 
LIMIT 1000;

-- 测试2: 字符串列  
SELECT 
    name,
    address, 
    mktsegment
FROM customer_test
LIMIT 1000;

-- 测试3: 混合类型
SELECT 
    c.name,
    o.orderkey,
    o.totalprice,
    o.orderdate
FROM customer_test c 
JOIN orders_test o ON c.custkey = o.custkey
LIMIT 1000;
```

#### 步骤3: 零拷贝机制验证
```java
// 验证Page切片的零拷贝特性
public class ZeroCopyTest {
    
    public static void testZeroCopy() {
        // 创建原始Page
        Block originalBlock = createLargeBlock(100000);
        Page originalPage = new Page(originalBlock);
        
        long originalMemory = originalPage.getRetainedSizeInBytes();
        System.out.println("Original page retained size: " + originalMemory);
        
        // 创建多个切片
        Page slice1 = originalPage.getRegion(0, 1000);
        Page slice2 = originalPage.getRegion(1000, 2000);
        Page slice3 = originalPage.getRegion(50000, 1000);
        
        long totalSliceMemory = slice1.getRetainedSizeInBytes() +
                               slice2.getRetainedSizeInBytes() + 
                               slice3.getRetainedSizeInBytes();
        
        System.out.println("Total slice retained size: " + totalSliceMemory);
        System.out.println("Memory sharing ratio: " + 
            (double) totalSliceMemory / originalMemory);
            
        // 验证数据一致性
        verifySliceConsistency(originalPage, slice1, 0);
        verifySliceConsistency(originalPage, slice2, 1000);
        verifySliceConsistency(originalPage, slice3, 50000);
    }
    
    private static void verifySliceConsistency(Page original, Page slice, int offset) {
        Block originalBlock = original.getBlock(0);
        Block sliceBlock = slice.getBlock(0);
        
        for (int i = 0; i < Math.min(slice.getPositionCount(), 10); i++) {
            long originalValue = originalBlock.getLong(offset + i);
            long sliceValue = sliceBlock.getLong(i);
            
            assert originalValue == sliceValue : 
                "Value mismatch at position " + i + ": " + originalValue + " vs " + sliceValue;
        }
        System.out.println("Slice consistency verified for offset " + offset);
    }
}
```

### 📊 实验记录表

| 数据类型 | Page大小 | Block类型 | 压缩率 | 切片开销 | 备注 |
|---------|----------|-----------|--------|---------|------|
| 整数列 |  |  |  |  |  |
| 字符串列 |  |  |  |  |  |
| 混合列 |  |  |  |  |  |

---

## 实验2: 算子执行流程追踪

### 🎯 学习目标
- 追踪单个算子的执行流程
- 理解Pull模式的数据流动
- 观察算子状态转换
- 分析算子性能特征

### 🔬 实验步骤

#### 步骤1: 算子执行追踪器
```java
// 算子执行状态追踪器
public class OperatorTracker {
    private final Map<String, OperatorStats> operatorStats = new ConcurrentHashMap<>();
    
    public static class OperatorStats {
        private final AtomicLong getOutputCalls = new AtomicLong();
        private final AtomicLong addInputCalls = new AtomicLong();
        private final AtomicLong blockedTime = new AtomicLong();
        private final AtomicLong processingTime = new AtomicLong();
        private final AtomicLong outputPages = new AtomicLong();
        private final AtomicLong outputRows = new AtomicLong();
        private final AtomicLong inputPages = new AtomicLong();
        private final AtomicLong inputRows = new AtomicLong();
        
        public void recordGetOutput(Page page, long duration) {
            getOutputCalls.incrementAndGet();
            processingTime.addAndGet(duration);
            if (page != null) {
                outputPages.incrementAndGet();
                outputRows.addAndGet(page.getPositionCount());
            }
        }
        
        public void recordAddInput(Page page, long duration) {
            addInputCalls.incrementAndGet();
            processingTime.addAndGet(duration);
            if (page != null) {
                inputPages.incrementAndGet();
                inputRows.addAndGet(page.getPositionCount());
            }
        }
        
        public void recordBlocked(long duration) {
            blockedTime.addAndGet(duration);
        }
        
        public void printStats(String operatorId) {
            System.out.println("\n=== " + operatorId + " Stats ===");
            System.out.println("GetOutput calls: " + getOutputCalls.get());
            System.out.println("AddInput calls: " + addInputCalls.get());
            System.out.println("Processing time (ms): " + processingTime.get() / 1_000_000);
            System.out.println("Blocked time (ms): " + blockedTime.get() / 1_000_000);
            System.out.println("Input pages/rows: " + inputPages.get() + "/" + inputRows.get());
            System.out.println("Output pages/rows: " + outputPages.get() + "/" + outputRows.get());
            
            if (inputRows.get() > 0) {
                System.out.println("Selectivity: " + 
                    (double) outputRows.get() / inputRows.get());
            }
            
            if (outputPages.get() > 0) {
                System.out.println("Avg rows per output page: " + 
                    (double) outputRows.get() / outputPages.get());
            }
        }
    }
    
    public OperatorStats getOrCreateStats(String operatorId) {
        return operatorStats.computeIfAbsent(operatorId, k -> new OperatorStats());
    }
    
    public void printAllStats() {
        operatorStats.forEach(OperatorStats::printStats);
    }
}

// 包装算子以添加追踪功能
public class TrackedOperator implements Operator {
    private final Operator delegate;
    private final String operatorId;
    private final OperatorTracker tracker;
    
    public TrackedOperator(Operator delegate, String operatorId, OperatorTracker tracker) {
        this.delegate = delegate;
        this.operatorId = operatorId;
        this.tracker = tracker;
    }
    
    @Override
    public Page getOutput() {
        long startTime = System.nanoTime();
        Page result = delegate.getOutput();
        long duration = System.nanoTime() - startTime;
        
        tracker.getOrCreateStats(operatorId).recordGetOutput(result, duration);
        return result;
    }
    
    @Override
    public void addInput(Page page) {
        long startTime = System.nanoTime();
        delegate.addInput(page);
        long duration = System.nanoTime() - startTime;
        
        tracker.getOrCreateStats(operatorId).recordAddInput(page, duration);
    }
    
    @Override
    public ListenableFuture<?> isBlocked() {
        ListenableFuture<?> blocked = delegate.isBlocked();
        
        if (!blocked.isDone()) {
            long startTime = System.nanoTime();
            blocked.addListener(() -> {
                long duration = System.nanoTime() - startTime;
                tracker.getOrCreateStats(operatorId).recordBlocked(duration);
            }, MoreExecutors.directExecutor());
        }
        
        return blocked;
    }
    
    // 其他方法委托给原始算子...
}
```

#### 步骤2: Pipeline执行追踪
```java
// 追踪完整的Pipeline执行
public class PipelineTracker {
    
    public static void trackPipelineExecution(List<Operator> operators, 
                                             List<Page> inputPages) {
        OperatorTracker tracker = new OperatorTracker();
        
        // 包装所有算子
        List<Operator> trackedOperators = new ArrayList<>();
        for (int i = 0; i < operators.size(); i++) {
            String operatorId = operators.get(i).getClass().getSimpleName() + "_" + i;
            trackedOperators.add(new TrackedOperator(operators.get(i), operatorId, tracker));
        }
        
        // 模拟Driver执行
        simulateDriverExecution(trackedOperators, inputPages);
        
        // 打印统计信息
        tracker.printAllStats();
    }
    
    private static void simulateDriverExecution(List<Operator> operators, 
                                               List<Page> inputPages) {
        Operator inputOperator = operators.get(0);
        Operator outputOperator = operators.get(operators.size() - 1);
        
        // 输入所有页面
        for (Page page : inputPages) {
            inputOperator.addInput(page);
        }
        inputOperator.finish();
        
        // 执行Pull模式处理
        List<Page> outputPages = new ArrayList<>();
        while (!outputOperator.isFinished()) {
            Page outputPage = outputOperator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
            
            // 模拟Driver的数据传递逻辑
            propagateDataThroughPipeline(operators);
        }
        
        System.out.println("Pipeline execution completed. Output pages: " + outputPages.size());
    }
    
    private static void propagateDataThroughPipeline(List<Operator> operators) {
        // 从后往前传播数据需求
        for (int i = operators.size() - 1; i > 0; i--) {
            Operator downstream = operators.get(i);
            Operator upstream = operators.get(i - 1);
            
            if (downstream.needsInput() && !upstream.isFinished()) {
                Page page = upstream.getOutput();
                if (page != null) {
                    downstream.addInput(page);
                }
            }
        }
    }
}
```

### 📊 算子性能分析表

| 算子类型 | 处理时间(ms) | 阻塞时间(ms) | 输入行数 | 输出行数 | 选择性 |
|---------|-------------|-------------|----------|----------|--------|
| TableScan |  |  |  |  |  |
| Filter |  |  |  |  |  |
| Project |  |  |  |  |  |
| HashJoin |  |  |  |  |  |

---

## 实验3: Exchange数据传输分析

### 🎯 学习目标
- 理解本地和远程Exchange的区别
- 观察数据传输的序列化开销
- 分析网络传输的批处理效果
- 验证背压机制的工作原理

### 🔬 实验步骤

#### 步骤1: Exchange性能测量工具
```java
// Exchange性能分析器
public class ExchangePerformanceAnalyzer {
    
    public static class ExchangeMetrics {
        private final AtomicLong totalBytes = new AtomicLong();
        private final AtomicLong totalPages = new AtomicLong();
        private final AtomicLong totalTime = new AtomicLong();
        private final AtomicLong serializationTime = new AtomicLong();
        private final AtomicLong networkTime = new AtomicLong();
        private final AtomicLong deserializationTime = new AtomicLong();
        
        public void recordTransfer(long bytes, long totalTime, 
                                 long serTime, long netTime, long deserTime) {
            totalBytes.addAndGet(bytes);
            totalPages.incrementAndGet();
            this.totalTime.addAndGet(totalTime);
            serializationTime.addAndGet(serTime);
            networkTime.addAndGet(netTime);
            deserializationTime.addAndGet(deserTime);
        }
        
        public void printMetrics() {
            System.out.println("=== Exchange Metrics ===");
            System.out.println("Total pages: " + totalPages.get());
            System.out.println("Total bytes: " + totalBytes.get());
            System.out.println("Average page size: " + 
                (totalPages.get() > 0 ? totalBytes.get() / totalPages.get() : 0));
            
            long totalTimeMs = totalTime.get() / 1_000_000;
            System.out.println("Total time (ms): " + totalTimeMs);
            System.out.println("Throughput (MB/s): " + 
                (totalTimeMs > 0 ? (totalBytes.get() / 1024.0 / 1024.0) / (totalTimeMs / 1000.0) : 0));
            
            System.out.println("Serialization time (ms): " + serializationTime.get() / 1_000_000);
            System.out.println("Network time (ms): " + networkTime.get() / 1_000_000);
            System.out.println("Deserialization time (ms): " + deserializationTime.get() / 1_000_000);
        }
    }
    
    // 模拟远程Exchange传输
    public static void simulateRemoteExchange(List<Page> pages) {
        ExchangeMetrics metrics = new ExchangeMetrics();
        PagesSerde pagesSerde = new PagesSerdeFactory(new BlockEncodingManager()).createPagesSerde();
        
        for (Page page : pages) {
            // 序列化
            long serStart = System.nanoTime();
            Slice serialized = pagesSerde.serialize(page);
            long serTime = System.nanoTime() - serStart;
            
            // 模拟网络传输 (添加延迟)
            long netStart = System.nanoTime();
            simulateNetworkDelay(serialized.length());
            long netTime = System.nanoTime() - netStart;
            
            // 反序列化
            long deserStart = System.nanoTime();
            Page deserialized = pagesSerde.deserialize(serialized.getInput());
            long deserTime = System.nanoTime() - deserStart;
            
            // 验证数据完整性
            verifyPageEquality(page, deserialized);
            
            long totalTime = serTime + netTime + deserTime;
            metrics.recordTransfer(serialized.length(), totalTime, serTime, netTime, deserTime);
        }
        
        metrics.printMetrics();
    }
    
    // 模拟本地Exchange传输
    public static void simulateLocalExchange(List<Page> pages) {
        ExchangeMetrics metrics = new ExchangeMetrics();
        
        for (Page page : pages) {
            long start = System.nanoTime();
            
            // 本地Exchange通常是直接传递引用，无序列化开销
            // 但可能涉及缓冲和排队
            simulateLocalBuffering(page);
            
            long totalTime = System.nanoTime() - start;
            metrics.recordTransfer(page.getSizeInBytes(), totalTime, 0, 0, 0);
        }
        
        metrics.printMetrics();
    }
    
    private static void simulateNetworkDelay(int bytes) {
        // 模拟网络延迟 (1Gbps网络，10ms基础延迟)
        long transmissionTime = bytes * 8L / 1_000_000_000L * 1_000_000_000L; // 纳秒
        long baseLatency = 10_000_000L; // 10ms基础延迟
        
        try {
            Thread.sleep((transmissionTime + baseLatency) / 1_000_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private static void simulateLocalBuffering(Page page) {
        // 模拟本地缓冲区操作延迟
        try {
            Thread.sleep(0, (int) (page.getPositionCount() * 100)); // 每行100ns
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    private static void verifyPageEquality(Page original, Page deserialized) {
        assert original.getChannelCount() == deserialized.getChannelCount();
        assert original.getPositionCount() == deserialized.getPositionCount();
        
        // 验证少量数据样本
        for (int channel = 0; channel < Math.min(original.getChannelCount(), 3); channel++) {
            Block origBlock = original.getBlock(channel);
            Block deserBlock = deserialized.getBlock(channel);
            
            for (int pos = 0; pos < Math.min(origBlock.getPositionCount(), 10); pos++) {
                assert origBlock.isNull(pos) == deserBlock.isNull(pos);
                // 更详细的值比较需要根据具体的数据类型实现
            }
        }
    }
}
```

#### 步骤2: 背压机制实验
```java
// 背压机制模拟实验
public class BackpressureExperiment {
    
    public static void runBackpressureTest() {
        // 创建有限容量的缓冲区
        BlockingQueue<Page> buffer = new ArrayBlockingQueue<>(10);
        
        AtomicBoolean producerBlocked = new AtomicBoolean(false);
        AtomicBoolean consumerBlocked = new AtomicBoolean(false);
        
        // 生产者线程 (高速产生数据)
        Thread producer = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                Page page = createTestPage(1000, i); // 1000行的测试页
                
                try {
                    long start = System.nanoTime();
                    buffer.put(page); // 阻塞式放入
                    long blocked = System.nanoTime() - start;
                    
                    if (blocked > 1_000_000) { // 超过1ms认为被阻塞
                        producerBlocked.set(true);
                        System.out.println("Producer blocked for " + blocked / 1_000_000 + "ms at page " + i);
                    }
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        // 消费者线程 (低速消费数据)
        Thread consumer = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                try {
                    long start = System.nanoTime();
                    Page page = buffer.take(); // 阻塞式取出
                    long blocked = System.nanoTime() - start;
                    
                    if (blocked > 1_000_000) { // 超过1ms认为被阻塞
                        consumerBlocked.set(true);
                        System.out.println("Consumer blocked for " + blocked / 1_000_000 + "ms at page " + i);
                    }
                    
                    // 模拟慢速处理
                    Thread.sleep(50); // 50ms处理时间
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        long start = System.currentTimeMillis();
        producer.start();
        consumer.start();
        
        try {
            producer.join();
            consumer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        long duration = System.currentTimeMillis() - start;
        
        System.out.println("\n=== Backpressure Test Results ===");
        System.out.println("Total duration: " + duration + "ms");
        System.out.println("Producer was blocked: " + producerBlocked.get());
        System.out.println("Consumer was blocked: " + consumerBlocked.get());
        System.out.println("Final buffer size: " + buffer.size());
    }
    
    private static Page createTestPage(int rowCount, int pageId) {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            BIGINT.writeLong(builder, pageId * 1000L + i);
        }
        return new Page(builder.build());
    }
}
```

### 📊 Exchange性能对比

| Exchange类型 | 吞吐量(MB/s) | 延迟(ms) | CPU使用率 | 序列化开销 | 备注 |
|-------------|-------------|----------|-----------|------------|------|
| 本地Exchange |  |  |  |  |  |
| 远程Exchange |  |  |  |  |  |
| 有背压 |  |  |  |  |  |
| 无背压 |  |  |  |  |  |

---

## 实验4: 内存管理机制验证

### 🎯 学习目标
- 验证三级内存上下文的层次结构
- 观察内存限制和溢写机制
- 理解可撤销内存的作用
- 分析内存回收策略

### 🔬 实验步骤

#### 步骤1: 内存上下文监控工具
```java
// 内存使用监控和分析工具
public class MemoryUsageMonitor {
    
    public static class MemorySnapshot {
        private final long timestamp;
        private final long queryMemory;
        private final long taskMemory;
        private final Map<String, Long> operatorMemory;
        private final long revocableMemory;
        
        public MemorySnapshot(long queryMemory, long taskMemory, 
                            Map<String, Long> operatorMemory, long revocableMemory) {
            this.timestamp = System.currentTimeMillis();
            this.queryMemory = queryMemory;
            this.taskMemory = taskMemory;
            this.operatorMemory = new HashMap<>(operatorMemory);
            this.revocableMemory = revocableMemory;
        }
        
        public void print() {
            System.out.println("=== Memory Snapshot @ " + timestamp + " ===");
            System.out.println("Query Memory: " + formatBytes(queryMemory));
            System.out.println("Task Memory: " + formatBytes(taskMemory));
            System.out.println("Revocable Memory: " + formatBytes(revocableMemory));
            System.out.println("Operator Memory:");
            operatorMemory.forEach((op, mem) -> 
                System.out.println("  " + op + ": " + formatBytes(mem)));
        }
        
        private String formatBytes(long bytes) {
            if (bytes < 1024) return bytes + "B";
            if (bytes < 1024 * 1024) return (bytes / 1024) + "KB";
            return (bytes / 1024 / 1024) + "MB";
        }
    }
    
    public static void monitorMemoryUsage(Runnable workload) {
        List<MemorySnapshot> snapshots = new ArrayList<>();
        AtomicBoolean running = new AtomicBoolean(true);
        
        // 内存监控线程
        Thread monitor = new Thread(() -> {
            while (running.get()) {
                MemorySnapshot snapshot = takeSnapshot();
                snapshots.add(snapshot);
                
                try {
                    Thread.sleep(100); // 每100ms采样一次
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        monitor.start();
        
        // 执行工作负载
        try {
            workload.run();
        } finally {
            running.set(false);
            try {
                monitor.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // 分析内存使用模式
        analyzeMemoryPattern(snapshots);
    }
    
    private static MemorySnapshot takeSnapshot() {
        // 这里需要实际的内存上下文访问代码
        // 简化实现，返回模拟数据
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        
        Map<String, Long> operatorMemory = Map.of(
            "TableScan", usedMemory / 4,
            "HashJoin", usedMemory / 2,
            "Aggregation", usedMemory / 4
        );
        
        return new MemorySnapshot(usedMemory, usedMemory * 3 / 4, operatorMemory, usedMemory / 10);
    }
    
    private static void analyzeMemoryPattern(List<MemorySnapshot> snapshots) {
        System.out.println("\n=== Memory Usage Analysis ===");
        
        if (snapshots.isEmpty()) return;
        
        long maxQueryMemory = snapshots.stream()
            .mapToLong(s -> s.queryMemory)
            .max()
            .orElse(0);
            
        long avgQueryMemory = (long) snapshots.stream()
            .mapToLong(s -> s.queryMemory)
            .average()
            .orElse(0);
            
        System.out.println("Peak query memory: " + formatBytes(maxQueryMemory));
        System.out.println("Average query memory: " + formatBytes(avgQueryMemory));
        
        // 检测内存泄漏模式
        if (snapshots.size() > 10) {
            long startMemory = snapshots.get(0).queryMemory;
            long endMemory = snapshots.get(snapshots.size() - 1).queryMemory;
            
            if (endMemory > startMemory * 1.5) {
                System.out.println("⚠️ Potential memory leak detected!");
            }
        }
        
        // 打印内存使用趋势
        System.out.println("\nMemory usage trend (last 10 snapshots):");
        int start = Math.max(0, snapshots.size() - 10);
        for (int i = start; i < snapshots.size(); i++) {
            MemorySnapshot snapshot = snapshots.get(i);
            System.out.printf("T+%03d: %s (revocable: %s)%n", 
                i * 100, 
                formatBytes(snapshot.queryMemory),
                formatBytes(snapshot.revocableMemory));
        }
    }
    
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return (bytes / 1024) + "KB";
        return (bytes / 1024 / 1024) + "MB";
    }
}
```

#### 步骤2: 内存溢写触发实验
```java
// 内存溢写机制实验
public class SpillTriggerExperiment {
    
    public static void runSpillTest() {
        System.out.println("=== Memory Spill Trigger Test ===");
        
        // 模拟可撤销内存的算子 (如HashAggregation)
        MockSpillableOperator operator = new MockSpillableOperator();
        
        // 设置内存限制
        long memoryLimit = 100 * 1024 * 1024; // 100MB
        MockMemoryContext memoryContext = new MockMemoryContext(memoryLimit);
        
        operator.setMemoryContext(memoryContext);
        
        // 逐步增加内存使用，观察溢写触发
        for (int i = 1; i <= 20; i++) {
            long memoryToAllocate = 10 * 1024 * 1024; // 每次10MB
            
            System.out.printf("Iteration %d: Allocating %s (total: %s)%n", 
                i, 
                formatBytes(memoryToAllocate),
                formatBytes(i * memoryToAllocate));
            
            try {
                operator.allocateMemory(memoryToAllocate);
                
                System.out.printf("  Current memory: %s / %s (%.1f%%)%n",
                    formatBytes(memoryContext.getCurrentMemory()),
                    formatBytes(memoryLimit),
                    100.0 * memoryContext.getCurrentMemory() / memoryLimit);
                    
            } catch (MemoryLimitExceededException e) {
                System.out.println("  ❌ Memory limit exceeded: " + e.getMessage());
                
                // 触发溢写
                boolean spillSuccess = operator.triggerSpill();
                System.out.println("  💾 Spill triggered: " + spillSuccess);
                
                if (spillSuccess) {
                    System.out.printf("  ✅ Memory after spill: %s%n",
                        formatBytes(memoryContext.getCurrentMemory()));
                }
            }
        }
        
        // 打印最终统计
        operator.printSpillStats();
    }
    
    // 模拟支持溢写的算子
    private static class MockSpillableOperator {
        private long currentMemory = 0;
        private int spillCount = 0;
        private long totalSpilledBytes = 0;
        private MockMemoryContext memoryContext;
        
        public void setMemoryContext(MockMemoryContext context) {
            this.memoryContext = context;
        }
        
        public void allocateMemory(long bytes) {
            if (memoryContext.getCurrentMemory() + bytes > memoryContext.getLimit()) {
                throw new MemoryLimitExceededException("Cannot allocate " + bytes + " bytes");
            }
            
            currentMemory += bytes;
            memoryContext.setCurrentMemory(currentMemory);
        }
        
        public boolean triggerSpill() {
            if (currentMemory == 0) {
                return false;
            }
            
            // 模拟溢写操作：释放一半内存
            long spilledBytes = currentMemory / 2;
            currentMemory -= spilledBytes;
            memoryContext.setCurrentMemory(currentMemory);
            
            spillCount++;
            totalSpilledBytes += spilledBytes;
            
            System.out.printf("    Spilled %s to disk%n", formatBytes(spilledBytes));
            return true;
        }
        
        public void printSpillStats() {
            System.out.println("\n=== Spill Statistics ===");
            System.out.println("Spill count: " + spillCount);
            System.out.println("Total spilled: " + formatBytes(totalSpilledBytes));
            System.out.println("Final memory usage: " + formatBytes(currentMemory));
        }
    }
    
    // 模拟内存上下文
    private static class MockMemoryContext {
        private final long limit;
        private long currentMemory = 0;
        
        public MockMemoryContext(long limit) {
            this.limit = limit;
        }
        
        public long getLimit() { return limit; }
        public long getCurrentMemory() { return currentMemory; }
        public void setCurrentMemory(long memory) { this.currentMemory = memory; }
    }
    
    private static class MemoryLimitExceededException extends RuntimeException {
        public MemoryLimitExceededException(String message) {
            super(message);
        }
    }
    
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return (bytes / 1024) + "KB";  
        return (bytes / 1024 / 1024) + "MB";
    }
}
```

### 📊 内存管理实验结果

| 内存类型 | 分配量 | 峰值使用 | 溢写次数 | 溢写量 | 备注 |
|---------|--------|----------|----------|--------|------|
| 用户内存 |  |  |  |  |  |
| 系统内存 |  |  |  |  |  |
| 可撤销内存 |  |  |  |  |  |

---

## 实验5-8: 其他高级实验

由于篇幅限制，这里提供其余实验的大纲：

### 实验5: 背压机制模拟
- 创建高产出/低消费场景
- 测量背压传播时间
- 验证内存保护效果

### 实验6: 性能热点分析
- 使用JProfiler/async-profiler
- 识别CPU热点函数
- 分析内存分配模式
- 优化关键代码路径

### 实验7: 自定义算子开发
- 实现简单的过滤算子
- 添加统计信息收集
- 集成到Trino执行器
- 性能基准测试

### 实验8: 源码级性能调优
- 修改Page大小参数
- 优化Block实现
- 调整内存管理策略
- A/B测试性能差异

---

## 🎯 实验总结与进阶

完成这些源码级实验后，你将：

### ✅ **深度理解执行引擎**
- Page/Block数据结构的设计原理
- 算子间的数据流动机制
- Exchange系统的实现细节
- 内存管理的层次架构

### ✅ **具备源码调试能力**
- 使用工具追踪执行流程
- 识别和分析性能瓶颈
- 理解错误和异常的根因
- 验证优化效果

### ✅ **掌握底层优化技巧**
- 调整关键参数提升性能
- 理解不同硬件环境的影响
- 设计针对性的优化方案
- 评估优化的收益和风险

### 🚀 **进阶发展方向**
1. **贡献开源代码**: 修复bug、优化性能、添加新功能
2. **开发自定义扩展**: 连接器、算子、优化规则  
3. **企业级定制**: 针对特定业务场景的深度优化
4. **技术布道**: 分享源码级的技术洞察

通过这些深度的源码实验，你已经从Trino的使用者转变为深度贡献者！继续探索Trino的无限可能吧！🌟
