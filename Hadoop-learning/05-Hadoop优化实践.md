# Hadoop 优化实践

## 目录
1. [Hadoop 优化概述](#hadoop-优化概述)
2. [HDFS 优化实践](#hdfs-优化实践)
3. [MapReduce 优化实践](#mapreduce-优化实践)
4. [YARN 优化实践](#yarn-优化实践)
5. [系统级优化](#系统级优化)
6. [应用级优化](#应用级优化)
7. [监控与调优](#监控与调优)

## Hadoop 优化概述

### 优化目标

Hadoop 优化的主要目标包括：

1. **性能提升**：提高数据处理速度
2. **资源利用率**：提高集群资源利用率
3. **稳定性**：提高系统稳定性
4. **可扩展性**：支持更大规模的数据处理
5. **成本效益**：降低硬件和运维成本

### 优化层次

```
Hadoop 优化层次：
┌─────────────────────────────────────────────────────────────┐
│                   应用层优化                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  算法优化   │  │  代码优化   │  │  参数调优   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                   框架层优化                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ MapReduce   │  │    YARN     │  │    HDFS     │         │
│  │   优化      │  │    优化     │  │    优化     │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                   系统层优化                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   JVM 调优  │  │   网络优化   │  │   存储优化   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                   硬件层优化                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   CPU 优化  │  │   内存优化   │  │   磁盘优化   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

### 优化原则

1. **测量优先**：先测量再优化
2. **逐步优化**：逐步调整参数
3. **全面考虑**：考虑整体系统性能
4. **持续监控**：持续监控优化效果
5. **文档记录**：记录优化过程和结果

## HDFS 优化实践

### 1. 存储优化

#### 数据块大小优化
```xml
<!-- 根据数据特点调整数据块大小 -->
<property>
    <name>dfs.blocksize</name>
    <value>268435456</value> <!-- 256MB，适合大文件 -->
</property>

<!-- 小文件场景 -->
<property>
    <name>dfs.blocksize</name>
    <value>67108864</value> <!-- 64MB，适合小文件 -->
</property>
```

#### 副本策略优化
```xml
<!-- 根据数据重要性调整副本数 -->
<property>
    <name>dfs.replication</name>
    <value>3</value> <!-- 生产环境推荐3个副本 -->
</property>

<!-- 测试环境可以减少副本数 -->
<property>
    <name>dfs.replication</name>
    <value>2</value> <!-- 测试环境可以使用2个副本 -->
</property>
```

#### 压缩优化
```xml
<!-- 启用压缩存储 -->
<property>
    <name>dfs.compression.codec</name>
    <value>org.apache.hadoop.io.compress.SnappyCodec</value>
</property>

<!-- 启用压缩算法 -->
<property>
    <name>io.compression.codecs</name>
    <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.SnappyCodec</value>
</property>
```

### 2. 性能优化

#### 网络优化
```xml
<!-- 增加网络传输线程数 -->
<property>
    <name>dfs.datanode.max.transfer.threads</name>
    <value>4096</value>
</property>

<!-- 启用短路读取 -->
<property>
    <name>dfs.client.read.shortcircuit</name>
    <value>true</value>
</property>

<!-- 短路读取缓存大小 -->
<property>
    <name>dfs.client.read.shortcircuit.streams.cache.size</name>
    <value>40960</value>
</property>
```

#### 内存优化
```xml
<!-- 增加数据节点处理线程数 -->
<property>
    <name>dfs.datanode.handler.count</name>
    <value>200</value>
</property>

<!-- 增加名称节点处理线程数 -->
<property>
    <name>dfs.namenode.handler.count</name>
    <value>192</value>
</property>

<!-- 启用内存映射 -->
<property>
    <name>dfs.datanode.fsdataset.volume.choosing.policy</name>
    <value>org.apache.hadoop.hdfs.server.datanode.fsdataset.AvailableSpaceVolumeChoosingPolicy</value>
</property>
```

### 3. 高可用性优化

#### HA 配置优化
```xml
<!-- 启用自动故障转移 -->
<property>
    <name>dfs.ha.automatic-failover.enabled</name>
    <value>true</value>
</property>

<!-- 故障转移超时时间 -->
<property>
    <name>ha.health-monitor.rpc-timeout.ms</name>
    <value>180000</value>
</property>

<!-- 健康检查间隔 -->
<property>
    <name>ha.health-monitor.check-interval.ms</name>
    <value>2000</value>
</property>
```

#### 负载均衡优化
```xml
<!-- 启用负载均衡 -->
<property>
    <name>dfs.balancer.dispatcherThreads</name>
    <value>5000</value>
</property>

<!-- 负载均衡带宽 -->
<property>
    <name>dfs.datanode.balance.bandwidthPerSec</name>
    <value>52428800</value> <!-- 50MB/s -->
</property>
```

## MapReduce 优化实践

### 1. 作业级优化

#### 输入优化
```java
// 自定义输入格式
public class CustomInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new CustomRecordReader();
    }
}

// 设置输入格式
job.setInputFormatClass(CustomInputFormat.class);
```

#### 输出优化
```java
// 启用输出压缩
FileOutputFormat.setCompressOutput(job, true);
FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

// 设置输出格式
job.setOutputFormatClass(TextOutputFormat.class);
```

### 2. Map 阶段优化

#### Map 任务优化
```xml
<!-- Map 任务内存配置 -->
<property>
    <name>mapreduce.map.memory.mb</name>
    <value>2048</value>
</property>

<!-- Map 任务 CPU 配置 -->
<property>
    <name>mapreduce.map.cpu.vcores</name>
    <value>2</value>
</property>

<!-- Map 输出缓冲区大小 -->
<property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>200</value>
</property>
```

#### Combiner 优化
```java
// 使用 Combiner 减少网络传输
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

// 设置 Combiner
job.setCombinerClass(WordCountCombiner.class);
```

### 3. Reduce 阶段优化

#### Reduce 任务优化
```xml
<!-- Reduce 任务内存配置 -->
<property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>4096</value>
</property>

<!-- Reduce 任务 CPU 配置 -->
<property>
    <name>mapreduce.reduce.cpu.vcores</name>
    <value>4</value>
</property>

<!-- Reduce 任务数量 -->
<property>
    <name>mapreduce.job.reduces</name>
    <value>10</value>
</property>
```

#### 分区优化
```java
// 自定义分区器
public class CustomPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        // 根据业务逻辑分区
        return Math.abs(key.hashCode()) % numPartitions;
    }
}

// 设置分区器
job.setPartitionerClass(CustomPartitioner.class);
```

### 4. 网络优化

#### 压缩优化
```xml
<!-- 启用 Map 输出压缩 -->
<property>
    <name>mapreduce.map.output.compress</name>
    <value>true</value>
</property>

<!-- 设置压缩编解码器 -->
<property>
    <name>mapreduce.map.output.compress.codec</name>
    <value>org.apache.hadoop.io.compress.SnappyCodec</value>
</property>
```

#### 网络传输优化
```xml
<!-- 网络传输缓冲区大小 -->
<property>
    <name>mapreduce.task.io.sort.factor</name>
    <value>100</value>
</property>

<!-- 网络传输超时时间 -->
<property>
    <name>mapreduce.task.timeout</name>
    <value>600000</value>
</property>
```

## YARN 优化实践

### 1. 资源管理优化

#### 资源分配优化
```xml
<!-- 最小容器内存 -->
<property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>128</value>
</property>

<!-- 最大容器内存 -->
<property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>8192</value>
</property>

<!-- 容器内存增量 -->
<property>
    <name>yarn.scheduler.increment-allocation-mb</name>
    <value>128</value>
</property>
```

#### 队列配置优化
```xml
<!-- 队列资源配置 -->
<property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>40</value>
</property>

<property>
    <name>yarn.scheduler.capacity.root.production.capacity</name>
    <value>40</value>
</property>

<property>
    <name>yarn.scheduler.capacity.root.development.capacity</name>
    <value>20</value>
</property>
```

### 2. 调度器优化

#### Capacity 调度器优化
```xml
<!-- 启用用户限制 -->
<property>
    <name>yarn.scheduler.capacity.root.default.user-limit-factor</name>
    <value>1.0</value>
</property>

<!-- 启用抢占 -->
<property>
    <name>yarn.scheduler.capacity.root.default.allow-preemption</name>
    <value>true</value>
</property>

<!-- 设置最大应用数 -->
<property>
    <name>yarn.scheduler.capacity.root.default.maximum-applications</name>
    <value>10000</value>
</property>
```

#### Fair 调度器优化
```xml
<!-- 启用抢占 -->
<property>
    <name>yarn.scheduler.fair.preemption</name>
    <value>true</value>
</property>

<!-- 抢占超时时间 -->
<property>
    <name>yarn.scheduler.fair.preemption-timeout</name>
    <value>60</value>
</property>
```

### 3. 容器优化

#### 容器配置优化
```xml
<!-- 容器内存限制 -->
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>8192</value>
</property>

<!-- 容器CPU限制 -->
<property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>8</value>
</property>

<!-- 容器磁盘限制 -->
<property>
    <name>yarn.nodemanager.resource.disk-mb</name>
    <value>100000</value>
</property>
```

#### 容器生命周期优化
```xml
<!-- 容器清理间隔 -->
<property>
    <name>yarn.nodemanager.localizer.cache.cleanup.interval-ms</name>
    <value>600000</value>
</property>

<!-- 容器超时时间 -->
<property>
    <name>yarn.nodemanager.container-executor.class</name>
    <value>org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor</value>
</property>
```

## 系统级优化

### 1. JVM 优化

#### 内存配置
```bash
# NameNode JVM 配置
export HADOOP_NAMENODE_OPTS="-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# DataNode JVM 配置
export HADOOP_DATANODE_OPTS="-Xmx2g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# ResourceManager JVM 配置
export YARN_RESOURCEMANAGER_OPTS="-Xmx4g -Xms4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# NodeManager JVM 配置
export YARN_NODEMANAGER_OPTS="-Xmx2g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

#### GC 优化
```bash
# 启用 G1 垃圾收集器
-XX:+UseG1GC

# 设置最大 GC 暂停时间
-XX:MaxGCPauseMillis=200

# 启用并行 GC
-XX:+UseParallelGC

# 设置 GC 线程数
-XX:ParallelGCThreads=8
```

### 2. 网络优化

#### 网络配置
```bash
# 增加网络缓冲区大小
echo 'net.core.rmem_max = 16777216' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 16777216' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem = 4096 87380 16777216' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem = 4096 65536 16777216' >> /etc/sysctl.conf

# 应用配置
sysctl -p
```

#### 网络拓扑优化
```xml
<!-- 网络拓扑脚本 -->
<property>
    <name>net.topology.script.file.name</name>
    <value>/etc/hadoop/topology_script.py</value>
</property>

<!-- 网络拓扑节点映射 -->
<property>
    <name>net.topology.node.switch.mapping.impl</name>
    <value>org.apache.hadoop.net.ScriptBasedMapping</value>
</property>
```

### 3. 存储优化

#### 磁盘配置
```bash
# 使用 SSD 存储
<property>
    <name>dfs.datanode.data.dir</name>
    <value>[SSD]file:///mnt/ssd/0/hdfs/data</value>
</property>

# 启用磁盘健康检查
<property>
    <name>dfs.datanode.disk.checker.enabled</name>
    <value>true</value>
</property>
```

#### 文件系统优化
```bash
# 调整文件系统参数
echo 'vm.swappiness = 10' >> /etc/sysctl.conf
echo 'vm.dirty_ratio = 15' >> /etc/sysctl.conf
echo 'vm.dirty_background_ratio = 5' >> /etc/sysctl.conf

# 应用配置
sysctl -p
```

## 应用级优化

### 1. 算法优化

#### 数据倾斜处理
```java
// 数据倾斜检测和处理
public class SkewHandler {
    
    // 检测数据倾斜
    public boolean detectSkew(Text key, Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable val : values) {
            count++;
        }
        return count > SKEW_THRESHOLD;
    }
    
    // 处理数据倾斜
    public void handleSkew(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        // 使用随机前缀分散数据
        Random random = new Random();
        for (IntWritable val : values) {
            Text newKey = new Text(random.nextInt(10) + "_" + key.toString());
            context.write(newKey, val);
        }
    }
}
```

#### 内存优化
```java
// 使用对象池减少 GC 压力
public class ObjectPool {
    private Queue<Text> textPool = new ConcurrentLinkedQueue<>();
    private Queue<IntWritable> intPool = new ConcurrentLinkedQueue<>();
    
    public Text getText() {
        Text text = textPool.poll();
        return text != null ? text : new Text();
    }
    
    public void returnText(Text text) {
        text.clear();
        textPool.offer(text);
    }
}
```

### 2. 数据结构优化

#### 自定义数据类型
```java
// 自定义 Writable 类型
public class CustomWritable implements Writable {
    private int value1;
    private long value2;
    private String value3;
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(value1);
        out.writeLong(value2);
        out.writeUTF(value3);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        value1 = in.readInt();
        value2 = in.readLong();
        value3 = in.readUTF();
    }
}
```

#### 序列化优化
```java
// 使用更高效的序列化方式
public class OptimizedMapper extends Mapper<Text, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private final Text word = new Text();
    
    @Override
    public void map(Text key, Text value, Context context) 
            throws IOException, InterruptedException {
        // 重用对象，减少 GC 压力
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}
```

### 3. 并行度优化

#### 任务并行度调整
```java
// 动态调整任务并行度
public class DynamicParallelism {
    
    public void adjustParallelism(Job job, long inputSize) {
        // 根据输入数据大小调整并行度
        int numMaps = (int) Math.ceil(inputSize / (128 * 1024 * 1024)); // 128MB per map
        int numReduces = Math.min(numMaps, 100); // 最多100个reduce任务
        
        job.setNumReduceTasks(numReduces);
    }
}
```

#### 资源分配优化
```java
// 根据任务特点分配资源
public class ResourceAllocation {
    
    public void allocateResources(Job job, TaskType taskType) {
        switch (taskType) {
            case CPU_INTENSIVE:
                job.getConfiguration().setInt("mapreduce.map.memory.mb", 4096);
                job.getConfiguration().setInt("mapreduce.map.cpu.vcores", 4);
                break;
            case MEMORY_INTENSIVE:
                job.getConfiguration().setInt("mapreduce.map.memory.mb", 8192);
                job.getConfiguration().setInt("mapreduce.map.cpu.vcores", 2);
                break;
            case IO_INTENSIVE:
                job.getConfiguration().setInt("mapreduce.map.memory.mb", 2048);
                job.getConfiguration().setInt("mapreduce.map.cpu.vcores", 1);
                break;
        }
    }
}
```

## 监控与调优

### 1. 性能监控

#### 关键指标监控
```bash
#!/bin/bash
# Hadoop 性能监控脚本

# 检查 HDFS 状态
hdfs dfsadmin -report | grep "DFS Used"

# 检查 YARN 资源使用
yarn node -list | grep "RUNNING"

# 检查应用状态
yarn application -list | grep "RUNNING"

# 检查磁盘使用率
df -h | grep "/data"

# 检查内存使用率
free -h

# 检查 CPU 使用率
top -bn1 | grep "Cpu(s)"
```

#### 监控工具配置
```xml
<!-- 启用监控指标 -->
<property>
    <name>yarn.resourcemanager.metrics.enabled</name>
    <value>true</value>
</property>

<!-- 监控指标输出 -->
<property>
    <name>yarn.resourcemanager.metrics.address</name>
    <value>0.0.0.0:8033</value>
</property>
```

### 2. 调优策略

#### 性能调优流程
```
性能调优流程：
┌─────────────────────────────────────────────────────────────┐
│                   性能调优流程                               │
├─────────────────────────────────────────────────────────────┤
│  1. 性能测试                                                │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│     │  基准测试   │───▶│  压力测试   │───▶│  性能分析   │  │
│     │  功能测试   │    │  稳定性测试  │    │  瓶颈识别   │  │
│     └─────────────┘    └─────────────┘    └─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  2. 参数调优                                                │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│     │  系统参数   │───▶│  应用参数   │───▶│  验证测试   │  │
│     │  框架参数   │    │  业务参数   │    │  效果评估   │  │
│     └─────────────┘    └─────────────┘    └─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  3. 持续优化                                                │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│     │  监控部署   │───▶│  性能监控   │───▶│  持续调优   │  │
│     │  告警配置   │    │  趋势分析   │    │  效果跟踪   │  │
│     └─────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

#### 调优检查清单
```bash
# 系统级调优检查
echo "=== 系统级调优检查 ==="
echo "1. JVM 参数配置"
ps aux | grep java | grep -E "(NameNode|DataNode|ResourceManager|NodeManager)"

echo "2. 网络配置"
sysctl net.core.rmem_max net.core.wmem_max

echo "3. 磁盘配置"
df -h | grep "/data"

echo "4. 内存配置"
free -h

# 应用级调优检查
echo "=== 应用级调优检查 ==="
echo "1. HDFS 配置"
hdfs dfsadmin -report | head -20

echo "2. YARN 配置"
yarn node -list | head -10

echo "3. 应用状态"
yarn application -list | head -10
```

### 3. 故障排查

#### 常见问题排查
```bash
# 检查日志文件
tail -f /var/log/hadoop/hdfs/hadoop-hdfs-namenode-*.log
tail -f /var/log/hadoop/yarn/yarn-resourcemanager-*.log

# 检查进程状态
jps | grep -E "(NameNode|DataNode|ResourceManager|NodeManager)"

# 检查端口状态
netstat -tlnp | grep -E "(8020|8032|8042|8088)"

# 检查磁盘空间
df -h | grep "/data"

# 检查内存使用
free -h
```

#### 性能问题诊断
```bash
# 检查 HDFS 性能
hdfs dfsadmin -report | grep -E "(DFS Used|DFS Remaining|DFS Used%)"

# 检查 YARN 性能
yarn node -list | grep -E "(Memory|VCores|Containers)"

# 检查应用性能
yarn application -list | grep -E "(State|Progress|FinalStatus)"
```

## 总结

Hadoop 优化是一个系统性的工程，需要从多个层面进行综合考虑。通过合理的配置优化、性能调优和监控运维，可以显著提高 Hadoop 集群的性能和稳定性。

在实际应用中，需要根据具体的业务需求、数据特点和硬件环境，制定合适的优化策略，并持续监控和调整，以达到最佳的性能效果。
