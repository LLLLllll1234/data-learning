# YARN 资源管理详解

## 目录
1. [YARN 概述](#yarn-概述)
2. [YARN 架构设计](#yarn-架构设计)
3. [YARN 核心组件](#yarn-核心组件)
4. [YARN 资源调度](#yarn-资源调度)
5. [YARN 应用管理](#yarn-应用管理)
6. [YARN 配置优化](#yarn-配置优化)
7. [YARN 监控运维](#yarn-监控运维)

## YARN 概述

### 什么是 YARN？

YARN (Yet Another Resource Negotiator) 是 Hadoop 2.0 引入的资源管理系统，它将资源管理和作业调度分离，为 Hadoop 生态系统提供了更灵活、更高效的资源管理能力。

### YARN 设计目标

1. **资源统一管理**：统一管理集群中的所有资源
2. **多应用支持**：支持多种计算框架同时运行
3. **可扩展性**：支持大规模集群扩展
4. **高可用性**：提供高可用的资源管理服务
5. **灵活性**：支持不同的调度策略

### YARN 解决的问题

#### Hadoop 1.0 的局限性
- **单点故障**：JobTracker 是单点故障
- **资源利用率低**：资源分配不够灵活
- **扩展性差**：难以支持大规模集群
- **多框架支持差**：只支持 MapReduce

#### YARN 的改进
- **高可用性**：ResourceManager 支持 HA
- **资源利用率高**：动态资源分配
- **扩展性好**：支持数千个节点
- **多框架支持**：支持 Spark、Flink 等

## YARN 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    YARN 架构图                              │
├─────────────────────────────────────────────────────────────┤
│  客户端 (Client)                                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Spark     │  │ MapReduce   │  │   Flink     │         │
│  │  Client     │  │  Client     │  │  Client     │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  资源管理层 (Resource Management)                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ResourceMgr  │  │ResourceMgr  │  │  Zookeeper  │         │
│  │  (Active)   │  │ (Standby)   │  │   (HA)      │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  应用管理层 (Application Management)                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Spark     │  │ MapReduce   │  │   Flink     │         │
│  │  AppMaster  │  │  AppMaster  │  │  AppMaster  │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  节点管理层 (Node Management)                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ NodeManager │  │ NodeManager │  │ NodeManager │         │
│  │   Node 1    │  │   Node 2    │  │   Node N    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

### 分层架构

#### 1. 客户端层
- **应用客户端**：提交应用程序
- **资源请求**：请求集群资源
- **状态监控**：监控应用状态

#### 2. 资源管理层
- **ResourceManager**：全局资源管理器
- **高可用性**：支持主备模式
- **调度策略**：实现不同的调度算法

#### 3. 应用管理层
- **ApplicationMaster**：应用主控程序
- **任务调度**：调度应用任务
- **资源请求**：向 ResourceManager 请求资源

#### 4. 节点管理层
- **NodeManager**：节点资源管理器
- **容器管理**：管理本地容器
- **资源监控**：监控节点资源使用

## YARN 核心组件

### 1. ResourceManager

ResourceManager 是 YARN 的主控节点，负责：

#### 主要功能
- **资源管理**：管理集群中的所有资源
- **应用调度**：调度应用程序
- **节点管理**：管理 NodeManager
- **安全控制**：控制资源访问权限

#### 架构设计
```
ResourceManager 架构：
┌─────────────────────────────────────────────────────────────┐
│                    ResourceManager                          │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Scheduler │  │  Application │  │   Security  │         │
│  │             │  │   Manager    │  │   Manager   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Node      │  │   Resource  │  │   Web UI    │         │
│  │  Manager    │  │   Monitor   │  │  Interface  │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

#### 核心组件
- **Scheduler**：资源调度器
- **ApplicationManager**：应用管理器
- **NodeManager**：节点管理器
- **SecurityManager**：安全管理器

### 2. NodeManager

NodeManager 是 YARN 的工作节点，负责：

#### 主要功能
- **资源监控**：监控节点资源使用情况
- **容器管理**：管理本地容器
- **任务执行**：执行应用程序任务
- **心跳报告**：向 ResourceManager 报告状态

#### 架构设计
```
NodeManager 架构：
┌─────────────────────────────────────────────────────────────┐
│                    NodeManager                              │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Container │  │   Resource  │  │   Heartbeat │         │
│  │   Manager   │  │   Monitor   │  │   Sender    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Local     │  │   Task      │  │   JVM       │         │
│  │   Storage   │  │  Executor   │  │  Manager    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

#### 核心组件
- **ContainerManager**：容器管理器
- **ResourceMonitor**：资源监控器
- **HeartbeatSender**：心跳发送器
- **TaskExecutor**：任务执行器

### 3. ApplicationMaster

ApplicationMaster 是应用程序的主控程序，负责：

#### 主要功能
- **资源请求**：向 ResourceManager 请求资源
- **任务调度**：调度应用程序任务
- **任务监控**：监控任务执行状态
- **故障处理**：处理任务故障

#### 架构设计
```
ApplicationMaster 架构：
┌─────────────────────────────────────────────────────────────┐
│                    ApplicationMaster                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Resource  │  │   Task      │  │   Fault     │         │
│  │  Requester  │  │  Scheduler  │  │  Handler    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Task      │  │   Status    │  │   Cleanup   │         │
│  │  Monitor    │  │  Reporter   │  │   Manager   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

#### 核心组件
- **ResourceRequester**：资源请求器
- **TaskScheduler**：任务调度器
- **FaultHandler**：故障处理器
- **TaskMonitor**：任务监控器

## YARN 资源调度

### 1. 调度器类型

#### FIFO 调度器
- **特点**：先进先出，简单易用
- **适用场景**：小规模集群，简单应用
- **缺点**：资源利用率低，不支持优先级

#### Capacity 调度器
- **特点**：基于队列的调度，支持多租户
- **适用场景**：多用户、多应用场景
- **优点**：资源隔离，支持优先级

#### Fair 调度器
- **特点**：公平分配资源，支持动态调整
- **适用场景**：多用户、多应用场景
- **优点**：资源公平分配，支持抢占

### 2. 调度策略

#### 资源分配策略
```
资源分配策略：
┌─────────────────────────────────────────────────────────────┐
│                   资源分配策略                               │
├─────────────────────────────────────────────────────────────┤
│  1. 资源请求处理                                            │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│     │  应用提交   │───▶│  资源请求   │───▶│  调度器处理  │  │
│     │  资源需求   │    │  资源规格   │    │  资源分配   │  │
│     └─────────────┘    └─────────────┘    └─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  2. 资源分配决策                                            │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│     │  队列优先级  │───▶│  资源可用性  │───▶│  分配决策   │  │
│     │  用户优先级  │    │  节点状态   │    │  资源分配   │  │
│     └─────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

#### 队列管理
```xml
<!-- Capacity 调度器配置 -->
<property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default,production,development</value>
</property>

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

### 3. 资源模型

#### 资源类型
- **CPU**：CPU 核心数
- **内存**：内存大小
- **磁盘**：磁盘空间
- **网络**：网络带宽

#### 资源单位
- **Container**：资源容器
- **Resource**：资源规格
- **Allocation**：资源分配

#### 资源请求示例
```java
// 资源请求示例
Resource resource = Resource.newInstance(1024, 1); // 1GB内存，1个CPU核心
ContainerRequest request = new ContainerRequest(
    resource, null, null, Priority.newInstance(1), true);
```

## YARN 应用管理

### 1. 应用生命周期

#### 应用状态
- **NEW**：新应用
- **NEW_SAVING**：保存中
- **SUBMITTED**：已提交
- **ACCEPTED**：已接受
- **RUNNING**：运行中
- **FINISHED**：已完成
- **FAILED**：失败
- **KILLED**：已杀死

#### 应用状态转换
```
应用状态转换：
┌─────────────────────────────────────────────────────────────┐
│                   应用状态转换                               │
├─────────────────────────────────────────────────────────────┤
│  NEW ──┐                                                    │
│        │                                                    │
│        ▼                                                    │
│  NEW_SAVING ──┐                                             │
│               │                                             │
│               ▼                                             │
│  SUBMITTED ──┐                                              │
│              │                                              │
│              ▼                                              │
│  ACCEPTED ──┐                                               │
│             │                                               │
│             ▼                                               │
│  RUNNING ──┐                                                │
│            │                                                │
│            ▼                                                │
│  FINISHED/FAILED/KILLED                                     │
└─────────────────────────────────────────────────────────────┘
```

### 2. 容器管理

#### 容器生命周期
- **NEW**：新容器
- **LOCALIZING**：本地化中
- **RUNNING**：运行中
- **COMPLETE**：已完成
- **FAILED**：失败
- **KILLED**：已杀死

#### 容器管理流程
```
容器管理流程：
┌─────────────────────────────────────────────────────────────┐
│                   容器管理流程                               │
├─────────────────────────────────────────────────────────────┤
│  1. 容器创建                                                │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│     │  资源请求   │───▶│  容器分配   │───▶│  容器创建   │  │
│     │  资源规格   │    │  资源分配   │    │  容器启动   │  │
│     └─────────────┘    └─────────────┘    └─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│  2. 容器执行                                                │
│     ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│     │  任务执行   │───▶│  状态监控   │───▶│  结果返回   │  │
│     │  资源使用   │    │  故障处理   │    │  容器清理   │  │
│     └─────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 3. 故障处理

#### 故障类型
- **节点故障**：NodeManager 故障
- **容器故障**：容器执行失败
- **应用故障**：ApplicationMaster 故障
- **资源故障**：资源不足或不可用

#### 故障处理策略
- **重试机制**：自动重试失败的任务
- **故障转移**：将任务转移到其他节点
- **资源回收**：回收故障节点的资源
- **状态恢复**：恢复应用状态

## YARN 配置优化

### 1. 基础配置

#### ResourceManager 配置
```xml
<!-- ResourceManager 地址 -->
<property>
    <name>yarn.resourcemanager.hostname</name>
    <value>resourcemanager</value>
</property>

<!-- ResourceManager 端口 -->
<property>
    <name>yarn.resourcemanager.address</name>
    <value>resourcemanager:8032</value>
</property>

<!-- 调度器类型 -->
<property>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
</property>
```

#### NodeManager 配置
```xml
<!-- NodeManager 地址 -->
<property>
    <name>yarn.nodemanager.hostname</name>
    <value>nodemanager</value>
</property>

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
```

### 2. 性能优化配置

#### 内存配置
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

#### CPU 配置
```xml
<!-- 最小容器CPU -->
<property>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
</property>

<!-- 最大容器CPU -->
<property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>8</value>
</property>

<!-- 容器CPU增量 -->
<property>
    <name>yarn.scheduler.increment-allocation-vcores</name>
    <value>1</value>
</property>
```

### 3. 高可用性配置

#### HA 配置
```xml
<!-- 启用HA -->
<property>
    <name>yarn.resourcemanager.ha.enabled</name>
    <value>true</value>
</property>

<!-- RM 集群ID -->
<property>
    <name>yarn.resourcemanager.cluster-id</name>
    <value>yarn-cluster</value>
</property>

<!-- RM 节点列表 -->
<property>
    <name>yarn.resourcemanager.ha.rm-ids</name>
    <value>rm1,rm2</value>
</property>
```

#### 故障转移配置
```xml>
<!-- 故障转移提供者 -->
<property>
    <name>yarn.client.failover-proxy-provider</name>
    <value>org.apache.hadoop.yarn.client.RMFailoverProxyProvider</value>
</property>

<!-- 自动故障转移 -->
<property>
    <name>yarn.resourcemanager.ha.automatic-failover.enabled</name>
    <value>true</value>
</property>
```

## YARN 监控运维

### 1. 监控指标

#### 集群监控指标
- **资源使用率**：CPU、内存、磁盘使用率
- **应用数量**：运行中的应用数量
- **队列状态**：队列资源使用情况
- **节点状态**：节点健康状态

#### 应用监控指标
- **应用状态**：应用运行状态
- **资源使用**：应用资源使用情况
- **任务进度**：任务执行进度
- **错误日志**：应用错误信息

### 2. 监控工具

#### Web UI
- **ResourceManager Web UI**：集群资源监控
- **NodeManager Web UI**：节点资源监控
- **Application Web UI**：应用状态监控

#### 命令行工具
```bash
# 查看集群状态
yarn node -list

# 查看应用状态
yarn application -list

# 查看应用详情
yarn application -status <application_id>

# 杀死应用
yarn application -kill <application_id>
```

#### 监控脚本
```bash
#!/bin/bash
# YARN 监控脚本

# 检查 ResourceManager 状态
yarn node -list | grep -c "RUNNING"

# 检查应用状态
yarn application -list | grep -c "RUNNING"

# 检查资源使用率
yarn node -list | awk '{print $4}' | grep -o '[0-9]*%' | sort -n | tail -1
```

### 3. 故障排查

#### 常见问题
- **资源不足**：调整资源分配策略
- **应用失败**：检查应用日志和配置
- **节点故障**：检查节点状态和网络
- **调度问题**：调整调度器配置

#### 排查步骤
1. **检查日志**：查看相关组件日志
2. **检查状态**：检查集群和节点状态
3. **检查配置**：验证配置文件
4. **检查资源**：检查资源使用情况

## 总结

YARN 作为 Hadoop 2.0 的核心组件，提供了强大的资源管理能力。通过深入理解其架构设计、核心组件和配置优化，可以更好地管理和运维 YARN 集群，提高资源利用率和系统稳定性。

在实际应用中，需要根据具体的业务需求和集群规模，合理配置 YARN 参数，并建立完善的监控和运维体系。
