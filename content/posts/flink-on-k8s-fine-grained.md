# 深入理解 Flink on Kubernetes（三）：细粒度资源管理从设计到实现

> 本文是 Flink on K8s 系列的第三篇。在[第一篇](flink-on-k8s.md)中我们剖析了 Flink Native K8s 的启动全流程和 ResourceManagerDriver 的分层抽象；[第二篇](flink-on-k8s-gpu.md)中我们深入了 GPU 加速的端到端工作流，但也在 [4.4 节](flink-on-k8s-gpu.md#44-静态分配的浪费问题) 中暴露了一个核心痛点——所有 TaskManager 被统一配置相同的资源规格，GPU 等昂贵资源被大量浪费。本文将以此为起点，深入 **FLIP-56（Dynamic Slot Allocation）** 和 **FLIP-156（Fine-Grained Resource Requirements）** 的设计与实现，看 Flink 如何从"千人一面"走向"按需定制"的资源管理。

---

## 第一部分：粗粒度模型的困境

### 1.1 静态 Slot 模型回顾

在 Flink 的传统资源模型中，每个 TaskManager 在启动时被切分成固定数量的 slot，每个 slot 的资源完全相同。这个数量由 `taskmanager.numberOfTaskSlots` 决定：

```
TaskManager（总资源：8 CPU, 32GB Memory, 1 GPU）
┌─────────────┬─────────────┬─────────────┬─────────────┐
│   Slot 0    │   Slot 1    │   Slot 2    │   Slot 3    │
│  2 CPU      │  2 CPU      │  2 CPU      │  2 CPU      │
│  8GB Mem    │  8GB Mem    │  8GB Mem    │  8GB Mem    │
│  0.25 GPU   │  0.25 GPU   │  0.25 GPU   │  0.25 GPU   │
└─────────────┴─────────────┴─────────────┴─────────────┘
          ↑ 所有 Slot 规格完全相同，启动时就固定
```

这个模型简单清晰，但在真实的生产场景中会造成三类浪费：

**浪费一：并行度不对齐**。一个 Job 的不同算子通常有不同的并行度。例如 Source 并行度为 2，Map 为 8，Sink 为 4。当使用 Slot Sharing 时，某些 Slot 中运行了完整的算子链（Source → Map → Sink），而另一些只运行了 Map。后者消耗的资源远低于前者，但分到的 slot 资源是一样的。

**浪费二：资源需求异构**。不同算子的资源特征天差地别——一个文本解析算子可能只需要 0.5 CPU 和 256MB 内存，而一个 GPU 推理算子可能需要 2 CPU、8GB 内存和 1 块 GPU。静态 slot 必须按照最大需求来配置，导致轻量算子的 slot 大量空闲。

**浪费三：Batch 场景的潮汐效应**。Batch 作业的不同阶段资源需求差异巨大——Shuffle 阶段需要大量网络和磁盘 IO，而计算阶段需要大量 CPU。固定 slot 无法根据执行阶段动态调整。

### 1.2 问题的本质

用一句话概括：**资源分配的粒度（TaskManager 级别）和资源使用的粒度（算子级别）之间存在鸿沟**。粗粒度模型用一个静态的、统一的资源规格去套所有算子，必然导致"削足适履"或"杀鸡用牛刀"。

```
理想状态：                          现实状况：
每个算子按需分配资源                  所有 Slot 被迫使用同一规格

┌──────┐ ┌────────────┐ ┌────┐    ┌──────────┐ ┌──────────┐ ┌──────────┐
│Source│ │GPU Inference│ │Sink│    │  Slot 0  │ │  Slot 1  │ │  Slot 2  │
│0.5CPU│ │ 2CPU + 1GPU│ │1CPU│    │ 2CPU+GPU │ │ 2CPU+GPU │ │ 2CPU+GPU │
│256MB │ │ 8GB        │ │1GB │    │ 8GB      │ │ 8GB      │ │ 8GB      │
└──────┘ └────────────┘ └────┘    └──────────┘ └──────────┘ └──────────┘
  精确匹配，零浪费                    Source 和 Sink 的 GPU 完全浪费
```

---

## 第二部分：FLIP-56——从静态 Slot 到动态 Slot

### 2.1 核心思想

[FLIP-56](https://cwiki.apache.org/confluence/display/FLINK/FLIP-56%3A+Dynamic+Slot+Allocation) 对 Slot 模型做了一个根本性的改变：**TaskManager 启动时不再预切 Slot，而是持有一整块可用资源；当 ResourceManager 请求 Slot 时，TaskManager 从可用资源中动态切出一块恰好满足需求的 Slot；Slot 释放后，资源归还给可用池**。

```
静态模型（改造前）：
TaskManager 启动时切好 4 个 Slot
┌──────────┬──────────┬──────────┬──────────┐
│  Slot 0  │  Slot 1  │  Slot 2  │  Slot 3  │
│ (固定)    │ (固定)    │ (固定)    │ (固定)    │
└──────────┴──────────┴──────────┴──────────┘


动态模型（FLIP-56）：
TaskManager 持有可用资源池，按需切割
┌──────────────────────────────────────────┐
│            Available Resources            │
│         8 CPU, 32GB Memory, 1 GPU        │
└──────────────────────────────────────────┘
                    │
          RM 请求不同规格的 Slot
                    │
         ┌──────────┼───────────┐
         ▼          ▼           ▼
    ┌─────────┐ ┌────────────┐ ┌──────┐
    │ Slot A  │ │  Slot B    │ │Slot C│    剩余可用:
    │ 0.5 CPU │ │ 2 CPU      │ │1 CPU │    4.5 CPU
    │ 256MB   │ │ 8GB + 1GPU │ │ 1GB  │    22.75GB
    └─────────┘ └────────────┘ └──────┘    0 GPU
        ↑            ↑            ↑
      Source    GPU Inference    Sink
```

### 2.2 协议层变化

这个设计改变了 ResourceManager 与 TaskManager 之间的交互协议。原来 RM 请求的是一个"Slot ID"（预定义的格子），现在请求的是一个"ResourceProfile"（资源规格描述）：

```java
// 改造前：请求特定的 Slot（按编号）
interface TaskExecutorGateway {
    CompletableFuture<Acknowledge> requestSlot(
        SlotID slotId,          // 固定的 Slot 编号
        JobID jobId,
        AllocationID allocationId,
        ResourceManagerId rmId);
}

// 改造后：请求特定规格的资源（按需切割）
interface TaskExecutorGateway {
    CompletableFuture<Acknowledge> requestSlot(
        SlotID slotId,
        JobID jobId,
        AllocationID allocationId,
        ResourceProfile resourceProfile,  // 需要多少资源
        ResourceManagerId rmId);
}
```

相应地，TaskManager 向 RM 汇报的内容也从"我有哪些固定 Slot"变成了"我有多少可用资源"：

```java
// 改造前的 SlotReport：
// [Slot-0: FREE, Slot-1: ALLOCATED(job-1), Slot-2: FREE, Slot-3: FREE]

// 改造后的 SlotReport：
// allocated: [{allocationId=xxx, jobId=job-1, resourceProfile={2CPU,8GB,1GPU}}]
// availableResources: {5.5CPU, 23.75GB, 0GPU}
```

### 2.3 SlotManager 的重新设计

SlotManager 是 ResourceManager 内部负责 Slot 分配的核心组件。在动态模型下，它的职责发生了根本变化：

```
静态模型下的 SlotManager：
┌─────────────────────────────────────────────┐
│  维护一张 "Slot 表"                           │
│  {TM-1:Slot-0:FREE, TM-1:Slot-1:ALLOCATED,  │
│   TM-2:Slot-0:FREE, TM-2:Slot-1:FREE, ...}  │
│                                              │
│  分配逻辑：找一个 FREE 的 Slot，标记为 ALLOCATED │
└─────────────────────────────────────────────┘


动态模型下的 SlotManager：
┌──────────────────────────────────────────────────┐
│  维护一张 "可用资源表"                               │
│  {TM-1: availableResources={5.5CPU, 24GB, 0GPU}, │
│   TM-2: availableResources={8CPU, 32GB, 1GPU}}   │
│                                                   │
│  分配逻辑：                                        │
│  1. 收到 SlotRequest{resourceProfile={2CPU,8GB}}   │
│  2. 遍历所有 TM，找到 availableResources 满足需求的  │
│  3. 向该 TM 发送 requestSlot(resourceProfile)      │
│  4. TM 从可用资源中切出 Slot，更新 availableResources│
│                                                   │
│  如果没有 TM 满足需求：                              │
│  → 向 K8s/YARN 请求新的 TaskManager                 │
└──────────────────────────────────────────────────┘
```

### 2.4 与 K8s 的交互

在 K8s Native 模式下，当 SlotManager 发现没有已注册的 TM 能满足某个 SlotRequest 时，会通过 `KubernetesResourceManagerDriver` 向 K8s 申请新的 TM Pod。这里有一个关键问题：**新 TM 应该申请多大的资源？**

当前的实现策略是：所有 TM Pod 使用统一的资源规格（由 `taskmanager.memory.process.size`、`taskmanager.cpu.cores` 等全局配置决定）。即使不同的 SlotRequest 有不同的 ResourceProfile，新创建的 TM Pod 都是一样大的。这意味着：

```
SlotRequest-A: {0.5CPU, 256MB}     ──┐
SlotRequest-B: {2CPU, 8GB, 1GPU}   ──┤── 都触发创建相同规格的 TM Pod
SlotRequest-C: {1CPU, 1GB}         ──┘    {8CPU, 32GB, 1GPU}
```

这是 FLIP-56 的一个已知局限——**动态 Slot 做到了 TM 内部的按需切割，但没有做到 TM 本身规格的差异化**。对于 K8s 来说，本可以创建一个小 TM（不带 GPU）给 Source 用、一个大 TM（带 GPU）给 Inference 用，但当前只能创建统一规格的"最大公约数"TM。

---

## 第三部分：FLIP-156——补上用户接口这块拼图

### 3.1 FLIP-56 留下的问题

FLIP-56 解决了运行时的动态 Slot 切割能力，但留下了一个关键问题：**用户如何告诉 Flink，不同的算子需要不同的资源？**

FLIP-56 的动态 Slot 虽然支持不同规格，但如果用户没有声明资源需求，SlotManager 只能用一个默认的 `ResourceProfile.UNKNOWN` 来分配——本质上退化回了"均匀切割"模式。

[FLIP-156](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173081645) 正是为了补上这块拼图：**它定义了用户声明细粒度资源需求的接口**。

两个 FLIP 的关系可以用一个类比理解：FLIP-56 是"建好了灵活的厨房"（动态 Slot 机制），FLIP-156 是"提供了点菜的菜单"（用户接口）。厨房再灵活，客人不点菜，厨师也只能上一桌统一的盒饭。

### 3.2 粒度选择：为什么是 SlotSharingGroup？

FLIP-156 在设计用户接口时面临一个粒度选择问题——让用户在哪个层级声明资源？有三个候选：

| 粒度 | 优点 | 缺点 |
|------|------|------|
| **算子（Operator）** | 最精确，每个算子独立声明 | 配置量爆炸；算子链（Chaining）后资源需要聚合，累积误差大 |
| **Task（算子链后的执行单元）** | 接近运行时 | 用户不直接感知 Task 的边界，难以配置 |
| **SlotSharingGroup** | 与 Slot 一一对应，配置量少 | 粒度较粗，同 Group 内的算子共享资源声明 |

最终 FLIP-156 选择了 **SlotSharingGroup（SSG）** 作为资源声明的粒度。原因是：Slot 是 Flink 运行时资源管理的基本单位，而 SSG 与 Slot 之间是一一映射关系——一个 SSG 内的算子共享同一个 Slot。在 SSG 粒度声明资源，既能让用户区分"重"算子和"轻"算子，又不会带来过大的配置负担。

### 3.3 用户 API

FLIP-156 提供的 DataStream API 允许用户创建带资源声明的 SlotSharingGroup，然后将算子分配到不同的 Group 中：

```java
final StreamExecutionEnvironment env =
    StreamExecutionEnvironment.getExecutionEnvironment();

// 定义一个轻量级的 SSG：用于 Source 和 Sink
SlotSharingGroup cpuGroup = SlotSharingGroup.newBuilder("cpu-group")
    .setCpuCores(1.0)
    .setTaskHeapMemoryMB(512)
    .build();

// 定义一个重量级的 SSG：用于 GPU 推理
SlotSharingGroup gpuGroup = SlotSharingGroup.newBuilder("gpu-group")
    .setCpuCores(2.0)
    .setTaskHeapMemoryMB(2048)
    .setTaskOffHeapMemoryMB(4096)
    .setManagedMemory(MemorySize.ofMebiBytes(256))
    .setExternalResource("gpu", 1.0)   // 声明需要 1 块 GPU
    .build();

DataStream<String> source = env
    .fromSource(KafkaSource.<String>builder().build(),
        WatermarkStrategy.noWatermarks(), "Kafka Source")
    .slotSharingGroup(cpuGroup);         // Source 用 cpuGroup

DataStream<String> result = source
    .map(new GpuInferenceFunction())
    .slotSharingGroup(gpuGroup);          // 推理用 gpuGroup

result
    .sinkTo(KafkaSink.<String>builder().build())
    .slotSharingGroup(cpuGroup);          // Sink 用 cpuGroup
```

这段代码的效果是：Flink Scheduler 在分配 Slot 时，会为 `cpuGroup` 请求 `{1CPU, 512MB}` 的 Slot，为 `gpuGroup` 请求 `{2CPU, 2048MB heap + 4096MB off-heap + 256MB managed + 1 GPU}` 的 Slot。只有 `gpuGroup` 的 Slot 会带 GPU 资源请求。

### 3.4 ResourceProfile 与 Slot 匹配

SlotSharingGroup 的资源声明在运行时被封装为 `ResourceProfile`，这是 Flink 内部表达资源需求的统一数据结构：

```java
public class ResourceProfile {
    private final CPUResource cpuCores;
    private final MemorySize taskHeapMemory;
    private final MemorySize taskOffHeapMemory;
    private final MemorySize managedMemory;
    private final MemorySize networkMemory;
    private final Map<String, ExternalResource> extendedResources;

    // 特殊值：表示未声明资源需求
    public static final ResourceProfile UNKNOWN = new ResourceProfile();
}
```

Slot 匹配采用**精确匹配**策略：

```
匹配规则：
  UNKNOWN ResourceProfile → 只能由 "默认规格" 的 Slot 满足
  指定的 ResourceProfile  → 只能由 "资源完全相等" 的 Slot 满足

示例：
  SlotRequest{2CPU, 8GB, 1GPU}  ✓ 匹配 Slot{2CPU, 8GB, 1GPU}
  SlotRequest{2CPU, 8GB, 1GPU}  ✗ 不匹配 Slot{4CPU, 16GB, 1GPU}（资源更多也不行）
  SlotRequest{UNKNOWN}          ✓ 匹配 Slot{默认规格}
  SlotRequest{UNKNOWN}          ✗ 不匹配 Slot{2CPU, 8GB, 1GPU}
```

为什么不用"大于等于"的宽松匹配？因为宽松匹配会导致资源碎片化——一个 `{4CPU, 16GB}` 的 Slot 被 `{1CPU, 1GB}` 的请求占用后，剩余的 3CPU 和 15GB 可能因为切割方式不佳而无法被利用。精确匹配虽然看似严格，但配合动态 Slot 切割（FLIP-56），Flink 总是从 TM 的可用资源中切出恰好大小的 Slot，不存在"大 Slot 满足小需求"的情况。

---

## 第四部分：端到端流程——细粒度资源在 K8s 上的流转

### 4.1 全景图

当用户在代码中声明了不同 SSG 的资源需求后，从 Job 提交到 Task 运行的完整流程如下：

```
① 用户代码声明资源需求
┌─────────────────────────────────────────────────────┐
│  cpuGroup: {1CPU, 512MB}                             │
│  gpuGroup: {2CPU, 2048MB heap, 4096MB off-heap, 1GPU}│
└───────────────────────┬─────────────────────────────┘
                        │
                        ▼
② Scheduler 生成差异化的 SlotRequest
┌──────────────────────────────────────────────────┐
│  SlotRequest-1: ResourceProfile{1CPU, 512MB}      │  ← Source
│  SlotRequest-2: ResourceProfile{2CPU, ~6.3GB, 1GPU} │  ← GPU Inference
│  SlotRequest-3: ResourceProfile{1CPU, 512MB}      │  ← Sink
└───────────────────────┬──────────────────────────┘
                        │
                        ▼
③ SlotManager 处理请求
┌──────────────────────────────────────────────────────────┐
│  对 SlotRequest-1: 查找有 ≥{1CPU, 512MB} 可用资源的 TM    │
│  对 SlotRequest-2: 查找有 ≥{2CPU, 6GB, 1GPU} 可用资源的 TM │
│                                                          │
│  如果找不到 → 向 K8s 请求新 TM Pod                         │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
④ KubernetesResourceManagerDriver 创建 TM Pod
┌──────────────────────────────────────────────────────────┐
│  当前实现：创建统一规格的 TM Pod                             │
│  Pod Spec: {8CPU, 32GB, 1GPU}                            │
│                                                          │
│  TM 启动后向 RM 注册，汇报 availableResources              │
└───────────────────────┬──────────────────────────────────┘
                        │
                        ▼
⑤ TM 内部动态切割 Slot
┌──────────────────────────────────────────────────────────┐
│  TM 收到 requestSlot(ResourceProfile{1CPU, 512MB})       │
│    → 从 available 中切出 {1CPU, 512MB}                    │
│    → available 变为 {7CPU, 31.5GB, 1GPU}                  │
│                                                          │
│  TM 收到 requestSlot(ResourceProfile{2CPU, 6GB, 1GPU})   │
│    → 从 available 中切出 {2CPU, 6GB, 1GPU}                │
│    → available 变为 {5CPU, 25.5GB, 0GPU}                  │
│                                                          │
│  TM 收到 requestSlot(ResourceProfile{1CPU, 512MB})       │
│    → 从 available 中切出 {1CPU, 512MB}                    │
│    → available 变为 {4CPU, 25GB, 0GPU}                    │
└──────────────────────────────────────────────────────────┘
```

### 4.2 关键问题：TM Pod 规格的"最大公约数"困境

从上面的流程可以看出，当前实现中所有 TM Pod 仍然使用统一的资源规格。这意味着即使 `SlotRequest-1`（Source）只需要 1CPU + 512MB，它所在的 TM Pod 也被配置了 1 块 GPU。

这并不是 FLIP-56/156 没有考虑到的问题，而是一个有意的分阶段实现策略。当前的优先级是：

```
已实现 ✓                              待实现 ○

✓ 同一 TM 内，不同 Slot 可以有不同规格      ○ 不同 TM Pod 可以有不同规格
✓ 用户可以通过 SSG 声明差异化的资源需求       ○ RM 根据 SlotRequest 智能选择 TM 规格
✓ SlotManager 的动态 Slot 切割              ○ K8s 上创建异构 TM Pod
```

在 K8s 场景下，当 TM Pod 规格统一时，细粒度资源管理的核心收益体现在 **TM 内部的资源利用率提升**——一个 `{8CPU, 32GB, 1GPU}` 的 TM 中，GPU 只被切给了真正需要 GPU 的 Slot，其他 Slot 不占用 GPU，同一块 GPU 不会被浪费给不需要的算子。但 TM 级别的 GPU 浪费（不需要 GPU 的 Slot 所在的 TM 也带了 GPU）仍然存在。

### 4.3 Slot 释放与资源回收

动态 Slot 的一个重要特性是 **Slot 释放后资源归还可用池**。在粗粒度模型中，一个预定义的 Slot 即使空闲也占着资源；在动态模型中，Slot 销毁后资源立即可被其他请求使用：

```
Slot-B (GPU Inference) 完成任务，被 JobManager 释放
  │
  ▼
TM 销毁 Slot-B，资源回收：
  available: {4CPU, 25GB, 0GPU} + {2CPU, 6GB, 1GPU}
           = {6CPU, 31GB, 1GPU}
  │
  ▼
新的 SlotRequest 可以利用回收的资源
  例如新的 GPU 推理任务可以复用这块 GPU
```

这对 Batch 作业尤其重要——不同的执行阶段可以复用 TM 的资源，而不需要为每个阶段预留固定的 Slot。

---

## 第五部分：源码追踪——从 SSG 声明到 K8s Pod 创建

### 5.1 资源需求的传播链路

用户在 DataStream API 中声明的 SSG 资源需求，经过层层传递最终到达 K8s：

```
用户代码
  │  stream.slotSharingGroup(gpuGroup)
  ▼
StreamGraphGenerator
  │  将 SSG 的 ResourceProfile 写入 StreamGraph
  ▼
StreamGraph → JobGraph
  │  SSG 信息保存在 JobVertex 中
  ▼
ExecutionGraph
  │  Scheduler 构建 ExecutionSlotSharingGroup
  │  每个 Group 关联一个 ResourceProfile
  ▼
SlotSharingExecutionSlotAllocator
  │  为每个 ExecutionSlotSharingGroup 生成 SlotRequest
  │  SlotRequest 携带 ResourceProfile
  ▼
SlotManager (ResourceManager 内部)
  │  匹配已有 TM 的可用资源
  │  或请求新 TM
  ▼
KubernetesResourceManagerDriver
  │  创建 TM Pod
  ▼
K8s API Server → Schedule Pod → TM 启动 → 注册 → 动态切 Slot
```

### 5.2 SlotManager 的匹配逻辑

SlotManager 收到 SlotRequest 后的核心匹配逻辑：

```java
// 简化的 SlotManager 匹配逻辑
void processSlotRequest(SlotRequest request) {
    ResourceProfile requiredProfile = request.getResourceProfile();

    // 1. 在已注册的 TM 中查找
    for (TaskManagerInfo tm : registeredTaskManagers) {
        ResourceProfile available = tm.getAvailableResources();

        if (available.isMatching(requiredProfile)) {
            // 找到了！向 TM 请求动态切割 Slot
            tm.getGateway().requestSlot(
                newSlotId(),
                request.getJobId(),
                request.getAllocationId(),
                requiredProfile    // 告诉 TM 切多大
            );
            return;
        }
    }

    // 2. 在 Pending（正在启动）的 TM 中查找
    for (PendingTaskManager ptm : pendingTaskManagers) {
        ResourceProfile pendingAvailable = ptm.getAvailableResources();
        if (pendingAvailable.isMatching(requiredProfile)) {
            // 标记：等 TM 启动后分配
            ptm.assignSlotRequest(request);
            return;
        }
    }

    // 3. 都不满足，向 K8s 请求新 TM
    requestNewTaskManager(requiredProfile);
}
```

### 5.3 TaskManager 的动态 Slot 创建

TaskManager 收到 `requestSlot` 后，在 `TaskSlotTable` 中动态创建 Slot：

```java
// 简化的 TaskSlotTable 动态 Slot 逻辑
class TaskSlotTable {
    private ResourceProfile availableResources;  // 可用资源池
    private Map<SlotID, TaskSlot> allocatedSlots; // 已分配的 Slot

    boolean allocateSlot(SlotID slotId, ResourceProfile profile) {
        // 检查可用资源是否足够
        if (!availableResources.isMatching(profile)) {
            return false;
        }

        // 从可用资源中扣除
        availableResources = availableResources.subtract(profile);

        // 创建新的 Slot
        TaskSlot slot = new TaskSlot(slotId, profile);
        allocatedSlots.put(slotId, slot);
        return true;
    }

    void freeSlot(SlotID slotId) {
        TaskSlot slot = allocatedSlots.remove(slotId);
        // 资源归还可用池
        availableResources = availableResources.merge(slot.getResourceProfile());
    }
}
```

---

## 第六部分：实战——在 K8s 上使用细粒度资源管理

### 6.1 完整示例：混合 CPU/GPU Pipeline

以下是一个完整的实战示例，展示如何在 Flink on K8s 中使用细粒度资源管理来优化 GPU 利用率：

```java
public class GpuInferencePipeline {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // ========== 定义两种资源规格的 SlotSharingGroup ==========

        // 轻量级 Group：用于 IO 密集型算子（Source、Sink）
        SlotSharingGroup ioGroup = SlotSharingGroup.newBuilder("io-group")
            .setCpuCores(0.5)
            .setTaskHeapMemoryMB(256)
            .setTaskOffHeapMemoryMB(128)
            .setManagedMemory(MemorySize.ofMebiBytes(64))
            // 不声明 GPU —— 这些算子不需要
            .build();

        // 重量级 Group：用于 GPU 推理算子
        SlotSharingGroup gpuGroup = SlotSharingGroup.newBuilder("gpu-group")
            .setCpuCores(2.0)
            .setTaskHeapMemoryMB(2048)
            .setTaskOffHeapMemoryMB(4096)    // GPU 推理框架的 off-heap 需求
            .setManagedMemory(MemorySize.ofMebiBytes(256))
            .setExternalResource("gpu", 1.0) // 声明 1 块 GPU
            .build();

        // ========== 构建 Pipeline ==========

        DataStream<String> source = env
            .addSource(new FlinkKafkaConsumer<>(...))
            .name("Kafka Source")
            .slotSharingGroup(ioGroup);      // IO Group，不需要 GPU

        DataStream<String> parsed = source
            .map(new JsonParser())
            .name("JSON Parse")
            .slotSharingGroup(ioGroup);      // IO Group，不需要 GPU

        DataStream<InferenceResult> inferred = parsed
            .map(new GpuInferenceFunction())
            .name("GPU Inference")
            .slotSharingGroup(gpuGroup);      // GPU Group，需要 GPU

        inferred
            .addSink(new FlinkKafkaProducer<>(...))
            .name("Kafka Sink")
            .slotSharingGroup(ioGroup);       // IO Group，不需要 GPU

        env.execute("GPU Inference Pipeline");
    }
}
```

### 6.2 Flink 配置

使用细粒度资源管理时，`flink-conf.yaml` 需要注意以下要点：

```yaml
# TaskManager 总资源配置
# 注意：这是 TM 的总量，不是单个 Slot 的量
# 应该足够容纳你声明的最大 SSG + 一些额外的 framework 开销
taskmanager.memory.process.size: 16384m
taskmanager.cpu.cores: 8

# GPU 外部资源（TM 级别）
external-resources: gpu
external-resource.gpu.amount: 1
external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory
external-resource.gpu.param.discovery-script.path: /opt/flink/plugins/gpu/gpu-discovery.sh
external-resource.gpu.kubernetes.config-key: nvidia.com/gpu

# K8s 配置
kubernetes.container.image: my-registry/flink-gpu:1.18
kubernetes.namespace: flink-gpu

# 细粒度资源管理不再使用 taskmanager.numberOfTaskSlots
# Slot 数量由实际的 SlotRequest 动态决定
```

关键区别：在粗粒度模式下，你需要设置 `taskmanager.numberOfTaskSlots: 4` 来预切 Slot；在细粒度模式下，这个配置不再生效（或者说退化为计算默认 Slot 规格的参考值），Slot 数量由 TM 的总资源和实际 SlotRequest 的规格动态决定。

### 6.3 资源利用率对比

以上面的 Pipeline 为例，假设 Source、Parse、Sink 的并行度都是 4，GPU Inference 的并行度是 2。来比较粗粒度和细粒度模式下的资源消耗：

```
粗粒度模式（所有算子在默认 SSG 中，Slot Sharing 生效）：
  所有算子共享同一个 SSG，Slot 数 = max(4, 4, 2, 4) = 4
  每个 Slot 按最大需求配: {2CPU, 6GB, 1GPU}（因为 Inference 需要 GPU）
  每个 TM 配 2 Slot: 需要 2 个 TM
  总 GPU 消耗: 2 TM × 1 GPU = 2 块 GPU
  实际使用 GPU 的子任务: 只有 2 个（Inference 并行度为 2）
  问题: 4 个 Slot 中每一个都"看得见"GPU，但只有 2 个 Slot 真正运行 Inference
        剩余 2 个 Slot 中的 Source/Parse/Sink 白白占着 GPU 资源


细粒度模式（SSG 差异化声明，Slot Sharing 在各自 SSG 内生效）：
  ioGroup:  Source + Parse + Sink 共享 → Slot 数 = max(4, 4, 4) = 4
  gpuGroup: GPU Inference 独占        → Slot 数 = 2

  ioGroup Slot: {0.5CPU, ~0.5GB}     × 4 个（不请求 GPU）
  gpuGroup Slot: {2CPU, ~6GB, 1GPU}  × 2 个（请求 GPU）

  总 Slot: 4 + 2 = 6
  总 GPU 消耗: 只有 gpuGroup 的 2 个 Slot 请求 GPU
  TM 层面仍统一配 GPU（当前限制），假设 2 个 TM：
  总 GPU 消耗: 2 TM × 1 GPU = 2 块 GPU
  GPU 利用率: TM 内部 GPU 只被切给 gpuGroup 的 Slot，不被 ioGroup 占用
```

在这个例子中，粗粒度和细粒度模式的 TM 数量恰好相同，但核心区别在于 **GPU 在 TM 内部的分配方式**。粗粒度模式下，所有 4 个 Slot 共享 GPU 访问权限，Slot 内的 Source/Parse/Sink 算子虽然不需要 GPU，却占着 GPU 的调度资源。细粒度模式下，GPU 被精确地切给了 2 个 gpuGroup Slot，4 个 ioGroup Slot 完全不占用 GPU。

当规模放大时差距更加明显：如果 Source/Parse/Sink 的并行度是 100 而 Inference 只有 4，粗粒度模式需要 100 个带 GPU 的 Slot（因为所有算子共享 SSG），而细粒度模式只需要 4 个带 GPU 的 Slot + 100 个纯 CPU Slot。

---

## 第七部分：当前局限与演进方向

### 7.1 当前的已知局限

作为一个 MVP（Minimum Viable Product），细粒度资源管理目前有几个重要的局限：

**仅支持 DataStream API**。Table API / SQL 暂不支持用户手动指定 SSG 资源。未来的方向是让 Planner 根据算子特征自动推导资源需求，只暴露少量配置旋钮给用户。

**不支持弹性伸缩（Reactive Scaling）**。细粒度资源管理与 Reactive Scheduler 目前不兼容。Reactive 模式的核心是"有多少资源就用多少"，而细粒度模式需要精确声明需求，两者在理念上存在冲突。

**TM Pod 规格统一**。如前所述，K8s 上所有 TM Pod 仍然使用相同的资源配置。对于 GPU 场景，这意味着不需要 GPU 的 Slot 所在的 TM 也可能带了 GPU（如果全局配置了 GPU）。

**Slot 分配的 NP-hard 问题**。将不同规格的 SlotRequest 最优地装入一组 TM，本质上是一个 bin packing 问题（NP-hard）。当前 Flink 的分配算法使用启发式策略，不保证最优解，某些情况下可能导致资源碎片。

**不建议混合声明模式**。同一个 Job 中，要么所有 SSG 都声明资源（细粒度模式），要么都不声明（粗粒度模式），不推荐混合使用。

### 7.2 演进方向

**异构 TM Pod**。最直接的优化方向。让 `KubernetesResourceManagerDriver` 根据 pending SlotRequest 的 ResourceProfile 决定新 TM 的规格——需要 GPU 的 SlotRequest 触发创建带 GPU 的 TM Pod，不需要 GPU 的触发创建纯 CPU 的 TM Pod。

```
当前：
  所有 SlotRequest → 统一规格 TM Pod {8CPU, 32GB, 1GPU}

未来：
  SlotRequest{0.5CPU, 512MB}      → TM Pod {4CPU, 8GB}        (无 GPU)
  SlotRequest{2CPU, 6GB, 1GPU}    → TM Pod {4CPU, 16GB, 1GPU} (带 GPU)
```

**Table/SQL 自动推导**。让 Flink Planner 分析查询计划，根据算子类型（Scan/Join/Aggregate/UDF）自动推导每个 SSG 的资源需求，用户只需要调整少量旋钮（如 UDF 的 GPU 需求）。

**资源死锁预防优化**。当前的死锁预防策略是串行化 SSG 的 Slot 请求——先满足一个 SSG 的所有 Slot，再处理下一个 SSG。这可能导致吞吐量下降。未来可以引入更智能的调度策略，在保证无死锁的前提下允许并行请求。

---

## 第八部分：粗粒度 vs 细粒度——如何选择？

### 8.1 决策矩阵

| 场景 | 推荐模式 | 原因 |
|------|---------|------|
| 所有算子资源需求相近 | 粗粒度 | 无需额外配置，简单稳定 |
| 有 GPU / FPGA 等昂贵外部资源 | 细粒度 | 避免昂贵资源被不需要的算子占用 |
| Batch 作业，各阶段资源差异大 | 细粒度 | 不同阶段可以复用 TM 资源 |
| 算子并行度差异大 | 细粒度 | 避免低并行度算子浪费 Slot 资源 |
| 使用 Table/SQL API | 粗粒度 | 细粒度暂不支持 Table/SQL |
| 需要弹性伸缩 | 粗粒度 | 细粒度不兼容 Reactive Scaling |
| 追求运维简单 | 粗粒度 | 少一个调优维度 |

### 8.2 生产建议

如果你决定使用细粒度资源管理，有几个实践建议：

**合理规划 TM 总资源**。TM 的 `taskmanager.memory.process.size` 和 `taskmanager.cpu.cores` 应该足够容纳你最大的 SSG，并留有余量给 framework 开销（JVM metaspace、network buffers 等）。一个 TM 能容纳多少个 Slot 取决于总资源除以各 Slot 的需求。

**避免 SSG 过多**。每个 SSG 意味着一种独立的 Slot 规格。SSG 种类过多会增加 bin packing 的复杂度，降低资源利用率。建议控制在 2-4 种。

**监控资源碎片**。通过 Flink REST API 和 Metrics 监控 TM 的可用资源（available resources）。如果发现大量 TM 有未使用的 CPU/Memory 碎片但无法满足任何新的 SlotRequest，说明 SSG 的规格设计需要调整。

---

## 总结

从第一篇到第三篇，我们完整地走过了 Flink on K8s 的资源管理演进路径：

**第一篇**建立了基础认知——Flink 通过 `ResourceManagerDriver` 抽象与 K8s 交互，JM 用 Deployment、TM 用 Bare Pod，资源需求被翻译成 K8s 的 `ResourceRequirements`。

**第二篇**引入了 GPU 这个"异类"——FLIP-108 的 External Resource Framework 解决了 GPU 的发现和使用问题，但也暴露了粗粒度资源模型的浪费。

**第三篇**给出了解法——FLIP-56 将 TM 从"预切 Slot"变成"资源池"，FLIP-156 让用户通过 SlotSharingGroup 声明差异化的资源需求。两者配合，实现了 Slot 级别的按需分配。

三篇文章串起来，可以看到 Flink 资源管理的设计哲学：**分层解耦、渐进演进**。每一层只解决一个问题——Driver 层解决"如何与 K8s 交互"，External Resource 层解决"如何管理 GPU 等特殊设备"，Fine-Grained 层解决"如何按需分配资源"。这种分层设计使得每一步的改进都能独立落地，不需要一次性推翻重来。

不过，正如本文分析的那样，细粒度资源管理仍然是一个 MVP——TM Pod 规格统一、Table/SQL 不支持、Slot 分配非最优——这些都是等待解决的问题。对于有 GPU 等昂贵资源需求的实时推理场景，当前的最佳实践仍然是结合细粒度 SSG 声明与合理的 Job 拆分策略，在框架能力与工程约束之间找到平衡点。

> **参考资料**
> - [FLIP-56: Dynamic Slot Allocation](https://cwiki.apache.org/confluence/display/FLINK/FLIP-56%3A+Dynamic+Slot+Allocation)
> - [FLIP-156: Runtime Interfaces for Fine-Grained Resource Requirements](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173081645)
> - [Flink Fine-Grained Resource Management 官方文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/finegrained_resource/)
> - [深入理解 Flink on Kubernetes（一）：从原理到源码的启动全流程](flink-on-k8s.md)
> - [深入理解 Flink on Kubernetes（二）：GPU 加速从原理到实战](flink-on-k8s-gpu.md)
> - Flink 源码 `flink-runtime` 模块 `slotmanager` 与 `scheduler` 包
