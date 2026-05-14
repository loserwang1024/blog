# 深入理解 Flink on Kubernetes（二）：GPU 加速从原理到实战

> 本文是 Flink on K8s 系列的第二篇。在[上一篇](flink-on-k8s.md)中，我们深入剖析了 Flink Native Kubernetes 的启动全流程——JM 用 Deployment、TM 用 Bare Pod 的设计哲学，以及 ResourceManagerDriver 的分层抽象。上篇 [1.4 扩展资源（Extended Resources）](flink-on-k8s.md#14-扩展资源extended-resources) 中提到，K8s 通过 Device Plugin 机制支持 GPU 等特殊硬件资源，但并未展开。本文将以此为起点，聚焦 **FLIP-108 External Resource Framework** 的设计与实现，深入探讨 Flink 如何将 GPU 纳入资源管理体系，并在 K8s 上实现端到端的 GPU 调度与隔离。

---

## 第一部分：为什么 Flink 需要 GPU？

### 1.1 背景：流计算遇上深度学习

随着实时 AI 推理需求的爆发，越来越多的场景要求在流处理管道中直接运行深度学习模型：实时风控、在线推荐、视频流分析、自然语言处理等。这些场景的共同特征是——**模型推理是计算瓶颈，而 GPU 是加速推理的核心硬件**。

传统方案是将 Flink 与 GPU 推理服务（如 TensorFlow Serving、Triton）分离部署，通过 RPC 调用。这种架构引入了额外的网络延迟和运维复杂度。如果 Flink 的 TaskManager 能直接使用 GPU，推理就可以在算子内部完成，消除跨服务调用的开销。

### 1.2 FLIP-108：External Resource Framework

Apache Flink 通过 [FLIP-108](https://cwiki.apache.org/confluence/display/FLINK/FLIP-108%3A+Add+GPU+support+in+Flink) 引入了一个通用的 **外部资源框架（External Resource Framework）**，GPU 只是第一个一等公民。这个框架遵循关注点分离原则：Flink 不直接管理 GPU 硬件，而是将资源的申请委托给底层资源管理器（K8s、YARN），将资源的发现委托给可插拔的 Driver。

整体设计可以用一句话概括：**ResourceManager 负责"要到"GPU，ExternalResourceDriver 负责"找到"GPU，RuntimeContext 负责"暴露"GPU**。

```
                    用户代码（UDF / Operator）
                           │
                           │  RuntimeContext.getExternalResourceInfos("gpu")
                           ▼
                   ┌────────────────┐
                   │ RuntimeContext  │    ← 暴露 GPU 信息给算子
                   └────────┬───────┘
                            │
                   ┌────────▼────────┐
                   │ ExternalResource│    ← 发现本地可用的 GPU 设备
                   │    Driver       │
                   └────────┬────────┘
                            │
                            │  discovery-script (nvidia-smi / rocm-smi)
                            ▼
                   ┌─────────────────┐
                   │  GPU Hardware   │    ← 由 K8s Device Plugin 分配
                   └─────────────────┘
```

---

## 第二部分：External Resource Framework 核心设计

### 2.1 核心接口

External Resource Framework 定义了三个核心接口，构成了一个清晰的分层抽象：

**ExternalResourceDriverFactory**：创建 Driver 的工厂，通过 SPI 机制加载。

```java
public interface ExternalResourceDriverFactory {
    ExternalResourceDriver createExternalResourceDriver(Configuration config);
}
```

**ExternalResourceDriver**：资源发现的核心，负责检测当前 TaskManager 可见的外部资源。

```java
public interface ExternalResourceDriver {
    Set<? extends ExternalResourceInfo> retrieveResourceInfo(long amount);
}
```

`retrieveResourceInfo` 的参数 `amount` 是用户配置的资源数量（如 2 块 GPU），Driver 需要返回恰好 `amount` 个可用设备的信息。如果实际可用设备不足，应抛出异常。

**为什么 CPU 和 Memory 不需要 Driver，而 GPU 需要？**

读到这里你可能会问：K8s 不是已经通过 Device Plugin 把 GPU 分配给容器了吗？为什么 Flink 还需要一个 ExternalResourceDriver 来"发现"GPU？CPU 和 Memory 不也是 K8s 分配的吗，怎么就不需要 Driver？

根本原因在于：**CPU 和 Memory 是操作系统内核透明管理的资源，而 GPU 是需要应用显式寻址的外部设备**。

```
CPU/Memory 的使用路径（全透明，无需发现）：

  K8s 分配            OS 内核透明管理           JVM / Flink 代码
 ┌──────────┐   cgroups 限制    ┌──────────┐   线程自动调度   ┌──────────────┐
 │ 2 cores  │ ───────────────> │ CFS 调度器│ ─────────────> │ a + b 直接执行 │
 │ 4GB mem  │   容器可见的 CPU   │ 内存管理   │  malloc 自动分配 │ 无需知道哪个核 │
 └──────────┘   被 cgroup 限制   └──────────┘                └──────────────┘
                      ↑
                整个过程对应用完全透明，无需任何"发现"步骤


GPU 的使用路径（需要显式发现和寻址）：

  K8s 分配             容器环境                  需要 Driver 填补的 gap
 ┌──────────┐  Device Plugin   ┌──────────────┐          ┌──────────────────┐
 │ 1 GPU    │ ──────────────> │ 设置环境变量    │          │ ExternalResource │
 └──────────┘  挂载 /dev/nvidia │ NVIDIA_VISIBLE │ ──?──> │    Driver        │
                               │ _DEVICES=uuid  │  JVM 不  │ 执行 nvidia-smi   │
                               └──────────────┘  知道有几  │ 解析出 index=0    │
                                                  块 GPU   └────────┬─────────┘
                                                  编号是啥          │
                                                           ┌───────▼──────────┐
                                                           │ Flink 算子        │
                                                           │ cudaSetDevice(0) │
                                                           │ 开始 GPU 推理     │
                                                           └──────────────────┘
```

CPU 之所以不需要 Driver，是因为 Linux 内核的 CFS 调度器"亲生"管理着 CPU——你的 Java 代码执行 `a + b`，编译器生成机器指令，OS 把线程扔到某个 core 上执行，应用从头到尾不需要知道自己在哪个 core 上跑。Memory 也一样，`new byte[1024]` 拿到的内存由 OS 透明分配，你不关心它在哪个物理地址。

GPU 则完全不同——它是一个通过 PCIe 总线连接的协处理器，有自己独立的内存空间（显存）。即使 K8s 通过 Device Plugin 把 GPU "给"了容器（设置了环境变量和设备映射），JVM 进程仍然不知道：容器里有几块 GPU？设备编号是什么？应用代码必须通过 `cudaSetDevice(index)` 显式指定使用哪块 GPU，必须通过 `cudaMemcpy` 显式地在 CPU 内存和 GPU 显存之间搬运数据。

ExternalResourceDriver 就是填补这个 gap 的桥梁：**K8s Device Plugin 负责"把 GPU 的门打开"，ExternalResourceDriver 负责"走进去看看门牌号"，然后告诉 Flink 算子可以用哪些设备**。

**ExternalResourceInfo**：资源信息的载体，以 key-value 形式提供设备属性。

```java
public interface ExternalResourceInfo {
    Optional<String> getProperty(String key);
    Collection<String> getKeys();
}
```

对于 GPU，`GPUInfo` 实现类的核心属性是 GPU index（设备编号），用户代码可以据此初始化 CUDA context 或设置 `CUDA_VISIBLE_DEVICES`。

### 2.2 RuntimeContext 扩展

框架在 `RuntimeContext` 中新增了一个方法，使得任意算子或 UDF 都可以获取分配到当前 TaskManager 的 GPU 信息：

```java
public interface RuntimeContext {
    Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName);
}
```

在用户代码中使用：

```java
public class GpuInferenceFunction extends RichMapFunction<String, String> {
    @Override
    public void open(Configuration parameters) {
        Set<ExternalResourceInfo> gpuInfos =
            getRuntimeContext().getExternalResourceInfos("gpu");
        for (ExternalResourceInfo info : gpuInfos) {
            // 获取 GPU 设备索引
            Optional<String> index = info.getProperty("index");
            index.ifPresent(idx -> {
                // 初始化 GPU 推理引擎（如 TensorRT、ONNX Runtime）
                initGpuEngine(Integer.parseInt(idx));
            });
        }
    }

    @Override
    public String map(String input) {
        return gpuInference(input);
    }
}
```

### 2.3 配置体系

External Resource Framework 采用统一的命名空间配置：

```properties
# 启用的外部资源列表（逗号分隔）
external-resources: gpu

# 每个 TaskExecutor 的 GPU 数量
external-resource.gpu.amount: 1

# Driver 工厂类
external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory

# GPU 发现脚本路径（可选，有默认脚本）
external-resource.gpu.param.discovery-script.path: /opt/flink/plugins/gpu/gpu-discovery.sh

# Kubernetes 特有：GPU 资源在 K8s 中的 resource key
external-resource.gpu.kubernetes.config-key: nvidia.com/gpu

# YARN 特有：GPU 资源在 YARN 中的 resource key
external-resource.gpu.yarn.config-key: yarn.io/gpu
```

配置命名规则为 `external-resource.{resourceName}.{property}`，这意味着你可以扩展框架支持任意外部资源——FPGA、TPU、InfiniBand 等，只需要实现对应的 Driver 并提供配置。

---

## 第三部分：GPU 在 K8s 上的端到端工作流

这是本文的核心。我们将从上篇中熟悉的 Flink Native K8s 架构出发，追踪 GPU 资源在整个生命周期中的流转过程。

### 3.1 K8s GPU 设备管理前置知识

在深入 Flink 之前，需要理解 K8s 如何管理 GPU。K8s 通过 **Device Plugin** 机制支持 GPU：

```
Node (物理机/虚拟机)
  │
  ├── kubelet
  │     └── Device Plugin Manager
  │           └── NVIDIA Device Plugin (DaemonSet)
  │                 ├── 向 kubelet 注册 nvidia.com/gpu 资源
  │                 ├── 报告可用 GPU 数量
  │                 └── 分配 GPU 时设置容器的环境变量和设备映射
  │
  └── GPU Hardware
        ├── GPU 0
        ├── GPU 1
        └── GPU 2
```

关键点：

- NVIDIA Device Plugin 以 DaemonSet 方式部署在每个 GPU 节点上
- 它通过 gRPC 向 kubelet 注册 `nvidia.com/gpu` 这个扩展资源
- 当 Pod 请求 `nvidia.com/gpu: 1` 时，kubelet 通过 Device Plugin 为容器分配一块具体的 GPU，并设置 `NVIDIA_VISIBLE_DEVICES` 环境变量和 `/dev/nvidia*` 设备映射
- **K8s 保证容器级别的 GPU 隔离**：一块 GPU 不会同时分配给多个 Pod

### 3.2 端到端流程

以 Application Mode 为例，GPU 资源的完整流转过程：

```
                    ① Flink Client 提交集群
                    ┌─────────────────────────────────────────────┐
                    │  KubernetesClusterDescriptor                │
                    │    .deployApplicationCluster()              │
                    │  创建 JM Deployment + ConfigMap (含 GPU 配置) │
                    └────────────────┬────────────────────────────┘
                                     │
                                     ▼
                    ② JM 启动，解析 GPU 配置
                    ┌──────────────────────────────────────────────┐
                    │  KubernetesApplicationClusterEntrypoint      │
                    │    → ResourceManager 启动                    │
                    │    → 读取 external-resource.gpu.amount       │
                    │    → 读取 external-resource.gpu.kubernetes.config-key│
                    └────────────────┬─────────────────────────────┘
                                     │
                                     ▼
                    ③ RM 请求带 GPU 的 TaskManager
                    ┌──────────────────────────────────────────────┐
                    │  KubernetesResourceManagerDriver             │
                    │    .requestResource(TaskExecutorProcessSpec)  │
                    │                                              │
                    │  TaskExecutorProcessSpec 包含:                │
                    │    - CPU: 2 cores                            │
                    │    - Memory: 4096 MB                         │
                    │    - ExternalResource: {"gpu": 1}            │
                    └────────────────┬─────────────────────────────┘
                                     │
                                     ▼
                    ④ 构建 TM Pod Spec (含 GPU 资源请求)
                    ┌──────────────────────────────────────────────┐
                    │  KubernetesTaskManagerFactory                │
                    │    .buildTaskManagerKubernetesPod()           │
                    │                                              │
                    │  InitTaskManagerDecorator:                    │
                    │    resources.requests["nvidia.com/gpu"] = 1   │
                    │    resources.limits["nvidia.com/gpu"] = 1     │
                    └────────────────┬─────────────────────────────┘
                                     │
                                     ▼
                    ⑤ K8s 调度到 GPU 节点
                    ┌──────────────────────────────────────────────┐
                    │  kube-scheduler                              │
                    │    Filter: 排除无 GPU 或 GPU 不足的 Node       │
                    │    Score: 对候选 Node 打分                    │
                    │    Bind: 绑定到选中的 GPU Node                │
                    │                                              │
                    │  Device Plugin:                               │
                    │    分配具体的 GPU 设备给容器                    │
                    │    设置 NVIDIA_VISIBLE_DEVICES=GPU-uuid       │
                    └────────────────┬─────────────────────────────┘
                                     │
                                     ▼
                    ⑥ TM 启动，GPU Driver 发现设备
                    ┌──────────────────────────────────────────────┐
                    │  KubernetesTaskExecutorRunner                 │
                    │    → 构造 GPUDriver                          │
                    │    → 执行 discovery-script (nvidia-smi)       │
                    │    → 返回 GPUInfo{index=0}                   │
                    │    → 注册到 RuntimeContext                    │
                    └────────────────┬─────────────────────────────┘
                                     │
                                     ▼
                    ⑦ 用户代码使用 GPU
                    ┌──────────────────────────────────────────────┐
                    │  RichMapFunction.open()                       │
                    │    → getRuntimeContext()                      │
                    │        .getExternalResourceInfos("gpu")       │
                    │    → 获取 GPU index                          │
                    │    → 初始化 CUDA / TensorRT / ONNX Runtime   │
                    └──────────────────────────────────────────────┘
```

### 3.3 源码追踪：GPU 资源如何写入 Pod Spec

在上篇中我们介绍了 `KubernetesUtils.getResourceRequirements()` 方法，它负责将 Flink 的抽象资源需求翻译成 K8s 的 `ResourceRequirements`。GPU 资源就是在这里被注入的：

```java
// KubernetesUtils.java
public static ResourceRequirements getResourceRequirements(
        ResourceSpec resourceSpec,
        double cpuLimitFactor,
        double memoryLimitFactor,
        Map<String, ExternalResource> externalResources,
        Map<String, String> externalResourceConfigKeys) {

    // CPU 和 Memory 的处理（上篇已分析）
    // ...

    // GPU 等外部资源的处理
    for (Map.Entry<String, ExternalResource> entry : externalResources.entrySet()) {
        String resourceName = entry.getKey();           // "gpu"
        ExternalResource resource = entry.getValue();    // amount = 1

        // 获取 K8s 中对应的 resource key
        String configKey = externalResourceConfigKeys.get(resourceName);
        // configKey = "nvidia.com/gpu"（来自 external-resource.gpu.kubernetes.config-key）

        if (configKey != null) {
            Quantity quantity = new Quantity(String.valueOf(resource.getValue()));
            resourceRequirementsBuilder
                .addToRequests(configKey, quantity)
                .addToLimits(configKey, quantity);
        }
    }
    return resourceRequirementsBuilder.build();
}
```

注意这里 GPU 的 `requests` 和 `limits` 始终相等，这是 K8s 扩展资源的要求——**Extended Resources 不支持超售（overcommit）**。

最终生成的 TM Pod Spec 中会包含：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-flink-app-taskmanager-1-1
  labels:
    component: taskmanager
spec:
  restartPolicy: Never
  containers:
    - name: flink-main-container
      image: my-flink-gpu:latest
      resources:
        requests:
          cpu: "2"
          memory: "4096Mi"
          nvidia.com/gpu: "1"
        limits:
          cpu: "2"
          memory: "4096Mi"
          nvidia.com/gpu: "1"
```

### 3.4 GPU 发现脚本：从硬件到 Flink

当 TM Pod 在 GPU 节点上启动后，Flink 需要知道哪些 GPU 设备可用。这由 `GPUDriver` 通过调用发现脚本完成。

Flink 自带的默认发现脚本工作原理：

```bash
#!/usr/bin/env bash
# 简化的 GPU 发现逻辑

# 1. 获取请求的 GPU 数量（由 Flink 传入）
requested_amount=$1

# 2. 使用 nvidia-smi 获取可见的 GPU 列表
gpu_indexes=$(nvidia-smi --query-gpu=index --format=csv,noheader | tr '\n' ',')

# 3. 检查可用 GPU 数量是否满足需求
available_count=$(echo "$gpu_indexes" | tr ',' '\n' | wc -l)
if [ "$available_count" -lt "$requested_amount" ]; then
    echo "Requested $requested_amount GPUs but only $available_count available" >&2
    exit 1
fi

# 4. 返回逗号分隔的 GPU 索引列表
echo "$gpu_indexes" | cut -d',' -f1-$requested_amount
```

在 K8s 环境下，由于 Device Plugin 已经通过 `NVIDIA_VISIBLE_DEVICES` 环境变量限制了容器可见的 GPU，`nvidia-smi` 只会列出被分配的 GPU。因此发现脚本通常不需要特殊配置就能正确工作。

---

## 第四部分：GPU 隔离——多层防护体系

GPU 隔离是生产环境中最关键的问题之一。Flink on K8s 的 GPU 隔离是一个多层体系：

### 4.1 Pod 级别隔离（K8s 保障）

这是最外层的隔离，由 K8s Device Plugin 机制保障：

- 每块 GPU 在同一时刻只被分配给一个 Pod
- 容器只能看到被分配的 GPU（通过 `NVIDIA_VISIBLE_DEVICES` 控制）
- 不同 Pod 之间完全隔离

这一层对 Flink 是透明的——Flink 提交 Pod 时声明需要 N 块 GPU，K8s 负责找到合适的 Node 并分配具体的 GPU 设备。

### 4.2 TaskExecutor 级别隔离（Flink + K8s 保障）

在 K8s Native 模式下，每个 TM 是一个独立的 Pod，天然具备 Pod 级别的 GPU 隔离。这比 Standalone 模式优雅得多——Standalone 模式需要通过发现脚本的 `--privilege` 选项和文件锁机制来实现多个 TM 进程之间的 GPU 互斥分配。

| 部署模式 | 隔离机制 | 可靠性 |
|---------|---------|-------|
| **K8s Native** | K8s Device Plugin（硬件级隔离） | 强隔离，由内核保障 |
| **Standalone** | 发现脚本 + flock 文件锁（协作式隔离） | 弱隔离，依赖进程合作 |
| **YARN** | YARN GPU 调度（cgroups 隔离） | 中等，依赖 YARN 版本 |

### 4.3 算子级别隔离（当前不支持）

当前 Flink 的一个限制是 **没有算子级别的 GPU 隔离**。同一个 TaskExecutor 中的所有算子可以看到相同的 GPU 设备列表。这意味着：

- 如果一个 Task Slot 运行的推理任务占满了 GPU 显存，同一 TM 的其他 Slot 的 GPU 任务会因 OOM 而失败
- GPU 计算核心（CUDA cores）是可以时间片复用的，多个任务共享不会导致错误，但会相互影响性能

目前的建议做法是：**每个 TaskManager 配置的 slot 数等于 GPU 数**，实现一对一绑定。或者使用 NVIDIA MPS（Multi-Process Service）实现更细粒度的 GPU 共享。

未来，Flink 的细粒度资源管理（FLIP-56: Dynamic Slot Allocation + FLIP-156: Fine-Grained Resource Requirements）成熟后，有望支持算子级别的 GPU 资源声明和隔离。

### 4.4 静态分配的浪费问题

除了隔离，当前 GPU 资源管理还有一个更实际的痛点：**所有 TaskManager 一视同仁地分配 GPU，造成大量浪费**。

`external-resource.gpu.amount` 是集群级别的配置，作用于每一个 TaskManager。考虑一个典型的实时推理 pipeline：

```
Kafka Source → JSON Parse → GPU Inference → Aggregation → Sink
```

五个算子中只有 GPU Inference 真正需要 GPU，但因为配置是统一的，所有 TM 启动时都会向 K8s 申请 GPU。如果这个 Job 需要 10 个 TM，那就占了 10 块 GPU，实际可能只有 2-3 块在被推理算子使用，其余 7-8 块纯粹浪费。GPU 本身又是集群中最昂贵的资源，这个问题在生产环境中尤其突出。

问题的根源在于 Flink 当前的 slot 分配模型。每个 slot 是一个"通用容器"，SlotManager 在分配时不区分 slot 的资源特性——它只看数量够不够，不看"这个 slot 有没有 GPU"。再加上用户代码只能"被动查询"GPU 信息，不能"主动声明"需求：

```java
// 现状：算子只能被动查询当前 TM 上有没有 GPU
getRuntimeContext().getExternalResourceInfos("gpu");

// 理想情况：算子主动声明自己需要 GPU（目前不支持）
@ResourceHint(externalResources = @ExternalResource(name = "gpu", amount = 1))
public class GpuInferenceFunction extends RichMapFunction<...> { ... }
```

ResourceManager 在请求 TM 时根本不知道"这个 TM 上将要跑的算子到底需不需要 GPU"，只能按全局配置给所有 TM 都带上 GPU。

**Flink 的细粒度资源管理（FLIP-56 + FLIP-156）** 正是为解决这个问题而设计的。FLIP-56（Dynamic Slot Allocation）让 TaskManager 从"预切固定 Slot"变成"持有资源池、按需动态切割"；FLIP-156 在此基础上提供了用户接口，允许在 SlotSharingGroup 级别声明不同的资源需求：

```java
// 按 SlotSharingGroup 声明差异化的资源需求
SlotSharingGroup gpuGroup = SlotSharingGroup.newBuilder("gpu-group")
    .setCpuCores(2)
    .setTaskHeapMemoryMB(1024)
    .setExternalResource("gpu", 1)   // 这个 group 需要 GPU
    .build();

SlotSharingGroup cpuGroup = SlotSharingGroup.newBuilder("cpu-group")
    .setCpuCores(1)
    .setTaskHeapMemoryMB(512)
    // 不声明 GPU——纯 CPU 计算
    .build();

// GPU 推理算子放进 gpu-group
inferenceStream.slotSharingGroup(gpuGroup);

// 其他算子放进 cpu-group
sourceStream.slotSharingGroup(cpuGroup);
sinkStream.slotSharingGroup(cpuGroup);
```

这样 ResourceManager 就能分别请求两种不同规格的 TM Pod：带 GPU 的和不带 GPU 的。只有运行推理算子的 TM 才会向 K8s 申请 GPU 资源，大幅降低 GPU 浪费。

不过 FLIP-56/156 到目前为止还没有完全落地，尤其是与 External Resource 的集成部分（详见本系列[第三篇](flink-on-k8s-fine-grained.md)的深入分析）。当前生产环境中，常见的 workaround 是 **将 GPU 推理算子拆成独立的 Flink Job**——这个 Job 的所有 TM 都配 GPU，其他非 GPU 的处理逻辑放在另一个 Job 中，两个 Job 之间通过 Kafka 串联：

```
┌─── Job 1（纯 CPU，不配 GPU）──────────────────┐
│  Kafka Source → JSON Parse → Kafka (中间 Topic) │
└─────────────────────────────────────────────────┘
                       │
                       ▼
┌─── Job 2（配 GPU，每个 TM 1 块）────────────────┐
│  Kafka (中间 Topic) → GPU Inference → Kafka Out  │
└──────────────────────────────────────────────────┘
                       │
                       ▼
┌─── Job 3（纯 CPU，不配 GPU）──────────────────┐
│  Kafka Out → Aggregation → Sink               │
└────────────────────────────────────────────────┘
```

虽然架构上多了 Kafka 的中间跳转，增加了一些延迟和运维复杂度，但避免了 GPU 的大量浪费。在 GPU 单卡成本动辄数万元的今天，这个 trade-off 通常是值得的。

---

## 第五部分：实战指南——在 K8s 上部署 GPU Flink 集群

### 5.1 前置条件

在开始之前，确保你的 K8s 集群满足以下条件：

- Kubernetes 版本 >= 1.10（支持 Device Plugin）
- GPU 节点已安装 NVIDIA 驱动
- 已部署 [NVIDIA Device Plugin](https://github.com/NVIDIA/k8s-device-plugin)（推荐以 DaemonSet 方式部署）
- 可以通过 `kubectl describe node <gpu-node>` 确认节点报告了 `nvidia.com/gpu` 资源

验证 GPU 节点状态：

```bash
$ kubectl describe node gpu-node-01 | grep -A 5 "Allocatable"
Allocatable:
  cpu:                32
  memory:             128Gi
  nvidia.com/gpu:     4
  pods:               110
```

### 5.2 构建 GPU Flink 镜像

在官方 Flink 镜像的基础上添加 GPU 相关依赖：

```dockerfile
FROM flink:1.18

# 安装 CUDA 运行时（根据你的 GPU 驱动版本选择）
# 注意：如果使用 NVIDIA Device Plugin，容器内不需要完整的 CUDA Toolkit
# 只需要运行时库
USER root

# 复制 GPU 发现脚本
COPY gpu-discovery.sh /opt/flink/plugins/gpu/
RUN chmod +x /opt/flink/plugins/gpu/gpu-discovery.sh

# 复制你的 ML 推理依赖（例如 ONNX Runtime GPU、TensorRT Java binding 等）
COPY ml-libs/ /opt/flink/lib/

# 复制你的 Flink Job JAR
COPY my-gpu-job.jar /opt/flink/usrlib/

USER flink
```

如果你使用 Python 进行 GPU 推理（PyFlink），可以基于 NVIDIA 的 CUDA 镜像：

```dockerfile
FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

# 安装 Java 和 Flink
RUN apt-get update && apt-get install -y openjdk-11-jre-headless python3 python3-pip
COPY flink-1.18/ /opt/flink/

# 安装 Python GPU 推理库
RUN pip3 install apache-flink onnxruntime-gpu torch --extra-index-url https://download.pytorch.org/whl/cu121

COPY gpu-discovery.sh /opt/flink/plugins/gpu/
RUN chmod +x /opt/flink/plugins/gpu/gpu-discovery.sh

ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
```

### 5.3 Flink 配置

`flink-conf.yaml` 中添加 GPU 相关配置：

```yaml
# ===== GPU 外部资源配置 =====
external-resources: gpu
external-resource.gpu.amount: 1
external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory
external-resource.gpu.param.discovery-script.path: /opt/flink/plugins/gpu/gpu-discovery.sh
external-resource.gpu.kubernetes.config-key: nvidia.com/gpu

# ===== TaskManager 资源配置 =====
taskmanager.numberOfTaskSlots: 1    # 建议与 GPU 数量一致
taskmanager.memory.process.size: 8192m
taskmanager.cpu.cores: 4

# ===== K8s 特有配置 =====
kubernetes.container.image: my-registry/flink-gpu:1.18
kubernetes.namespace: flink-gpu
kubernetes.service-account: flink
```

### 5.4 使用 Pod Template 进行高级定制

某些场景下你需要对 TM Pod 进行更精细的定制（如挂载共享内存、设置 GPU 拓扑亲和性）。Flink 支持通过 Pod Template 实现：

```yaml
# pod-template.yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    gpu-workload: "true"
spec:
  # 增大共享内存（某些 GPU 框架需要）
  volumes:
    - name: dshm
      emptyDir:
        medium: Memory
        sizeLimit: 8Gi
    - name: model-cache
      hostPath:
        path: /data/model-cache
        type: DirectoryOrCreate
  containers:
    - name: flink-main-container
      volumeMounts:
        - name: dshm
          mountPath: /dev/shm
        - name: model-cache
          mountPath: /models
      env:
        - name: NVIDIA_DRIVER_CAPABILITIES
          value: "compute,utility"
        - name: CUDA_CACHE_PATH
          value: "/tmp/cuda_cache"
  # 调度到 GPU 节点
  nodeSelector:
    accelerator: nvidia-gpu
  tolerations:
    - key: "nvidia.com/gpu"
      operator: "Exists"
      effect: "NoSchedule"
```

提交 Application Mode 集群时指定 Pod Template：

```bash
./bin/flink run-application \
    --target kubernetes-application \
    -Dkubernetes.cluster-id=gpu-inference-app \
    -Dkubernetes.container.image=my-registry/flink-gpu:1.18 \
    -Dkubernetes.namespace=flink-gpu \
    -Dkubernetes.pod-template-file=pod-template.yaml \
    -Dexternal-resources=gpu \
    -Dexternal-resource.gpu.amount=1 \
    -Dexternal-resource.gpu.driver-factory.class=org.apache.flink.externalresource.gpu.GPUDriverFactory \
    -Dexternal-resource.gpu.param.discovery-script.path=/opt/flink/plugins/gpu/gpu-discovery.sh \
    -Dexternal-resource.gpu.kubernetes.config-key=nvidia.com/gpu \
    local:///opt/flink/usrlib/my-gpu-job.jar
```

### 5.5 验证 GPU 分配

集群启动后，可以通过以下方式验证 GPU 是否正确分配：

```bash
# 查看 TM Pod 的资源请求
$ kubectl get pod -n flink-gpu -l component=taskmanager -o jsonpath='{
    .items[*].spec.containers[*].resources}'
{"limits":{"cpu":"4","memory":"8192Mi","nvidia.com/gpu":"1"},
 "requests":{"cpu":"4","memory":"8192Mi","nvidia.com/gpu":"1"}}

# 进入 TM Pod 验证 GPU 可见性
$ kubectl exec -it -n flink-gpu <tm-pod-name> -- nvidia-smi
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 535.129.03   Driver Version: 535.129.03   CUDA Version: 12.2     |
|-------------------------------+----------------------+----------------------+
| GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
| Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
|===============================+======================+======================|
|   0  Tesla T4            On   | 00000000:00:1E.0 Off |                    0 |
| N/A   35C    P8    10W /  70W |      0MiB / 15360MiB |      0%      Default |
+-------------------------------+----------------------+----------------------+
```

---

## 第六部分：生产环境最佳实践

### 6.1 资源规划

GPU 资源昂贵，合理规划至关重要：

| 配置项 | 建议 | 说明 |
|--------|------|------|
| `external-resource.gpu.amount` | 1 | 除非你的算子能利用多 GPU 并行推理，否则 1 块就够 |
| `taskmanager.numberOfTaskSlots` | 等于 GPU 数量 | 避免多个 Slot 竞争 GPU 显存 |
| `taskmanager.memory.process.size` | 充足的堆外内存 | GPU 推理框架通常需要较多的 off-heap 内存做数据搬运 |
| Pod Template `nodeSelector` | 指定 GPU 节点标签 | 避免调度到非 GPU 节点导致 Pending |
| Pod Template `tolerations` | 容忍 GPU 节点的 taint | 很多集群对 GPU 节点设置 NoSchedule taint |

### 6.2 GPU 显存管理

GPU 推理的显存管理是一个常见的坑：

**预分配策略**：TensorFlow 默认会吃掉整块 GPU 的显存。如果你在同一个 TM 中运行多个推理任务，务必限制每个任务的显存用量。对于 TensorFlow：

```python
import tensorflow as tf
gpus = tf.config.experimental.list_physical_devices('GPU')
for gpu in gpus:
    tf.config.experimental.set_memory_growth(gpu, True)
```

对于 PyTorch，默认按需分配，通常不需要特殊处理。但建议设置 `PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:512` 避免显存碎片化。

**ONNX Runtime** 推理时可以精确控制显存：

```java
OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
OrtCUDAProviderOptions cudaOpts = new OrtCUDAProviderOptions();
cudaOpts.add("device_id", gpuIndex);
cudaOpts.add("gpu_mem_limit", String.valueOf(4L * 1024 * 1024 * 1024)); // 4GB
opts.addCUDA(cudaOpts);
```

### 6.3 故障恢复

GPU 相关的故障场景和处理方式：

**GPU 硬件故障（ECC 错误、掉卡）**：TM Pod 中的 GPU 任务会失败，Pod 被标记为 Failed。Flink RM 检测到 `onPodTerminated()` 事件后，会请求新的 TM Pod。新 Pod 会被 K8s 调度到其他有可用 GPU 的 Node 上。这个过程是自动的，无需人工干预。

**GPU 节点资源耗尽**：如果集群中所有 GPU 节点的 GPU 都被占满，新的 TM Pod 会处于 Pending 状态。建议配合 K8s Cluster Autoscaler 自动扩容 GPU 节点。

**GPU OOM（显存溢出）**：这是应用层的错误，需要优化模型大小或 batch size。Flink 的 checkpoint 机制可以保证状态不丢失，重启后从最近的 checkpoint 恢复。

### 6.4 监控

生产环境务必建立完善的 GPU 监控体系：

- **DCGM Exporter + Prometheus + Grafana**：NVIDIA 官方的 GPU 监控方案，以 DaemonSet 方式部署，采集 GPU 利用率、显存使用率、温度、功耗等指标
- **Flink Metrics**：在你的 GPU 算子中自定义 metrics，如推理延迟（p99/p50）、batch 吞吐量、GPU 利用率
- **告警规则**：GPU 利用率持续低于 20%（资源浪费）、显存使用率超过 90%（OOM 风险）、GPU 温度超过 85°C（硬件风险）

---

## 第七部分：AMD GPU 与多厂商支持

虽然 NVIDIA 是 GPU 推理的主流选择，Flink 的 External Resource Framework 也支持 AMD GPU：

```properties
# AMD GPU 配置
external-resources: gpu
external-resource.gpu.amount: 1
external-resource.gpu.driver-factory.class: org.apache.flink.externalresource.gpu.GPUDriverFactory
external-resource.gpu.param.discovery-script.path: /opt/flink/plugins/gpu/gpu-discovery.sh

# 关键区别：K8s resource key 不同
external-resource.gpu.kubernetes.config-key: amd.com/gpu
```

AMD GPU 在 K8s 上需要部署 [AMD GPU Device Plugin](https://github.com/ROCm/k8s-device-plugin)，发现脚本使用 `rocm-smi` 替代 `nvidia-smi`。Flink 自带的默认发现脚本已经内置了对两种 GPU 的支持——它会先尝试 `nvidia-smi`，如果失败再尝试 `rocm-smi`。

---

## 总结

Flink on Kubernetes 的 GPU 支持是一个多层协作的架构：

**框架层**——FLIP-108 的 External Resource Framework 提供了可扩展的抽象。它不仅支持 GPU，还可以扩展到任何外部硬件资源。Driver 的可插拔设计使得 Flink 不需要了解具体的 GPU 硬件细节。

**调度层**——Flink 将 GPU 需求翻译成 K8s 的 Extended Resource 请求，具体的 GPU 分配和调度完全委托给 K8s Scheduler 和 Device Plugin。这与上篇中 CPU/Memory 的处理逻辑一脉相承：Flink 只声明需求，K8s 负责满足。

**隔离层**——K8s 的 Device Plugin 机制提供了 Pod 级别的硬件级 GPU 隔离。相比 Standalone 模式下基于文件锁的协作式隔离，这是质的飞跃。

**应用层**——通过 `RuntimeContext.getExternalResourceInfos("gpu")`，任意 Flink 算子都可以感知 GPU，并在代码中直接使用 CUDA 进行推理加速。

从上篇到本篇，我们可以看到 Flink on K8s 架构设计的一致性：**声明式资源请求、关注点分离、装饰器模式构建 Pod Spec、Driver 抽象屏蔽底层差异**。GPU 支持并不是一个"补丁"，而是架构中自然生长出来的能力。

> **参考资料**
> - [FLIP-108: Add GPU support in Flink](https://cwiki.apache.org/confluence/display/FLINK/FLIP-108%3A+Add+GPU+support+in+Flink)
> - [NVIDIA Device Plugin for Kubernetes](https://github.com/NVIDIA/k8s-device-plugin)
> - [Flink External Resource Framework 官方文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/advanced/external_resources/)
> - [深入理解 Flink on Kubernetes（一）：从原理到源码的启动全流程](flink-on-k8s.md)
> - Flink 源码 `flink-kubernetes` 模块与 `flink-external-resource-gpu` 模块
