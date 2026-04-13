# 深入理解 Flink on Kubernetes：从原理到源码的启动全流程

> 本文基于 Apache Flink 源码，结合 Kubernetes 基本原理，深入剖析 Flink 如何在 K8s 集群上启动和运行。

---

## 第一部分：Kubernetes 基本原理与资源调度

### 1.1 Kubernetes 架构概览

Kubernetes（K8s）是一个容器编排平台，它管理一组物理或虚拟机器（Node），并在其上按需调度容器化的工作负载。核心架构分为两层：

- **Control Plane（控制平面）**：包括 API Server、etcd、Controller Manager、Scheduler
- **Data Plane（数据平面）**：由多个 Worker Node 组成，每个 Node 上运行 kubelet 和容器运行时

### 1.2 K8s 的资源管理与调度

K8s 并不预先为应用划分"资源池"，而是采用 **声明式 + 按需调度** 的模型：

1. **用户声明资源需求**：在 Pod Spec 中通过 `resources.requests` 和 `resources.limits` 声明 CPU、Memory 以及扩展资源（如 `nvidia.com/gpu`）
2. **kube-scheduler 负责调度**：收到 Pod 创建请求后，Scheduler 通过 **Filter → Score → Bind** 三阶段找到最优 Node
   - **Filter 阶段**：排除资源不足、nodeSelector 不匹配、taint 不容忍的 Node
   - **Score 阶段**：对剩余 Node 打分排序（如资源均衡性、亲和性）
   - **Bind 阶段**：将 Pod 绑定到最高分的 Node
3. **如果没有满足条件的 Node，Pod 将处于 Pending 状态**，直到有 Node 释放资源或新 Node 加入

### 1.3 核心资源对象：Pod vs Deployment

| 对象 | 说明 |
|------|------|
| **Pod** | K8s 最小调度单元，一个或多个容器的集合，生命周期短暂 |
| **Deployment** | 高层抽象，管理一组 Pod 的副本，提供滚动更新、自动恢复、副本管理 |

关键区别：Deployment 内嵌了 ReplicaSet，当 Pod 挂掉时 K8s **自动重启**；而裸 Pod 挂了就没了，需要外部控制器负责重建。这个区别是理解 Flink on K8s 设计的关键。

### 1.4 扩展资源（Extended Resources）

K8s 天然支持 CPU 和 Memory，对于 GPU 等特殊硬件，通过 **[Extended Resources](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/)** 机制支持：

```yaml
resources:
  requests:
    nvidia.com/gpu: 1
  limits:
    nvidia.com/gpu: 1
```

设备插件（Device Plugin）向 kubelet 注册可用的 GPU 数量，Scheduler 在调度时自动匹配有 GPU 的 Node。关于 Flink 如何利用 K8s 的 Extended Resources 机制实现 GPU 调度与隔离，详见本系列的第二篇：[深入理解 Flink on Kubernetes（二）：GPU 加速从原理到实战](flink-on-k8s-gpu.md)。

---

## 第二部分：Flink Native Kubernetes 整体架构

### 2.1 什么是 "Native" 集成？

Flink on K8s 有两种方式：**Standalone**（用户自己写 YAML 部署）和 **Native**（Flink 自己管理 K8s 资源）。Native 模式的核心含义是：

1. **Flink 内嵌 K8s 客户端**：无需 kubectl 等外部工具，Flink Client 直接调用 K8s API Server 创建 JobManager Deployment
2. **动态资源分配**：Flink 的 ResourceManager 根据 Job 需要的 slot 数量，按需向 K8s 申请/释放 TaskManager Pod
3. **配置自动下发**：Client 端的配置（flink-conf、log4j、Hadoop 配置等）通过 ConfigMap 挂载到 Pod 中

### 2.2 两种部署模式

**Session Mode**：先创建一个长期运行的 Flink 集群，多个 Job 共享该集群资源。入口类为 `KubernetesSessionClusterEntrypoint`。

**Application Mode**：每个应用独占一个 Flink 集群，应用的 `main()` 方法在 JobManager 中执行，集群生命周期与 Job 绑定。入口类为 `KubernetesApplicationClusterEntrypoint`。

两种模式在 **底层资源管理上完全相同**，都使用 `KubernetesResourceManagerFactory` 创建 `ActiveResourceManager`。

### 2.3 整体启动流程

```
                          ┌─────────────────────────────┐
                          │     Flink Client (用户侧)     │
                          └──────────┬──────────────────┘
                                     │ 1. 调用 KubernetesClusterDescriptor
                                     │    .deploySessionCluster() 或
                                     │    .deployApplicationCluster()
                                     ▼
                          ┌─────────────────────────────┐
                          │    K8s API Server            │
                          └──────────┬──────────────────┘
                                     │ 2. 创建 JM Deployment + Service + ConfigMap
                                     ▼
                          ┌─────────────────────────────┐
                          │  JobManager Pod (由 K8s 启动) │
                          │  运行 ResourceManager        │
                          └──────────┬──────────────────┘
                                     │ 3. RM 根据 Job 需求
                                     │    调用 requestResource()
                                     ▼
                     ┌───────────────────────────────────────┐
                     │  TaskManager Pod 1  │  TM Pod 2  │ ...│
                     │  (由 Flink RM 按需创建裸 Pod)          │
                     └───────────────────────────────────────┘
```

### 2.4 统一的 Resource Manager 抽象

Flink 设计了一套优雅的分层抽象，使得 K8s、YARN 等不同资源管理系统可以复用同一套上层逻辑：

```
ResourceManagerFactory (抽象)
  └── ActiveResourceManagerFactory<WorkerType> (抽象, flink-runtime 层)
        ├── KubernetesResourceManagerFactory   → 创建 KubernetesResourceManagerDriver
        └── YarnResourceManagerFactory         → 创建 YarnResourceManagerDriver

ResourceManagerDriver (接口)
  └── AbstractResourceManagerDriver (抽象基类)
        ├── KubernetesResourceManagerDriver    → 调 K8s API 创建/删除 Pod
        └── YarnResourceManagerDriver          → 调 YARN API 创建/释放 Container
```

`ActiveResourceManager` 是运行时核心，它持有一个 `ResourceManagerDriver`，通过 `requestResource()` / `releaseResource()` 接口与底层资源系统交互。不同的 K8s、YARN 实现只需要提供自己的 Driver 即可。

### 2.5 资源规格翻译

Flink 的抽象资源需求 (`TaskExecutorProcessSpec`) 需要翻译成 K8s 能理解的 `ResourceRequirements`。这个翻译在 `KubernetesUtils.getResourceRequirements()` 中完成：

```java
// CPU
Quantity cpuQuantity = new Quantity(String.valueOf(cpu));
Quantity cpuLimit    = new Quantity(String.valueOf(cpu * cpuLimitFactor));
// Memory
Quantity memQuantity = new Quantity(mem + "Mi");
Quantity memLimit    = new Quantity(((int)(mem * memoryLimitFactor)) + "Mi");
// GPU 等外部资源
for (ExternalResource resource : externalResources) {
    // 例如 configKey = "nvidia.com/gpu", value = 1
    builder.addToRequests(configKey, quantity).addToLimits(configKey, quantity);
}
```

最终生成的 Pod Spec 中的 resources 部分类似：

```yaml
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

**Flink 并不会预先查询 K8s 集群是否存在满足条件的 Node**。它直接提交 Pod，由 K8s Scheduler 负责调度。如果没有合适的 Node，Pod 会处于 Pending 状态。Flink 通过 Pod Watch 机制监听结果：`onPodScheduled()` 表示成功，`onPodTerminated()` 表示失败。

---

## 第三部分：JM 与 TM 的生命周期——Deployment vs Bare Pod

这是 Flink on K8s 架构中最精妙的设计决策之一：**JobManager 使用 Kubernetes Deployment，而 TaskManager 使用裸 Pod（Bare Pod）**。

### 3.1 JobManager 的生命周期（Deployment）

#### 创建过程

JM 的创建发生在 **集群启动阶段**，由 Client 端的 `KubernetesClusterDescriptor.deployClusterInternal()` 驱动：

```
KubernetesClusterDescriptor.deployClusterInternal()
  │
  ├── 1. 创建 KubernetesJobManagerParameters（包含 CPU、Memory、Replicas 等）
  │
  ├── 2. 加载 Pod Template（用户自定义模板，可选）
  │
  ├── 3. KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification()
  │      通过装饰器链逐步构建：
  │      InitJobManagerDecorator      → 设置 CPU/Memory/ServiceAccount/NodeSelector
  │      EnvSecretsDecorator          → 注入 Secret 环境变量
  │      MountSecretsDecorator        → 挂载 Secret Volume
  │      CmdJobManagerDecorator       → 设置启动命令
  │      InternalServiceDecorator     → 创建内部 Service (RPC 通信)
  │      ExternalServiceDecorator     → 创建外部 Service (REST 端点)
  │      FlinkConfMountDecorator      → 挂载 flink-conf ConfigMap
  │      PodTemplateMountDecorator    → 挂载 Pod Template
  │
  ├── 4. 包装成 Kubernetes Deployment 对象
  │
  └── 5. 调用 Fabric8FlinkKubeClient.createJobManagerComponent()
         同步提交 Deployment + 附属资源（Service、ConfigMap）到 K8s API
```

生成的 Deployment 结构（简化）：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {clusterId}          # 例如 "my-flink-cluster"
  labels:
    type: flink-native-kubernetes
    app: {clusterId}
    component: jobmanager
spec:
  replicas: 1                 # HA 模式下可以 > 1
  selector:
    matchLabels: { app: {clusterId}, component: jobmanager }
  template:
    spec:
      serviceAccountName: flink
      containers:
        - name: flink-main-container
          image: flink:1.18
          command: ["/docker-entrypoint.sh"]
          args: ["bash", "-c", "kubernetes-jobmanager.sh kubernetes-session"]
          resources:
            requests: { cpu: "1", memory: "1600Mi" }
            limits:   { cpu: "1", memory: "1600Mi" }
          ports:
            - { name: rest, containerPort: 8081 }
            - { name: jobmanager-rpc, containerPort: 6123 }
            - { name: blob-server, containerPort: 6124 }
```

#### 为什么用 Deployment？

| 特性 | 说明 |
|------|------|
| **自动恢复** | JM Pod 崩溃后，Deployment Controller 自动重建，保证集群控制平面可用 |
| **HA 支持** | 通过 `getReplicas() > 1`，可以运行多个 JM 实例进行 leader election |
| **级联删除** | 删除 Deployment 时，所有关联的 ReplicaSet、Pod、以及以它为 OwnerReference 的 TM Pod 都会被 K8s GC 回收 |
| **滚动更新** | 未来可支持 JM 的版本升级 |

#### 启动链

JM Pod 启动后的代码执行链：

```
/docker-entrypoint.sh
  └── kubernetes-jobmanager.sh kubernetes-session (或 kubernetes-application)
        └── flink-console.sh kubernetes-session
              └── Java Main: KubernetesSessionClusterEntrypoint.main()
                    (或 KubernetesApplicationClusterEntrypoint.main())
                    │
                    ├── 启动 Dispatcher（接收 Job 提交）
                    ├── 启动 ResourceManager（资源分配）
                    └── 启动 WebMonitorEndpoint（REST API）
```

### 3.2 TaskManager 的生命周期（Bare Pod）

#### 创建过程

TM 的创建发生在 **运行时**，由 JM 内部的 `KubernetesResourceManagerDriver` 驱动。当 SlotManager 判断需要更多 slot 时，触发以下流程：

```
ActiveResourceManager: "需要更多 slot"
  │
  └── KubernetesResourceManagerDriver.requestResource(TaskExecutorProcessSpec)
        │
        ├── 1. 生成 Pod 名称: "{clusterId}-taskmanager-{attemptId}-{podIndex}"
        │
        ├── 2. 创建 KubernetesTaskManagerParameters（CPU、Memory、GPU、JVM 参数）
        │
        ├── 3. KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod()
        │      装饰器链：
        │      InitTaskManagerDecorator  → CPU/Memory/NodeSelector/Tolerations/Affinity
        │      EnvSecretsDecorator       → Secret 环境变量
        │      MountSecretsDecorator     → Secret 挂载
        │      CmdTaskManagerDecorator   → 启动命令 + JVM 内存参数
        │      FlinkConfMountDecorator   → flink-conf ConfigMap
        │
        ├── 4. 构建原生 Pod 对象（不是 Deployment！）
        │
        └── 5. Fabric8FlinkKubeClient.createTaskManagerPod()
               异步提交裸 Pod 到 K8s API
               设置 OwnerReference → JM Deployment（级联删除）
```

生成的 Pod 结构（简化）：

```yaml
apiVersion: v1
kind: Pod                     # 注意：是 Pod，不是 Deployment
metadata:
  name: my-flink-cluster-taskmanager-1-1
  labels:
    type: flink-native-kubernetes
    app: my-flink-cluster
    component: taskmanager
  ownerReferences:             # 关键：指向 JM Deployment
    - apiVersion: apps/v1
      kind: Deployment
      name: my-flink-cluster
      uid: xxx
spec:
  restartPolicy: Never         # 关键：不自动重启
  containers:
    - name: flink-main-container
      image: flink:1.18
      command: ["/docker-entrypoint.sh"]
      args: ["bash", "-c", "kubernetes-taskmanager.sh <dynamic-properties> <resource-args>"]
      resources:
        requests: { cpu: "2", memory: "4096Mi", nvidia.com/gpu: "1" }
        limits:   { cpu: "2", memory: "4096Mi", nvidia.com/gpu: "1" }
      env:
        - name: FLINK_TM_JVM_MEM_OPTS
          value: "-Xmx1024m -Xms1024m -XX:MaxDirectMemorySize=512m ..."
```

#### 为什么用裸 Pod 而不是 Deployment？

这是一个关键的设计决策，原因有三：

**1. Flink 本身就是 Controller**

Flink 的 ResourceManager 扮演了 K8s Deployment Controller 的角色。它决定何时创建、何时销毁 TM Pod。如果用 Deployment，K8s 会自动重启被 Flink 主动杀掉的 TM，造成冲突。源码中明确设置了：

```java
// InitTaskManagerDecorator.java
.withRestartPolicy(Constants.RESTART_POLICY_OF_NEVER)
```

**2. 异构资源需求**

不同的 Job 可能需要不同规格的 TM（例如有的需要 GPU，有的不需要）。每个 TM Pod 的 `TaskExecutorProcessSpec` 可能不同。Deployment 要求同一模板下所有副本规格一致，无法满足这种灵活性。

**3. 精确的生命周期控制**

TM Pod 的完整生命周期：

```
创建 (requestResource)
  │
  ├── K8s Scheduler 调度到某个 Node
  │     └── onPodScheduled() → 完成 requestResourceFuture
  │
  ├── TM 启动，向 RM 注册，提供 slot
  │
  ├── 执行 Task
  │
  └── 释放 (releaseResource)
        └── stopPod() → 删除 Pod
```

如果 TM Pod 异常终止：
```
Pod 异常终止
  └── onPodTerminated()
        ├── 通知 ResourceEventHandler
        ├── 清理 requestResourceFutures
        └── RM 决定是否重新 requestResource() 创建新 Pod
```

#### 启动链

TM Pod 启动后的代码执行链：

```
/docker-entrypoint.sh
  └── kubernetes-taskmanager.sh <dynamic-properties> <resource-args>
        └── flink-console.sh kubernetes-taskmanager
              └── Java Main: KubernetesTaskExecutorRunner.main()
                    │
                    ├── 加载配置
                    ├── 获取 Pod 所在 Node ID (环境变量)
                    └── TaskManagerRunner.runTaskManagerProcessSecurely()
                          │
                          ├── 启动 TaskExecutor
                          ├── 向 ResourceManager 注册
                          └── 提供 Slot 给 JobManager
```

### 3.3 JM 与 TM 的所有权关系

```
Kubernetes Deployment (JM)  ──owns──>  Service (REST)
        │                   ──owns──>  Service (Internal RPC)
        │                   ──owns──>  ConfigMap (flink-conf)
        │
        └──── OwnerReference ────>  Pod (TM-1)
                             ────>  Pod (TM-2)
                             ────>  Pod (TM-3)
```

当整个 Flink 集群需要销毁时（如 Job 取消），只需删除 JM Deployment，K8s 的 Garbage Collection 机制会自动级联删除所有 TM Pod 和附属资源。

### 3.4 对比总结

| 维度 | JobManager (Deployment) | TaskManager (Bare Pod) |
|------|------------------------|----------------------|
| K8s 资源类型 | `apps/v1 Deployment` | `v1 Pod` |
| 创建时机 | 集群启动时，由 Client 端提交 | 运行时按需，由 JM 中的 RM 动态创建 |
| 创建方式 | 同步，`createJobManagerComponent()` | 异步，`createTaskManagerPod()` 返回 `CompletableFuture` |
| 生命周期管理者 | K8s Deployment Controller | Flink ResourceManager |
| restartPolicy | 由 Deployment 控制（Always） | `Never`——不自动重启 |
| 副本管理 | 支持多副本 HA（`replicas >= 1`） | 每个 Pod 独立，资源规格可能不同 |
| 异常恢复 | K8s 自动重建 Pod | Flink RM 决定是否创建新 Pod |
| OwnerReference | 被附属资源引用 | 引用 JM Deployment（级联删除） |
| 附属资源 | Service、ConfigMap 等 | 无 |
| 工厂类 | `KubernetesJobManagerFactory` | `KubernetesTaskManagerFactory` |

---

## 总结

Flink on Kubernetes 的 Native 集成是一个精心设计的架构：

1. **统一的 Driver 抽象**：通过 `ResourceManagerDriver` 接口，K8s、YARN 等不同资源系统共享同一套 `ActiveResourceManager` 上层逻辑，只需各自实现 Driver。

2. **JM 用 Deployment、TM 用 Bare Pod**：这不是随意选择，而是深思熟虑的决策——JM 需要 K8s 保障其高可用，TM 则需要 Flink 精确控制其弹性伸缩。

3. **声明式资源请求**：Flink 将自己的资源需求（CPU、Memory、GPU）翻译成 K8s 的 `ResourceRequirements`，具体的调度完全交给 K8s Scheduler，Flink 不关心也不需要知道底层有哪些 Node。

4. **优雅的装饰器模式**：Pod Spec 通过 `InitDecorator → SecretDecorator → CmdDecorator → ConfDecorator` 等一系列装饰器逐步构建，每个装饰器职责单一，易于扩展。

如果你想要进一步学习 K8s Scheduler 的调度代码，可以直接阅读 Kubernetes 源码仓库 `pkg/scheduler/` 目录，尤其是 `framework/plugins/noderesources` 插件，它负责根据资源 requests/limits 进行 Node 筛选。关于 GPU 加速和细粒度资源管理的深入分析，请继续阅读本系列的[第二篇：GPU 加速](flink-on-k8s-gpu.md)和[第三篇：细粒度资源管理](flink-on-k8s-fine-grained.md)。

> **参考资料**
> - [How to natively deploy Flink on Kubernetes with HA](https://flink.apache.org/2021/02/10/how-to-natively-deploy-flink-on-kubernetes-with-high-availability-ha/)
> - [Apache Flink 进阶（四）：Flink on Yarn/K8s 原理剖析及实践](https://tianchi.aliyun.com/forum/post/78949)
> - Flink 源码 `flink-kubernetes` 模块
> - [k8s device-plugins](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/)
