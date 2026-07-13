---
title: "Kubernetes Operator 学习指南"
date: 2026-05-14
draft: false
tags: ["Kubernetes", "Operator", "CRD"]
categories: ["Kubernetes"]
---

## Kubernetes Operator 学习指南

### 一、什么是 Kubernetes Operator？

要理解 Operator，首先需要回顾 Kubernetes 的核心设计哲学：**声明式管理**。你告诉 Kubernetes "我想要什么状态"（比如"我想要 3 个 Pod 在运行"），Kubernetes 的 **控制器（Controller)** 会持续工作，确保集群的实际状态与你声明的期望状态保持一致。这个不断检查并修正的过程叫做 **Reconciliation Loop（调谐循环）**。

Kubernetes 内置的控制器能管理 Pod、Deployment、Service 这些通用资源（Common Resource)，但如果你想管理像数据库、消息队列这种有复杂运维逻辑的有状态应用呢？这就是 Operator 诞生的原因。

**Operator = Custom Resource Definition (CRD) + Custom Controller**

简单来说，Operator 是一种打包、部署和管理 Kubernetes 应用的方式。它将人类运维工程师的领域知识（比如如何扩缩容一个 Kafka 集群、如何做 Flink 作业的 Savepoint）编码成软件，让 Kubernetes 能自动完成这些复杂操作。

### 二、核心概念拆解

#### 2.1 Custom Resource Definition (CRD)

CRD 是 Kubernetes 提供的扩展机制，允许你定义自己的"资源类型"。比如 Kafka Operator 定义了 `Kafka`、`KafkaTopic`、`KafkaUser` 等自定义资源，Flink Operator 定义了 `FlinkDeployment`、`FlinkSessionJob` 等。

定义好 CRD 后，你就可以像操作原生 Kubernetes 资源一样，用 `kubectl apply` 来声明你期望的状态：

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: my-flink-job
spec:
  flinkVersion: v1_17
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
```

#### 2.2 Custom Controller（自定义控制器）

控制器是 Operator 的"大脑"。它运行在 Kubernetes 集群中，持续监听（Watch）自定义资源的变化，并通过 Reconciliation Loop 确保实际状态与期望状态一致。

工作流程如下：

1. **Watch（监听）**：控制器通过 Kubernetes API Server 监听 CRD 资源的创建、更新、删除事件。
2. **Diff（比较）**：将当前实际状态与用户声明的期望状态进行对比。
3. **Act（行动）**：执行必要操作（创建 Pod、修改配置、触发 Savepoint 等）来消除差异。
4. **Repeat（循环）**：回到第 1 步，持续运行。

#### 2.3 Operator 能做什么？

对比手动运维，Operator 可以自动化完成：

- 应用部署与配置管理
- 滚动升级与版本回退
- 自动扩缩容
- 故障检测与自动恢复
- 备份与恢复（Savepoint / Snapshot）
- 安全配置（TLS 证书轮转、用户认证）

### 三、实例：Flink Kubernetes Operator

Apache Flink Kubernetes Operator 让你可以用声明式的方式在 Kubernetes 上管理 Flink 应用的完整生命周期。

#### 核心架构

```
用户 (kubectl apply FlinkDeployment YAML)
        │
        ▼
┌─────────────────────────┐
│   Kubernetes API Server │
└─────────────────────────┘
        │  Watch 事件
        ▼
┌─────────────────────────┐
│  Flink Kubernetes       │
│  Operator Controller    │
│  (基于 Java Operator SDK)│
└─────────────────────────┘
        │  创建/管理
        ▼
┌─────────────────────────┐
│  Flink Cluster          │
│  (JobManager + TaskManager Pods) │
└─────────────────────────┘
```

#### 关键 CRD

- **FlinkDeployment**：定义一个完整的 Flink 集群及其作业（Application 模式或 Session 模式）。
- **FlinkSessionJob**：向已有的 Session 集群提交作业。

#### Operator 自动管理的运维操作

- 作业提交与监控
- Savepoint 触发与恢复
- 作业失败自动重启
- 滚动升级（先做 Savepoint，再用新版本恢复）
- 资源自动调优（Autoscaler）

#### 示例工作流

当你修改了 FlinkDeployment 的 YAML（比如增加 parallelism），Operator 的 Reconcile 逻辑大致如下：

1. 检测到 FlinkDeployment spec 变更
2. 对当前运行的 Flink 作业触发 Savepoint
3. 等待 Savepoint 完成
4. 停止旧的 Flink 集群
5. 用新配置创建新的 Flink 集群
6. 从 Savepoint 恢复作业

### 四、实例：Strimzi — Kafka on Kubernetes

Strimzi 是目前最流行的 Kafka Kubernetes Operator 实现，由 CNCF 孵化。

#### 核心架构

Strimzi 由多个 Operator 协同工作：

- **Cluster Operator**：管理 Kafka 集群、ZooKeeper（或 KRaft）、Kafka Connect、Mirror Maker 等。
- **Topic Operator**：管理 Kafka Topic 的创建、配置变更。
- **User Operator**：管理 Kafka 用户的认证与授权。

#### 关键 CRD

- `Kafka`：定义完整的 Kafka 集群（Broker + ZooKeeper/KRaft）
- `KafkaTopic`：声明式管理 Topic
- `KafkaUser`：声明式管理用户权限
- `KafkaConnect`：定义 Kafka Connect 集群
- `KafkaMirrorMaker2`：跨集群数据复制

#### 声明式 Kafka 集群示例

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    storage:
      type: persistent-claim
      size: 100Gi
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 100Gi
```

#### Operator 自动管理的运维操作

- Broker 滚动升级（逐个重启，确保零停机）
- 分区重平衡（Cruise Control 集成）
- TLS 证书自动签发与轮转
- 用户认证（SCRAM-SHA / mTLS）与 ACL 管理
- 集群扩缩容（增减 Broker 并自动迁移分区）

### 五、Operator 开发框架

如果你将来想自己开发 Operator，以下是主流的开发框架：

| 框架 | 语言 | 适用场景 |
|------|------|----------|
| **Operator SDK** (Red Hat) | Go / Ansible / Helm | 最成熟的 Go 生态方案 |
| **Kubebuilder** | Go | Operator SDK 的底层基础 |
| **Java Operator SDK (JOSDK)** | Java | Flink Operator 使用此框架 |
| **Kopf** | Python | 适合快速原型 |
| **Metacontroller** | 任意语言 | 通过 Webhook 方式，语言无关 |

### 六、学习路径建议

作为初学者，建议按以下顺序推进：

1. 先确保熟悉 Kubernetes 基础（Pod、Deployment、Service、ConfigMap）
2. 理解 Controller 的 Watch + Reconcile 模式
3. 用 Minikube 或 Kind 在本地搭建集群，部署一个现成的 Operator（比如 Strimzi）体验一下
4. 阅读 Operator SDK 官方教程，尝试用 Go 写一个简单的 Operator
5. 深入阅读 Flink Operator 或 Strimzi 的源码

### 七、参考文档与资料

**Kubernetes 官方**

- [Operator Pattern 概念](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)
- [Custom Resources 文档](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)

**Flink Kubernetes Operator**

- [官方文档（最新稳定版）](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
- [GitHub 仓库](https://github.com/apache/flink-kubernetes-operator)
- [Flink Operator 文档入口](https://flink.apache.org/documentation/flink-kubernetes-operator-stable/)

**Strimzi (Kafka Operator)**

- [Strimzi 官网](https://strimzi.io/)
- [Strimzi 文档中心](https://strimzi.io/documentation/)
- [Strimzi Overview（架构总览）](https://strimzi.io/docs/operators/latest/overview)
- [GitHub 仓库](https://github.com/strimzi/strimzi-kafka-operator)

**Operator 开发**

- [Operator SDK 官方文档](https://sdk.operatorframework.io/docs/)
- [Kubebuilder Book](https://book.kubebuilder.io/)
- [Java Operator SDK](https://javaoperatorsdk.io/)
- [OperatorHub.io（浏览已有 Operator）](https://operatorhub.io/)

**入门教程推荐**

- [Red Hat: Kubernetes Operators 101](https://developers.redhat.com/articles/2021/06/22/kubernetes-operators-101-part-2-how-operators-work)
- [CNCF: Operator 白皮书](https://github.com/cncf/tag-app-delivery/blob/main/operator-wg/whitepaper/Operator-WhitePaper_v1-0.md)
