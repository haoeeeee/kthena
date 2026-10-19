# 推理负载故障快速恢复

大模型推理任务通常由多个 Pod 协同运行。一个 Role 实例可以包含一个 Entry Pod 和多个 Worker Pod，多个 Role 又可以共同组成一个 ServingGroup。当其中某个 Pod 被删除、运行失败或容器异常重启时，仅恢复单个 Pod 可能无法重新建立原有的通信关系和运行状态。

Kthena 提供推理拓扑感知的故障恢复能力。用户可以通过 ModelServing 的 `recoveryPolicy` 配置故障恢复范围，在恢复速度、资源开销和运行一致性之间进行选择。

## 配置示例

下面示例包含 `prefill` 和 `decode` 两个 Role，并使用 Role 实例级故障恢复策略：

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: ModelServing
metadata:
  name: llm-serving
  namespace: default

spec:
  # ServingGroup 数量
  replicas: 1

  # 故障恢复策略：
  # None、RoleRecreate 或 ServingGroupRecreate
  recoveryPolicy: RoleRecreate

  template:
    # 检测到 Pod 失败或容器异常重启后等待 30 秒。
    # Pod 在此期间恢复 Ready，则不执行重建。
    restartGracePeriodSeconds: 30

    roles:
      - name: prefill
        replicas: 1

        # 一个 prefill 实例包含 1 个 Entry 和 1 个 Worker
        workerReplicas: 1

        entryTemplate:
          spec:
            containers:
              - name: prefill-entry
                image: <prefill-entry-image>

        workerTemplate:
          spec:
            containers:
              - name: prefill-worker
                image: <prefill-worker-image>

      - name: decode
        replicas: 1

        # 一个 decode 实例只有 Entry Pod
        workerReplicas: 0

        entryTemplate:
          spec:
            containers:
              - name: decode
                image: <decode-image>
```

对应的推理拓扑为：

```text
ServingGroup
├── prefill-0
│   ├── Entry Pod
│   └── Worker Pod
└── decode-0
    └── Entry Pod
```

Role 实例名称由 Role 名称和副本序号组成。因此，`prefill` 的第一个实例为 `prefill-0`，`decode` 的第一个实例为 `decode-0`。

## 恢复策略

Kthena 支持以下三种故障恢复策略：

| 策略 | 恢复范围 | 适用场景 |
| --- | --- | --- |
| `None` | 仅重新创建缺失或终止的单个 Pod；现有 Pod 内的容器重启由 Kubelet 处理 | Pod 之间相互独立，单个 Pod 能够独立初始化和恢复 |
| `RoleRecreate` | 重建故障 Pod 所属的一个 Role 实例，包括该实例的 Entry 和全部 Worker | Entry 与 Worker 需要共同启动、重新组网的分布式推理任务；默认推荐 |
| `ServingGroupRecreate` | 重建故障 Pod 所属 ServingGroup 中的全部 Role 和 Pod | ServingGroup 内多个 Role 强耦合，任意 Role 故障后都需要整组重新初始化 |

`RoleRecreate` 的恢复边界是一个具体 Role 实例。例如，`prefill-0` 的 Entry 或 Worker 发生故障时，Kthena 会重建 `prefill-0` 的 Entry 和 Worker，但不会影响 `decode-0`。

当同一 Role 配置了多个副本时，例如：

```yaml
- name: prefill
  replicas: 3
```

对应的 Role 实例为 `prefill-0`、`prefill-1` 和 `prefill-2`。如果 `prefill-1` 发生故障，只重建 `prefill-1`，不会重建其他两个实例。

`ServingGroupRecreate` 的恢复边界是一个具体 ServingGroup。当 ModelServing 包含多个 ServingGroup 时，只重建发生故障的 ServingGroup，其他 ServingGroup 继续运行。该策略应配合 `ServingGroupRollingUpdate` 使用，不能与 `RoleRollingUpdate` 组合。

## 故障恢复行为

Kthena 感知到推理 Pod 被删除、进入失败状态或容器发生异常重启后，会先确定该 Pod 所属的 ModelServing、ServingGroup 和 Role 实例，再根据故障类型及 `recoveryPolicy` 执行恢复。

| 故障类型 | Kthena 处理方式 |
| --- | --- |
| Pod 已经被删除 | 立即按照 `recoveryPolicy` 恢复，不再等待恢复宽限期 |
| Pod 进入 `Failed` | 等待 `restartGracePeriodSeconds`；Pod 未恢复时，按照 `recoveryPolicy` 重建 |
| 容器异常重启，策略为 `RoleRecreate` 或 `ServingGroupRecreate` | 等待 `restartGracePeriodSeconds`；Pod 未恢复 Ready 时，按照配置的范围重建 |
| 容器异常重启，策略为 `None` | 保留现有 Pod，由 Kubelet 负责重启容器，不扩大为 Role 或 ServingGroup 重建 |

`restartGracePeriodSeconds` 用于为短暂故障预留自恢复时间。等待期间，故障 Pod 所属的 Role 和 ServingGroup 会被标记为暂不可用：

- 如果 Pod 重新达到 Running 且 Ready，Kthena 保留原实例，不执行重建；
- 如果等待结束后 Pod 仍未恢复，Kthena 按照所配置的恢复策略执行重建；
- 默认值为 `0`，表示检测到异常后立即处理。

重建完成后，只有当期望的 Entry 和 Worker 全部达到 Running 且 Ready，Role 和 ServingGroup 才会重新进入运行状态，ModelServing 随后恢复为 `Available`。

对于推理进程卡死但容器未退出的场景，应在推理容器中配置能够反映真实业务状态的 `livenessProbe`。探针连续失败后，Kubelet 重启容器，Kthena 即可感知容器异常并执行相应的恢复策略。`readinessProbe` 主要用于判断实例能否接收流量，不能替代进程存活检测。

## 策略选择建议

对于能够独立初始化、没有分布式状态的单 Pod 推理服务，可以使用 `None`，以减少故障恢复对其他实例的影响。

对于 Entry 和 Worker 共同组成一个推理实例的多机、多卡任务，建议使用默认的 `RoleRecreate`。该策略能够重新建立 Role 内部运行状态，同时避免影响同一 ServingGroup 中的其他 Role。

当 ServingGroup 内的多个 Role 存在强运行依赖，任意 Role 故障后其他 Role 的状态也不再可信时，应使用 `ServingGroupRecreate`，通过整组重建保证所有 Role 重新建立一致的运行状态。
