# Autoscaler 升级指南

本文介绍如何将旧版的 `AutoscalingPolicy` 和
`AutoscalingPolicyBinding` 切换为当前版本的单一
`AutoscalingPolicy`。

:::warning

以下操作删除的是旧 CR 对象，不是 CRD。删除 CRD 会同时删除该类型下的
全部资源。

:::

## 操作步骤

### 1. 备份旧配置

```bash
kubectl get autoscalingpolicies.workload.serving.volcano.sh -A -o yaml \
  > autoscalingpolicies-backup.yaml

kubectl get autoscalingpolicybindings.workload.serving.volcano.sh -A -o yaml \
  > autoscalingpolicybindings-backup.yaml
```

### 2. 更新 CRD 和 Kthena

Helm 升级不会自动更新 Chart `crds/` 目录中的 CRD。升级 Kthena 前，先应用
目标版本随附的 CRD：

```bash
helm show crds oci://ghcr.io/volcano-sh/charts/kthena \
  --version vX.Y.Z \
  | kubectl apply --server-side -f -

helm upgrade kthena oci://ghcr.io/volcano-sh/charts/kthena \
  --version vX.Y.Z \
  --namespace kthena-system
```

如果 Kthena 由平台插件管理，请使用平台提供的升级方式，并确保 CRD 与
Controller 版本一致。

### 3. 创建新的 AutoscalingPolicy

根据需要选择 ServingGroup 级或 Role 级配置。

#### ServingGroup 级扩缩容

以下配置根据 Pod 指标修改 `llama-serving.spec.replicas`：

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: AutoscalingPolicy
metadata:
  name: llama-scaling-policy
  namespace: default
spec:
  metrics:
    - name: queue_depth
      targetValue: "10"
  tolerancePercent: 10
  behavior:
    scaleUp:
      stablePolicy:
        period: 30s
        stabilizationWindow: 1m
        instances: 2
        percent: 100
        selectPolicy: Or
      panicPolicy:
        period: 5s
        percent: 200
        panicThresholdPercent: 200
        panicModeHold: 1m
    scaleDown:
      period: 1m
      stabilizationWindow: 5m
      instances: 1
      percent: 100
      selectPolicy: Or
  homogeneousTarget:
    minReplicas: 1
    maxReplicas: 10
    target:
      targetRef:
        kind: ModelServing
        name: llama-serving
      metricSources:
        queue_depth:
          pod:
            name: vllm:num_requests_waiting
            uri: /metrics
            port: 8000
```

#### Role 级扩缩容

以下配置根据 Pod 指标修改 `pd-serving` 中 `decode` Role 的
`replicas`：

```yaml
apiVersion: workload.serving.volcano.sh/v1alpha1
kind: AutoscalingPolicy
metadata:
  name: decode-scaling-policy
  namespace: default
spec:
  tolerancePercent: 10
  behavior:
    scaleUp:
      stablePolicy:
        period: 30s
        stabilizationWindow: 1m
      panicPolicy:
        period: 5s
        panicThresholdPercent: 200
        panicModeHold: 1m
    scaleDown:
      period: 1m
      stabilizationWindow: 5m
  disaggregatedTarget:
    targetRef:
      kind: ModelServing
      name: pd-serving
    roles:
      decode:
        minReplicas: 1
        maxReplicas: 8
        metrics:
          - name: decode_queue
            targetValue: "10"
        metricSources:
          decode_queue:
            pod:
              name: vllm:num_requests_waiting
              uri: /metrics
              port: 8000
```

应用新配置：

```bash
kubectl apply -f autoscalingpolicy.yaml
```

### 4. 验证并清理旧 CR

确认新 Policy 已创建：

```bash
kubectl get autoscalingpolicy -A
```

观察目标副本数和 Controller 日志，确认指标采集及扩缩容正常后，删除对应的
旧 Binding；旧 Policy 不再被其他 Binding 使用时，也可以一并删除。

```bash
kubectl delete autoscalingpolicybinding <binding-name> -n <namespace>
kubectl delete autoscalingpolicy <old-policy-name> -n <namespace>
```

删除旧 Autoscaler CR 不会删除目标 `ModelServing` 或已有 Pod。

## 注意事项

- 一个旧 Policy 被多个 Binding 引用时，应为每个 Binding 创建一个新的
  Policy，再删除共享的旧 Policy。
- 新旧 Policy 名称相同时，可以直接使用新的 YAML 更新原 Policy；确认生效后
  再删除旧 Binding。
- 创建新 Policy 时如果提示目标字段不存在，说明集群中的 CRD 尚未更新。
- `metricSources` 的 key 必须与对应的指标名称一致。

新旧字段对应关系请参见
[自动扩缩容 API 迁移](./autoscaler-api-migration.md)。完整字段说明请参见
[Kthena Autoscaler](./autoscaler.md)。
