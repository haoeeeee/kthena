# 自动扩缩容 API 迁移

早期版本使用两个资源配置自动扩缩容：

- `AutoscalingPolicy`：定义指标和扩缩容策略；
- `AutoscalingPolicyBinding`：指定目标 `ModelServing`、指标来源和副本范围。

当前版本将两部分配置合并到一个 `AutoscalingPolicy` 中，不再需要
`AutoscalingPolicyBinding`。

```text
旧版：AutoscalingPolicy <- AutoscalingPolicyBinding -> ModelServing
新版：AutoscalingPolicy ----------------------------> ModelServing
```

## 主要变化

| 配置 | 旧版位置 | 当前版本位置 |
| --- | --- | --- |
| 指标和扩缩容策略 | `AutoscalingPolicy.spec` | `AutoscalingPolicy.spec` |
| 目标对象和副本范围 | `AutoscalingPolicyBinding.spec` | `AutoscalingPolicy.spec` |
| `policyRef` | Binding | 已移除 |
| Role 级目标 | `homogeneousTarget.target.subTargets` | `disaggregatedTarget.roles` |
| 指标来源类型 | 显式配置 `type` | 通过 `pod` 或 `prometheus` 字段确定 |

当前每个 `AutoscalingPolicy` 必须且只能配置一种目标模式：

- `homogeneousTarget`：扩缩单个 `ModelServing` 的 `spec.replicas`；
- `heterogeneousTarget`：在多个 `ModelServing` 之间分配副本；
- `disaggregatedTarget`：扩缩单个 `ModelServing` 中指定 Role 的
  `replicas`。

## 配置迁移方法

以每个旧 `AutoscalingPolicyBinding` 为单位创建一个新的
`AutoscalingPolicy`：

1. 从旧 Policy 复制 `metrics`、`tolerancePercent` 和 `behavior`；
2. 从旧 Binding 复制目标对象、指标来源和副本范围；
3. 将目标配置写入新 Policy 的 `spec`；
4. 删除 `policyRef`，并删除指标来源中的 `type`；
5. Role 级目标改用 `disaggregatedTarget.roles`。

:::note

如果一个旧 Policy 被多个 Binding 引用，需要为每个 Binding 分别创建一个
新的 Policy，因为每个新 Policy 都包含自己的目标配置。

:::

旧资源不会自动转换。完整的 ServingGroup 级和 Role 级 YAML，以及具体切换
步骤，请参见
[Autoscaler 升级指南](./autoscaler-cr-cutover.md)。当前版本的字段说明
请参见 [Kthena Autoscaler](./autoscaler.md)。
