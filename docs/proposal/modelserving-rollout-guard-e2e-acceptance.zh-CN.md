# ModelServing 滚动保护端到端用例与验收标准

验证 `modelserving.volcano.sh/rollout-version` 能拦截未经放行的模板，同时允许扩缩容、补建和调整滚动配置。验收以实际 Pod、ControllerRevision 和 ModelServing Status 的变化为准。

## 测试准备

- 在 kind 中部署待验证的 controller，记录镜像和对应提交。在独立 namespace 执行用例。
- 下面的基本用例分别用 `ServingGroupRollingUpdate` 和 `RoleRollingUpdate` 执行。基线使用 2 个 ServingGroup、每组同名 Role 的 2 个实例、每实例 1 个 Entry Pod 和 1 个 Worker Pod。准备可区分的 v1、v2、v3 镜像。默认关闭自动恢复级联（`recoveryPolicy: None`），将当前模式的 `maxUnavailable`、`maxSurge`、`partition` 分别设为 `1`、`0`、`0`。
- `v1/A` 表示模板 v1 和 annotation 值 A。`R1` 表示 v1 对应的 ControllerRevision，`R2` 表示 v2 对应的 ControllerRevision。版本标记位于 **ControllerRevision 自身的 `metadata.annotations`**。
- 除表中明确要求连续操作外，每例从独立基线开始。涉及正在执行的升级时，用 readiness probe 控制新 Pod 的就绪时机。

## 基本用例

| 编号 | 操作 | 验收标准 |
|---|---|---|
| E01 | 创建 `v1/A`。 | Pod 正常创建并就绪。`updateRevision` 指向 R1，R1 的 annotation 记录 A。 |
| E02 | 从 `v1/A` 只改模板，提交 `v2/A`。 | API 中的 MS 仍声明 v2。实际 Pod 保持 v1，Pod UID 不变，`updateRevision` 保持 R1，未为被拦截的模板创建 revision。 |
| E03 | 在没有待放行模板时，只把 `v1/A` 改成 `v1/B`。等 R1 的标记为 B，再提交 `v2/B`。再用已有 v2 历史 revision 的基线重复一次，可先按授权版本滚到 v2，再滚回 `v1/A` 并保留 v2 快照。 | 第一步只更新 R1 的版本标记，Pod UID 和 revision 不变。第二步拦截 v2，未创建新 revision，未切换到已存在的 v2 revision，未删除 v1 Pod。 |
| E04 | `v2/A` 已被拦截，只把 annotation 改成 B。 | 仅靠 annotation 更新即可启动滚动。v2 完成滚动后，`currentRevision` 和 `updateRevision` 均指向 R2，R2 记录 B。 |
| E05 | 从 `v1/A` 一次提交 `v2/B`。 | 正常滚到 v2，目标 ControllerRevision 记录 B。 |
| E06 | 分别验证三种操作：在无 annotation 的 v1 上一并提交 v2 和 A；从被拦截的 `v2/A` 移除 annotation；在无 annotation 时继续改模板。 | 第一次启用时允许 v2。移除 annotation 本身触发 reconcile，放行原本被拦截的 v2。随后在保护关闭时，模板更新按原滚动逻辑生效。 |
| E07 | 创建 `v1/""`，提交 `v2/""`，再删除 annotation key。 | R1 上存在该 key，值为空字符串。版本同为空时拦截 v2；删除 key 后，恢复原有滚动行为并收敛到 v2。 |
| E08 | `v2/A` 被拦截时，分别对 SG 和同名 Role 执行扩容、缩容。另测一次在同一请求里改模板和副本数。 | 到达最新期望副本数。扩容用 v1，缩容移除多余实例，保留的 Pod UID 不变。`updateRevision` 仍是 R1。 |
| E09 | 在 `v1/A` 已稳定时只改当前模式的 `maxUnavailable`、`maxSurge`、`partition`。 | 新配置被 API 接受，`observedGeneration` 跟上此次 Spec 修改。没有新模板滚动，revision 和 Pod UID 均保持不变。 |
| E10 | 在 3 个 SG（SG 模式）或 1 个 SG 内的 3 个同名 Role 实例（Role 模式）上，先放行 v2/B。用 readiness probe 保持首个 v2 实例未就绪。保持 B，同时提交 v3 并把 `maxUnavailable` 从 1 改为 2，继续使用 `maxSurge: 0` 和 `partition: 0`。 | 改配置前，只有 1 个实例更新到 v2 并处于未就绪状态。改配置后，在第一个实例仍未就绪时，第二个实例也能更新到 v2。整个过程中 `updateRevision` 保持 R2，没有 v3 Pod。解除 readiness 限制后，所有实例最终收敛到 v2。 |
| E11 | `v2/A` 被拦截时，分别删除一个 Entry Pod 和一个 Worker Pod，等每次补建完成再删除下一个。 | 补建 Pod 的 UID 与被删除的不同，使用的是 v1 模板。`updateRevision` 保持 R1。 |
| E12 | 保持 API 中为被拦截的 `v2/A`，重启 controller 后触发 reconcile。另测在同一状态下将 SG 缩到 0，重启后再扩容。 | 有存量 Pod 时仍拦截 v2。没有存量 Pod 时仍可根据 R1 补建 v1。两种情况下 `updateRevision` 都保持 R1。 |
| E13 | 保持 A，单独修改 Entry 镜像、Worker 镜像、环境变量、资源请求或 `workerReplicas`，每项从独立基线开始。 | 每项都应触发模板拦截，实际 Pod 配置和 Worker 数量保留 v1 的值，`updateRevision` 保持 R1。`workerReplicas` 是每个 Role 实例的 Worker 数量，E08 调整的是 Role 实例数。 |

## Status 与 Event 验收

| 编号 | 操作 | 验收标准 |
|---|---|---|
| S01 | v1 Pod 均已 Ready，执行 E02。 | `status.conditions` 包含 `RolloutBlocked=True`，Reason 为 `RolloutVersionUnchanged`。MS 仍可同时为 `Available=True`。`updatedReplicas` 按实际维护的 v1 统计，`updateRevision=R1`，`observedGeneration` 跟上 MS 的新代次。产生关联到该 MS UID 的 `Warning` Event，Reason 为 `RolloutBlocked`。 |
| S02 | 从已拦截的状态分别用三种方式解除：改版本放行、改回 R1 对应模板、删除 annotation。 | 原有 `RolloutBlocked` 均转为 `False`。改回旧模板的分支还需确认 Pod UID 不变。从未拦截的对象没有这条 Condition。 |
| S03 | 拦截期间通过 `/status` 加入一条 controller 不负责维护的 Condition，再触发一次 SG 扩容。 | 副本计数和滚动保护 Condition 正确更新，额外写入的 Condition 仍在，API Spec 仍保留用户提交的模板。`409 Conflict` 后重新读取并成功提交的时序由[冲突重试单测](../../pkg/model-serving-controller/controller/rollout_guard_test.go)确定性覆盖。 |

## 异常验收

| 编号 | 操作 | 验收标准 |
|---|---|---|
| N01 | 在独立 kind 测试对象上删除已接受的 R1，再提交同版本的新模板。 | controller 报告已接受的历史记录缺失。新模板不得被选为目标，也不得用它创建或替换 Pod。 |

## 通过标准与取证

- 变更前后记录 MS 的 Spec、Status、Pod 名称和 UID，以及 ControllerRevision 的名称、UID 和版本标记。对「未滚动」「未创建 revision」的验收，同时观察用例执行期间的 Pod 与 revision 事件，避免漏掉中间的创建或删除。
- Spec 更新等待对应的 `observedGeneration`，并检查该用例要求的 Pod 和 Status。仅改 annotation 时用 ControllerRevision 标记、拦截 Condition 或 Pod 滚动进度确认 reconcile 已执行。存在 readiness 限制时，先记录中间结果，再开放就绪并验证最终收敛。
- 两种模式的适用用例均符合表中预期才算通过。记录用例编号、模式、controller 镜像、实际结果和相关快照。历史 Event 可以继续存在，当前是否被拦截以 `RolloutBlocked` Condition 为准。
