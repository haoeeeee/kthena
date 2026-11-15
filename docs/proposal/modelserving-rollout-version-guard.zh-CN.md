# ModelServing 滚动升级保护设计方案（评审稿）

## 1. 背景与目标

ModelServing 的工作负载模板变化后，控制器会按既定策略推进升级。业务配置误改或自动化操作可能因此引发非预期的 Pod 更新。

本方案增加由业务控制的滚动保护：模板发生变化时，通过 rollout-version 判断是否允许采用新模板；未放行时继续使用原模板，SG 和同名 Role 的普通副本数量调整仍可执行。

## 2. 总体方案

业务在 ModelServing 顶层设置 annotation 开启保护，版本值由业务自行决定：

```yaml
metadata:
  annotations:
    modelserving.volcano.sh/rollout-version: "approved-a"
```

key 存在即开启，值为 "" 也有效；key 不存在则关闭保护，恢复原有更新行为。

方案需要区分“业务现在提交的模板”和“控制器此前决定使用的模板”。例如，系统原先使用 v1，业务将 MS 中的镜像改为 v2，但没有改变版本值。本方案应让 API 中继续保留 v2 声明，而控制器仍按 v1 维护工作负载。因此本文定义：

- **当前模板**：当前 ModelServing 中声明的工作负载模板。上述例子中是 v2。
- **已接受模板**：控制器此前已确定用于创建或升级工作负载、并已保存到 ControllerRevision 的模板。上述例子中是 v1，通过 MS.status.updateRevision 找到它。

已接受模板不一定等于所有运行 Pod 的模板。v2 获准升级并保存目标后，已接受模板就变为 v2，即使仍有部分 v1 Pod 尚未完成更新。

保护开启且已存在版本记录时，按以下三条规则选择本轮模板：

| 当前模板与已接受模板比较 | 当前版本值与已记录值比较 | 本轮处理 |
| --- | --- | --- |
| 相同 | 任意 | 使用当前 MS，保持原 revision；版本值不同时更新记录，不因此重建 Pod |
| 不同 | 相同 | 使用已接受模板，固定原 revision，拦截新模板 |
| 不同 | 不同 | 使用当前新模板，保存新目标及版本值，按原有策略升级 |

**所有分支均使用当前 MS 的 SG/同名 Role 期望副本数。** 这里的数量指 ModelServing.spec.replicas 和同名 Role.replicas；Role 内部 workerReplicas 属于模板变化。

rolloutStrategy、maxUnavailable、maxSurge、partition 等原有升级策略也使用当前配置，继续决定升级粒度、方式、不可用预算和分区范围；这些配置不用于放行不同模板。

## 3. 详细设计

### 3.1 控制流程

流程图中的编号与以下步骤一致：

1. **读取 MS，检查保护开关。** 没有 rollout-version key 时，直接进入原有流程；存在时继续以下步骤。
2. **读取已接受模板。** 根据 MS.status.updateRevision 找到 ControllerRevision，读取其中的模板及版本值。首次创建没有历史模板，按第 3.4 节确定初始模板后转步骤 5；已有目标的快照读取失败则返回错误。
3. **比较模板和版本值。** 先比较当前模板与快照模板。模板不同时，再比较 MS 上的版本值与快照记录值；快照没有版本记录时，先保持原模板并建立记录。
4. **确定本轮维护配置。** 模板相同或新模板获准时采用当前 MS；拦截时复制当前 MS，仅将模板字段恢复为快照模板，保留最新副本数，并固定原 revision。
5. **保存版本值和目标引用。** 将本轮结果记录到 ControllerRevision annotation 和 MS.status.updateRevision，具体写入方式见第 3.3 节。记录失败则不进入新目标的后续正常扩容与升级。
6. **执行原有资源维护。** 使用上述配置和 revision 处理 SG/Role 数量、成员补建、滚动或原地更新，并按同一目标统计状态。拦截时发出 RolloutBlocked Event。

本轮恢复模板只影响控制器使用的配置，不回写 API Spec。因此拦截不会取消副本调整，也不会把业务提交的新模板改回旧值。

**图 1：单轮控制流程** · [查看大图](images/rollout-guard/changes.png)

![图 1：读取、比较、选择、保存和执行的六个步骤](images/rollout-guard/changes.png)

### 3.2 模板与版本值的比较

每轮从当前 MS 取得当前模板和版本值，再从 status.updateRevision 对应的 ControllerRevision 取得已接受模板和已记录版本值。**ControllerRevision.Data 提供模板，ControllerRevision 自身的 metadata.annotations 提供版本记录。** 不需要保留上一轮 MS，也不依赖 Update 事件中的旧对象。

比较的是实际模板内容，复用现有模板比较机制。镜像、命令、环境变量、资源及其他 Entry/Worker 模板输入，以及快照定义的 scheduler/plugin 渲染输入参与比较；SG/Role 普通副本数、顶层 rollout-version 和升级策略不参与模板比较。revision 标识用于定位快照，不能仅凭名称或 hash 不同认定模板变化。

例如，当前 MS 声明 app:v2、approved-a、SG=3，快照保存 app:v1、approved-a。两侧模板不同但版本值相同，后续使用 **app:v1、SG=3、原 revision**：新模板被拦截，数量调整继续执行。

**图 2：比较数据与拦截结果示例** · [查看大图](images/rollout-guard/comparison.png)

![图 2：当前 MS 与 ControllerRevision 的比较及维护结果](images/rollout-guard/comparison.png)

如果 API 中仍保留被拦截的 app:v2，本次只修改副本数，处理方式不变：仍从快照恢复 app:v1，保留当前副本数。同名 Role.replicas 遵循相同规则。

### 3.3 版本记录的保存与使用

下一轮需要知道“上一次使用这个模板时，版本值是什么”。该值不能只放在当前 MS 上，因为业务会修改它。因此，**本次新增将版本值写入 ControllerRevision 自身 annotation 的行为**，不把 ModelServing 顶层 annotation 嵌入模板 Data。

以已接受 v1/approved-a 为例，保存的数据为：

| Kubernetes 对象 | 保存内容 |
| --- | --- |
| ModelServing.status.updateRevision | 指向 v1 对应的 revision R1 |
| R1 对应的 ControllerRevision.Data | v1 模板 |
| 同一 ControllerRevision.metadata.annotations | rollout-version=approved-a |

后续根据请求分别处理：

- **拦截 v2/approved-a：**上述记录保持不变，下轮仍用 v1/approved-a 比较。
- **放行 v2/approved-b：**创建或复用 v2 快照，先在其自身 annotation 记录 approved-b，再将 MS.status.updateRevision 指向该目标，之后才执行后续正常扩容和升级。下轮从新目标读到 v2/approved-b。
- **仅改版本值：**模板仍为 v1，只把版本值改为 approved-b，则更新 R1 自身的 annotation，目标引用不变，不强制重建 Pod。后续再以 approved-b 提交不同模板会被拦截。

这些写入不修改 MS.Spec，不新增 Spec/Status 字段，不改变 ControllerRevision.Data 或模板 hash 输入。状态统计沿用选定目标；本轮提前写入 status 后，后续写入使用返回的资源版本。

### 3.4 开关变化与初始记录

| 场景 | 行为 |
| --- | --- |
| 删除 MS 上的 key，同时修改模板 | 关闭保护，按原逻辑采用新模板 |
| 新增 key，但已接受快照没有版本记录 | 保持原模板，在其快照记录当前版本值；本次不同模板先拦截，副本调整照常处理 |
| 新增 key，已接受快照已有版本记录 | 当前值与记录值相同则拦截不同模板，不同则放行 |
| 真正首次创建，无历史模板 | 允许建立初始目标，并保存版本值及目标引用 |

关闭保护不清除快照上的记录。重新开启时，使用当前目标快照的记录。空字符串是有效版本值，与没有 key 区分处理。

例如首次开启时，已有 v1 快照但没有版本记录，本次提交 v2/a：保持 v1 并记录 a；之后将版本值改为 b，才会放行 v2。

### 3.5 对原有链路的改动

| 位置 | 改动 |
| --- | --- |
| 变更感知 | 除 Spec 外，感知版本值和 key 存在性变化 |
| 资源维护之前 | 增加图 1 中的保护判定、维护配置选择及版本记录写入 |
| 扩缩容、补建和升级 | 复用原有算法，使用选定模板、匹配 revision 及当前数量 |
| 状态统计 | 使用与执行阶段一致的配置及 revision |

原有 partition、surge、依赖限制、故障恢复和清理机制继续生效。已经发出的操作不因后续模板被拦截而撤销。

### 3.6 连续变更示例

初始使用 R1：模板 v1、版本值 approved-a、SG=2。以下三次变更分别被控制器处理：

**图 3：拦截、放行及后续再次拦截** · [查看大图](images/rollout-guard/example.png)

![图 3：三次变更中的模板、数量和版本记录](images/rollout-guard/example.png)

第二次放行 v2 后，第三次应与 v2 比较。即使还有 v1 Pod，原有 v1 → v2 升级也可继续，不会因 v3 被拦截而停止。

## 4. 重启恢复与异常处理

### 4.1 控制器重启

**普通控制器重启不会清除保护记录，也不会因此放行被拦截模板。** 模板保存在 ControllerRevision.Data，版本值保存在该对象自身 annotation，目标引用保存在 MS.status.updateRevision；三者都是 Kubernetes 中的持久化数据，不依赖控制器内存。

启动后，控制器等待资源缓存同步，重新加载 MS 和运行资源；处理 MS 时再读取对应的 ControllerRevision。假设重启前拦截了 v2/approved-a，持久化记录仍是 R1/v1/approved-a，重启后就会得到相同的拦截结果。

重启发生在新模板记录写入过程中时：

| 重启前完成的写入 | 重启后的处理 |
| --- | --- |
| 只准备了新快照，尚未更新 MS.status.updateRevision | 仍以原目标比较，重新判断本次请求；候选快照存在不代表已经放行 |
| 已更新 MS.status.updateRevision，尚未完成 Pod 更新 | 从新目标恢复模板和版本值，继续已经放行的升级 |

缩到 0 再扩容也使用上述持久化记录，不需要通过现存 Pod 反推模板。

### 4.2 异常及已知边界

| 情况 | 处理原则 |
| --- | --- |
| 目标快照缺失、损坏、归属不符或比较失败 | 返回错误，不以当前新模板兜底；尝试既有安全缩容 |
| 版本记录或目标引用保存失败 | 不进入新目标的后续正常扩容、升级；重试时重新读取已保存的记录 |
| 并发修改导致状态冲突 | 后续完整 reconcile 重新判断，本轮状态更新可能延后 |
| 旧格式快照未记录 scheduler/plugin 输入 | 无法恢复当时未记录的内容，保护范围受历史信息限制 |

安全缩容仍受原有策略限制，异常时不保证所有数量调整都能执行。快照与 MS 状态的写入不是跨资源事务。

**持久化状态被清空与控制器重启不同。** 当前实现以 updateRevision、currentRevision 均为空且 observedGeneration 为 0 识别初始化；存量资源若这些状态全部丢失，可能被误判为首次创建，进而接受不同模板。该场景尚未覆盖，属于当前方案的已知缺口。其他有处理痕迹但目标引用缺失的情况按异常处理。

## 5. 验证与验收

| 验证项 | 验收要求 |
| --- | --- |
| 三条主判定规则 | 分别叠加 SG/Role 数量变化，模板与数量目标符合第 2 节约定 |
| 拦截后只改数量 | API 中保留未放行模板时，继续使用快照模板处理数量变化 |
| 开关与版本值 | 覆盖删除、新增、重新开启、空字符串、仅版本值变化后再修改模板 |
| 升级途中再次变更 | 与最新已接受模板比较，拒绝未放行模板，原有升级可继续 |
| 控制器重启 | 拦截状态保持；已放行目标继续执行；写入中断按第 4.1 节恢复 |
| 错误处理 | 快照异常、记录写入失败和状态冲突不放行未经允许的模板 |

验证覆盖 SG/Role 两种滚动粒度、重建和原地更新。现有 controller 相关回归已通过，保护专项 UT 尚未新增，不能替代上述专项验收。
