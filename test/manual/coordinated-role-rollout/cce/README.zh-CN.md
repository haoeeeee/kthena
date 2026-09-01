# CCE 上验证等比例 Role 滚动升级

本用例验证一个 `ModelServing` 内 A、B、C 三个 Role 的协调滚动升级：

- A、B、C 各 10 个副本；
- A 依赖 B，B 依赖 C；数据调用方向为 A → B → C；
- `maxSkew` 为 20%，即三个 Role 的归一化滚动进度最多相差 20 个百分点；
- `roles` 为空，默认选择全部 Role；
- 每个 Role 省略 `maxUnavailable`，由 CRD 默认成 1；
- 每个 Role 省略 `partition`，按 0 处理。

本目录的 YAML 使用 `busybox:1.36`。如果 CCE 无法拉取该镜像，请先替换成 CCE 可以拉取、且包含 `sh`、`sleep`、`touch` 和 `test` 命令的镜像。

## Ready 控制方式

v1 容器启动时自动创建 `/tmp/ready`，因此 Pod 自动 Ready。v2 容器不会创建该文件，需要执行以下命令手工放行：

```bash
kubectl exec -n coordinated-role-rollout-test POD_NAME -- touch /tmp/ready
```

readinessProbe 每秒检查一次 `/tmp/ready` 是否存在。

## 1. 环境检查

进入仓库根目录并设置变量：

```bash
export TEST_NS=coordinated-role-rollout-test
export TEST_MS=proportional-role-rollout
export TEST_DIR=test/manual/coordinated-role-rollout/cce
```

确认当前操作的是目标 CCE 集群：

```bash
kubectl config current-context
kubectl cluster-info
```

确认安装的 CRD 包含协调滚动字段：

```bash
kubectl explain modelserving.spec.rolloutStrategy.roleCoordination
```

该命令应显示 `maxSkew`、`roles` 和 `dependencies`。

## 2. 准备观察和控制函数

将以下函数复制到当前 shell：

```bash
pod_rows() {
  kubectl get pods -n "${TEST_NS}" \
    -l "modelserving.volcano.sh/name=${TEST_MS}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.modelserving\.volcano\.sh/role}{"\t"}{.spec.containers[0].env[?(@.name=="VERSION")].value}{"\t"}{.status.containerStatuses[0].ready}{"\n"}{end}'
}

count_pods() {
  pod_rows | awk -v role="$1" -v version="$2" \
    '$2 == role && $3 == version {n++} END {print n + 0}'
}

count_ready() {
  pod_rows | awk -v role="$1" -v version="$2" \
    '$2 == role && $3 == version && $4 == "true" {n++} END {print n + 0}'
}

show_state() {
  printf '%-52s %-6s %-8s %-6s\n' POD ROLE VERSION READY
  pod_rows | sort

  echo
  for role in a b c; do
    printf '%s: v1=%s, v2=%s, v2Ready=%s\n' \
      "${role}" \
      "$(count_pods "${role}" v1)" \
      "$(count_pods "${role}" v2)" \
      "$(count_ready "${role}" v2)"
  done
}

wait_count() {
  local role="$1"
  local version="$2"
  local expected="$3"
  local timeout="${4:-300}"

  for _ in $(seq 1 "${timeout}"); do
    if [ "$(count_pods "${role}" "${version}")" -eq "${expected}" ]; then
      return 0
    fi
    sleep 1
  done

  echo "Timeout waiting for ${role}/${version}=${expected}"
  show_state
  return 1
}

wait_ready() {
  local role="$1"
  local version="$2"
  local expected="$3"
  local timeout="${4:-300}"

  for _ in $(seq 1 "${timeout}"); do
    if [ "$(count_ready "${role}" "${version}")" -eq "${expected}" ]; then
      return 0
    fi
    sleep 1
  done

  echo "Timeout waiting for Ready ${role}/${version}=${expected}"
  show_state
  return 1
}

touch_role() {
  local role="$1"

  pod_rows |
    awk -v role="${role}" \
      '$2 == role && $3 == "v2" && $4 != "true" {print $1}' |
    while read -r pod; do
      [ -n "${pod}" ] || continue
      kubectl exec -n "${TEST_NS}" "${pod}" -- touch /tmp/ready || true
    done
}

touch_all_v2() {
  touch_role a
  touch_role b
  touch_role c
}
```

## 3. 创建 v1 基线

如果专用 namespace 中存在上一次测试资源，先清理同名 `ModelServing`：

```bash
kubectl delete modelserving "${TEST_MS}" -n "${TEST_NS}" \
  --ignore-not-found --wait=true --timeout=300s
```

创建 v1 资源：

```bash
kubectl apply -f "${TEST_DIR}/modelserving-v1.yaml"
```

等待三个 Role 的 30 个 v1 Pod 全部 Ready：

```bash
wait_ready a v1 10 600
wait_ready b v1 10 600
wait_ready c v1 10 600
show_state
```

预期：

| Role | v1 | v2 | v1 Ready |
|---|---:|---:|---:|
| A | 10 | 0 | 10 |
| B | 10 | 0 | 10 |
| C | 10 | 0 | 10 |

确认默认 `maxUnavailable`：

```bash
kubectl get modelserving "${TEST_MS}" -n "${TEST_NS}" \
  -o jsonpath='{range .spec.template.roles[*]}{.name}{"="}{.maxUnavailable}{" "}{end}{"\n"}'
```

预期输出：

```text
a=1 b=1 c=1
```

## 4. 同时把 A、B、C 更新到 v2

```bash
kubectl apply -f "${TEST_DIR}/modelserving-v2.yaml"
```

等待第一批目标 Pod 出现：

```bash
wait_count b v2 1 300
wait_count c v2 1 300
sleep 10
show_state
```

预期：

| Role | v1 | v2 | v2 Ready |
|---|---:|---:|---:|
| A | 10 | 0 | 0 |
| B | 9 | 1 | 0 |
| C | 9 | 1 | 0 |

B、C 已开始滚动，但 A 必须保持 0 个 v2。再观察 30 秒，B、C 未 Ready 时 A 仍不能启动：

```bash
for _ in $(seq 1 6); do
  printf 'A-v2=%s B-v2=%s C-v2=%s\n' \
    "$(count_pods a v2)" "$(count_pods b v2)" "$(count_pods c v2)"
  sleep 5
done
```

## 5. 放行第一批 B、C

```bash
touch_role b
touch_role c

wait_ready b v2 1
wait_ready c v2 1
wait_count a v2 1 300
wait_count b v2 2 300
wait_count c v2 2 300
show_state
```

预期：

| Role | v1 | v2 | v2 Ready |
|---|---:|---:|---:|
| A | 9 | 1 | 0 |
| B | 8 | 2 | 1 |
| C | 8 | 2 | 1 |

这说明 B、C 出现目标版本 Ready 容量后，A 才开始滚动。

## 6. 验证 maxSkew=20%

只放行 B、C 的第二个 v2，不放行 A：

```bash
touch_role b
touch_role c
wait_ready b v2 2
wait_ready c v2 2
```

连续观察一分钟：

```bash
for _ in $(seq 1 12); do
  printf 'A-v2=%s B-v2=%s C-v2=%s | Ready=%s/%s/%s\n' \
    "$(count_pods a v2)" "$(count_pods b v2)" "$(count_pods c v2)" \
    "$(count_ready a v2)" "$(count_ready b v2)" "$(count_ready c v2)"
  sleep 5
done
```

预期稳定在：

```text
A-v2=1 B-v2=2 C-v2=2 | Ready=0/2/2
```

A 的目标版本 Ready 进度为 0%，B、C 为 20%。即使 B、C 已 Ready，也不能继续创建第 3 个 v2，否则会超过 20% 的进度偏差。

## 7. 释放下一推进窗口

放行 A 的第一个 v2：

```bash
touch_role a
wait_ready a v2 1
wait_count a v2 2 300
wait_count b v2 3 300
wait_count c v2 3 300
show_state
```

预期：

| Role | v1 | v2 | v2 Ready |
|---|---:|---:|---:|
| A | 8 | 2 | 1 |
| B | 7 | 3 | 2 |
| C | 7 | 3 | 2 |

最慢 Role 的 Ready 进度增长 10% 后，三个 Role 的下一推进窗口被释放，目标版本副本数差距仍不超过 2。

## 8. 驱动滚动完成

循环放行新创建的 v2 Pod，直到三个 Role 都收敛：

```bash
deadline=$((SECONDS + 1200))

while [ "${SECONDS}" -lt "${deadline}" ]; do
  touch_all_v2

  a_old="$(count_pods a v1)"
  b_old="$(count_pods b v1)"
  c_old="$(count_pods c v1)"
  a_new="$(count_pods a v2)"
  b_new="$(count_pods b v2)"
  c_new="$(count_pods c v2)"
  a_ready="$(count_ready a v2)"
  b_ready="$(count_ready b v2)"
  c_ready="$(count_ready c v2)"

  printf 'old=%s/%s/%s v2=%s/%s/%s ready=%s/%s/%s\n' \
    "${a_old}" "${b_old}" "${c_old}" \
    "${a_new}" "${b_new}" "${c_new}" \
    "${a_ready}" "${b_ready}" "${c_ready}"

  if [ "${a_old}" -eq 0 ] && [ "${b_old}" -eq 0 ] && [ "${c_old}" -eq 0 ] && \
     [ "${a_ready}" -eq 10 ] && [ "${b_ready}" -eq 10 ] && [ "${c_ready}" -eq 10 ]; then
    break
  fi

  sleep 3
done
```

由于采用 delete-first 且默认 `maxUnavailable=1`，删除旧 Pod 与创建新 Pod 之间，短暂看到某个 Role 只有 9 个 Pod 属于正常现象。

## 9. 最终验收

```bash
show_state

test "$(count_pods a v1)" -eq 0
test "$(count_pods b v1)" -eq 0
test "$(count_pods c v1)" -eq 0

test "$(count_pods a v2)" -eq 10
test "$(count_pods b v2)" -eq 10
test "$(count_pods c v2)" -eq 10

test "$(count_ready a v2)" -eq 10
test "$(count_ready b v2)" -eq 10
test "$(count_ready c v2)" -eq 10
```

以上命令全部返回 0，表示该用例验证通过。

## 10. 清理

确认该 namespace 仅用于本次测试后执行：

```bash
kubectl delete namespace "${TEST_NS}" --wait=true --timeout=300s
```
