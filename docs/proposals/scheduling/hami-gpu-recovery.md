# HAMi GPU 恢复的迁移接口

健康控制器使用集群级 `scheduling.koordinator.sh/v1alpha1` `PodMigrationJob` 指定一个源 Pod 和目标 GPU。源 Pod 和 ReplicaSet 模板须显式使用 `hami-scheduler`。演练期间固定副本数，暂停发布，并对同一 ReplicaSet 串行提交任务。

```yaml
apiVersion: scheduling.koordinator.sh/v1alpha1
kind: PodMigrationJob
metadata:
  name: recovery-plan-1-step-1
  annotations:
    hami.io/target-node: node-a
    hami.io/target-gpu-uuids: GPU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
spec:
  mode: EvictDirectly
  ttl: 30m
  podRef:
    namespace: gpu-recovery-demo
    name: small-model-7f5c845dd7-6z2br
    uid: 6986da65-cac8-427f-bdac-fcdf67b0c910
```

| 用途 | 操作 |
| --- | --- |
| 健康副本迁移 | 等待 Koordinator 仲裁和 HAMi 卡级预留。Koordinator 通过 Eviction API 驱逐源 Pod，遵守 PDB。 |
| 故障副本定向放置 | 添加 `hami.io/placement-only: "true"`。等待 `status.phase=Running` 且 `status.conditions` 中 `type=HamiPlacementReady`、`status=True`，然后由健康控制器调用 Eviction API，将源 Pod 的 UID 和最新 resourceVersion 设为前置条件。目标容量可以在此后释放。 |

`hami.io/target-gpu-uuids` 使用逗号分隔的唯一 NVIDIA GPU UUID。创建后保持请求字段固定。替代 Pod 在 `status.podRef` 中记录，包括 `namespace`、`name` 和 `uid`。仅在 `status.phase=Succeeded` 时，以该引用和 `status.nodeName` 作为已就绪且核对实际 GPU 分配的结果，再执行网关探测。`Failed` 或 `Aborted` 时停止后续驱逐，查看 `status.reason` 和 `status.message`。

健康路径要求源 Pod 带有 `hami.io/recovery-evictable: "true"`。源 UID 与 PMJ 一致且命名空间启用 `hami.io/managed-placement: "true"` 时，显式定向的 HAMi 健康迁移可丢弃 `emptyDir` 临时数据。含 HostPath、PVC、通用临时卷或 CSI 卷的 Pod 仍适用原有存储策略。命名空间、优先级、驱逐成本、副本约束、限流和 PDB 继续生效。故障路径的健康控制器须在 PMJ 未暂停、未删除且仍在 `metadata.creationTimestamp + spec.ttl` 截止时间内时发出驱逐请求。PDB 拒绝记录在 `Eviction` 条件的 `reason=HamiEvictionBlocked`。健康控制器可在阶段期限内等待预算恢复。阶段失败且源 Pod 仍活跃时，可携带最新 PMJ 的 `resourceVersion` 更新 `spec.paused=true`。

Koordinator 在调用 Eviction API 前，以预留的 UID 和资源版本为条件，写入 `hami.io/eviction-intent`。该注解记录源 Pod 的名称、UID、资源版本和本次提交标识。预留的取消或到期回收先更新对象时，Koordinator 的写入冲突，本次不会调用 Eviction API。写入先完成时，调度器继续保留原卡和保护 finalizer，直到驱逐结果明确。Koordinator 在写入驱逐意图前后直读 PMJ，核对 UID、资源版本、暂停、删除和截止时间。发送前以该 PMJ 版本写入本次尝试的发送检查点，并再次核对截止时间。检查点提交前重启时，控制器可以用同一个 PMJ 版本写入未发送结果，阻止旧提交者继续，再清除意图。检查点提交后，暂停无法撤回已获准发送的驱逐。

对于独立健康迁移，Eviction API 接受请求或明确拒绝后，Koordinator 先将结果写入 PMJ，再清除本次驱逐意图。HTTP 429 记录为 `HamiEvictionBlocked`，可在 PMJ 有效期内重试。请求结果不明时，`Eviction` 条件保持 `HamiEvictionSubmitting`，控制器重启后也不会重发。同一 UID 的源 Pod 仍存在且未进入终止状态时，预留会继续持有，覆盖 PMJ 超时、暂停及预留 DELETE。Pod 资源版本变化和 `DisruptionTarget` 均不足以确认删除结果。同一 UID 的源 Pod 进入删除流程或消失后，Koordinator 或调度器可解除持有，并按现有预留生命周期继续处理。驱逐意图未解决时，PMJ 的 `hami.io/source-eviction-recovery` finalizer 保留恢复所需的状态，删除请求可以进入等待。

独立健康迁移的发送检查点已写入、源 Pod 仍存续且长期无法确认结果时，预留和 PMJ 可能一直等待。运维可查看 API Server 审计记录（若已启用），并核对源 Pod UID 的生命周期。等待期间保留驱逐意图和保护 finalizer。

## 依赖目标预留的健康驱逐

故障副本 PMJ 保留 `hami.io/placement-only: "true"`，并用 `hami.io/target-reservation-occupants` 指定允许暂时占用目标卡的 Pod UID。健康迁移 PMJ 通过 `hami.io/required-target-reservation` 引用故障副本的实际目标预留。Koordinator 先在故障副本的目标预留写入 `hami.io/dependent-eviction-intent`，再在健康迁移自身的预留写入同一尝试的 `hami.io/eviction-intent`。

15 秒是发送检查点写入前的准备期限。期限结束后，接管者可以用 PMJ 的资源版本阻止旧提交者写入发送检查点，并清理未发送的尝试。发送检查点先提交时，控制器可能在发出请求之前退出，也可能已发出请求。只要源 Pod 的原 UID 仍存在且缺少结果证据，PMJ 就报告 `HamiEvictionOutcomeUnknown=True`，两份预留继续排除其他分配。

使用该条件中的尝试标识和源 Pod UID 核对同一次 Eviction 的结果。有 API Server 审计记录时，查看能关联到该次请求的记录。控制器记录同一次尝试的明确拒绝或确认未发送后，可以回收两份驱逐意图。已接受或结果仍未知的尝试，要等 API Server 确认原 Pod UID 消失。即使源 Pod 已进入 Terminating，或任一预留收到 DELETE，两份预留仍保持保护。单凭超时、Pod 资源版本变化或另一次请求的拒绝，无法确认本次结果。缺少上述证据时，PMJ 和预留会持续等待。

## 调度与失败处理

定向放置根据 ReplicaSet UID 在命名空间内创建唯一的不可变 ConfigMap。放置就绪且源 Pod 离开后，Pod CREATE webhook 给该 ReplicaSet 的替代 Pod 添加调度门、目标节点亲和性和 `nvidia.com/use-gpuuuid`。控制器通过 dry-run Pod CREATE 验证注入生效。若命名空间限制 Pod 数量或 GPU 配额，验证时须能额外准入一个同规格的 Pod。控制器记录实际替代 Pod 的 UID 后才移除调度门。此流程要求固定 ReplicaSet 成员，并支持一个 GPU 容器，其 `nvidia.com/gpu` 数量等于目标卡数。Webhook 接受 Kubernetes 内建 ReplicaSet 控制器身份 `system:serviceaccount:kube-system:replicaset-controller` 和 `system:kube-controller-manager`。

健康路径通过池授权和定向放置预检后，在替代 Pod 的命名空间创建 `resource.hami.io/v1alpha1` `GPUReservation`。调度器添加保护 finalizer 并报告 `Reserved` 后，Koordinator 在驱逐前再次执行预检。明确的准入拒绝报告为 `HamiAdmissionRejected`，并用 UID 和资源版本前置条件回收尚未登记消费者的预留。超时或其他无法确认的 API 错误报告为 `HamiAdmissionUnavailable`，已有预留在截止时间内保持有效，等待复查。Koordinator 在写入 `spec.consumerRef` 前重新读取同一 UID 的预留，确认它仍为 `Reserved`、`deletionTimestamp` 为空且未达到 `expiresAt`，然后以资源版本为前置条件写入。解除替代 Pod 的调度门前再次核对有效期。使用预留时，成功还要求调度器报告 `Consumed`。两条路径都检查绑定节点、`hami.io/vgpu-devices-allocated` 中的完整 UUID 集合和 Kubernetes `PodReady=True`。仅定向放置的任务中，替代 Pod 在指定节点和卡尚无容量时等待。

成功后控制器回收放置指令和预留。失败、取消或过期时，Koordinator 以预留的 UID 和资源版本为前置条件请求删除。调度器先等待在途源驱逐的结果明确，再用消费者 Pod 的资源版本确认 Bind 已提交，或完成未绑定分配的回滚。DELETE 成功表示已受理取消。确认预留回收完成时，等待原预留 UID 消失。如果需要判断 GPU 是否已被替代 Pod 使用，读取该 Pod 的绑定和分配状态。普通分配在调度器完成裁决前仍受预留约束。已就绪的放置指令保留到编排器处置，继续约束替代 Pod 的落点。编排器应保留 PMJ 直到记录结果。需要取消或重试时，先停止编排并核对源 Pod 与带对应 `hami.io/placement-job-uid` 的替代 Pod，再删除旧 PMJ。级联回收处理该任务的放置 ConfigMap 和独立健康迁移的预留。Koordinator 在依赖驱逐结算后回收故障副本的目标预留。已经写入 Pod 的调度门与落点约束仍属于该 Pod，替代 Pod 的处理须与新计划一起决定。

## 与资源池准入一起使用

Demo 的模型 Pod 模板显式设置 `hami.io/resource-pool: gpu-recovery-demo`，命名空间通过 `ResourcePoolBinding` 获得该池的访问权限。PMJ 的所有目标 UUID 都须属于这个池中已发现的健康 NVIDIA GPU。目标卡已被使用时，RPM 仍可授权；故障路径的替代 Pod 将等待调度器看到目标容量。

准入支持两种 webhook 调用顺序。

- Koordinator 先运行时，写入 `nvidia.com/use-gpuuuid` 的精确目标。RPM 将其作为已有 UUID 选择，按绑定和健康库存授权并保留该子集。
- RPM 先运行时，Koordinator 将 RPM 已填入的 UUID 白名单收窄到 PMJ 目标。目标必须全部在原白名单内，并且未被 `nvidia.com/nouse-gpuuuid` 排除。资源池选择、授权 revision 和配额注解由 RPM 保持。RPM 的 NVIDIA 授权 revision 基于筛选 UUID 子集前的池库存，最终 validator 会用收窄后的 UUID 集合重新验证授权。

保留 RPM 的 `reinvocationPolicy: Never` 和 Koordinator 的 `reinvocationPolicy: IfNeeded`。Koordinator 重入时会保留已有调度门和目标亲和性。驱逐前的 dry-run Pod CREATE 从 ReplicaSet 模板发起，经 API Server 的最终校验后，Koordinator 还会检查返回的精确 UUID、节点约束和调度门。模板显式选池时，预检也要求返回相同的池名及 RPM 的授权和选池 revision。预检失败时，查看 PMJ 的 `HamiAdmission` 条件。 [Kubernetes 准入 webhook 调用顺序](https://kubernetes.io/docs/concepts/cluster-administration/admission-webhooks-good-practices/#mutating-webhook-ordering-and-idempotence)
