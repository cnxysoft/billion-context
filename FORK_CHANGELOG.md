# Fork Changelog (`cnxysoft/billion-context`)

本文件记录 `cnxysoft/billion-context`（默认分支 `fork`）相对上游 [`ranxianglei/billion-context`](https://github.com/ranxianglei/billion-context) 新增与修改的全部内容。`master` 分支保持与上游纯净同步；上游不包含本文件，以保持后续同步上游时的零合并冲突。

- **当前上游基线**：`v0.1.217` + PR `#7891`（`24ba3ad09`，于 `2026-11-29` 合并 `854a4569b`；内置 `acp-kernel` 版本 `0.0.215`）
- **分支约定**：`master` 纯净跟踪上游 `origin/master`；`fork` 为唯一长期维护的 Fork 默认分支
- **Fork 构建入口**：GitHub Actions → `Android long-session baseline`（推送至 `fork` 分支自动触发，亦支持 `workflow_dispatch` 手动触发，自动产出 `billion-context-android-baseline` npm tarball 并运行内核单测、回归套件与 Codex fake-upstream E2E）

---

## 核心特性总览

### 1. Responses 全量重发极速快路径与 `O(tail)` 折叠对账
面向移动端（Android / DSH）与超长会话在每轮全量重发历史时的首字延迟（TTFT）瓶颈：
- **字节级 SHA-256 前缀缓存（`src/server/responses-prefix-cache.ts`）**：在 Responses 全量重发轮次中对历史前缀做字节级校验命中，直接复用前序 canonical 投影结果，避免重复解析与重算。
- **`O(tail)` 折叠对账见证与 steady-state 跳过**：在 `fold-reconcile` 中引入尾窗见证与稳态跳过，避免每轮对未变动折叠前缀做全量扫描；同时按需延迟 continuity/majority 检查、对齐位移位置副本、缓存 word bags 与负向 rebase 查找结果。
- **跨 `foldResidentTail` 保持单会话 TTL 规范提示**：通过无 sidecar 的 `CanonicalBaseHint` 槽位与延长 TTL，使 `responsesToCore` 复用上一轮 canonical id，并跳过冗余的二次图片索引转换与延迟初始化 multipart map。
- **启动后热会话预热**：代理启动后自动对近期活跃热会话进行预热，缩短冷启动后的首轮响应时间。

### 2. 首字延迟（TTFT）与存储 / 亲和性热路径深度优化
将可异步或可复用的工作移出首字关键路径，显著压低长会话每轮代理侧 CPU 开销：
- **预转发落盘与上游 RTT 重叠**：将转发前的持久化 ingest 与上游网络 RTT 重叠执行，配合 `checkOnly` 分叉探针与 CCR/orphans ingest 缓存，缩短请求发出前的阻塞时间。
- **SQLite 存储引擎热路径加速**：复用高频 SQLite ingest 预编译语句；零拷贝解码消息 BLOB；限定密集 block coverage 扫描范围；记忆化未变动 block 视图、delta block 边界、ref 前缀、`buildRefRows` 与元数据 JSON；规划器单次序列化身份键并避免重解析消息；将冷加载视图批量拓宽至 256。
- **时间片化 shadow 校验与已稳定分叉跳过**：对 shadow 校验做时间片切分与预发送采样延后，跳过已稳定分叉解析及不可用父快照的冷加载历史工作。
- **亲和性指纹与节流低开销化**：仅哈希保留项与分片产物、加速中性规范化，并将大型已识别会话的 `chainFingerprint` 延后至 TTFT 之外、按块让出事件循环消除流式输出抖动；无活跃模型 hold 时 `early-hold` 跳过请求体扫描；消除 `estimateRawBodyTokens` 中的临时元组与 `Object.entries` 分配。

### 3. 内置压缩内核（`kernel/`）热路径加速
在严格遵循内核无外部依赖、id 不可复用与 golden wire contract 的前提下，对 `kernel/src/` 热路径进行系统性优化：
- 优化 `protected`、`turn-integrity`、`boundaries`、`refs` 与 `sync` 模块的核心循环开销。
- 优化 `remintCoveredLiveIds`、`resolveBlockSpan`、`recommend` 共享路径，并将 `responsesToCore` canonical-id 复用切换为无 sidecar 的 `CanonicalBaseHint` 槽位设计。
- 合并上游 `v0.1.217` 内核改动后推进 `kernel/package.json` 至 `0.0.215`，内核 1,697 项单元测试与 `verif` 子集守卫全部通过。

### 4. DSH 原生宿主优化与 Android 长会话恢复基线
- **跳过宿主侧 `/responses` 全量 `JSON.parse`**：在 `dsh-native` 的 `beforeSend` recall 与增量检查中避免重复全量反序列化大体积 Responses 请求体（同时保留上游 `#5792` 无宿主消息短路守卫）。
- **会话打开预加载**：在 DSH 打开会话时预加载 billion-context 历史状态。
- **Android 长会话恢复基线**：保留并持续维护 Android 长会话冷恢复、分叉采纳与上游 `#7857` 同步+注入 preset 覆盖兼容性。

### 5. 插件流式输出净化修复
- 修复插件直通/标签过滤链路中的**前导空行雪球式累积**问题，并剥离模型偶尔泄漏的**畸形全角 DSML 标签**，保证客户端流式显示干净稳定。

### 6. Fork 独立 CI 与 Android 基线回归套件
- 新增 `.github/workflows/fork-android-long-session.yml`：仅在 `cnxysoft/billion-context` 运行，完成构建、`npm pack` 干净安装冒烟测试并上传 tarball artifact，执行 `kernel` 全量单测、30+ 项核心回归测试（含前缀缓存、存储水印、折叠对账、DSH 原生、标签过滤、`verif` 守卫等）以及真实 Codex CLI 对抗 deterministic fake upstream 的端到端测试。

---

## 变更日志

### 2026-11-29
- **Fork 结构整理与文档完善** — 将 `2026-11-23_dsh-p0-ttft` 更名为长期默认分支 `fork`，保持 `master` 纯净同步上游；更新 `README.md` / `README.zh-CN.md` 顶部 Fork 说明并新增 `FORK_CHANGELOG.md`；将 Fork CI 工作流切换至 `fork` 分支并补充 `workflow_dispatch` 与新增测试项。
- **`854a4569b`** — 合并上游 `origin/master` 至 **`v0.1.217` + PR `#7891`**（`24ba3ad09`，含自 `371930c5c` 以来的 669 个提交 / 77 个 PR），完整保留全部 34 项 Fork 优化与修复；推进 `kernel/package.json` 至 `0.0.215`；修复合并引入的 `#6785` slimCcrEnvelope 适配、`#5792` 无宿主消息早退守卫与 `#7857` 同步+延迟 preset 覆盖包装，全量 `typecheck`、`build`、kernel 1,697 单测、pi-subagents 275 单测及回归套件通过。

### 2026-11-24
- **`d675d897b`** — 实现 Responses `O(tail)` 折叠对账见证、steady-state 跳过与代理启动后热会话预热。
- **`0df300cf9`** — 新增 `src/server/responses-prefix-cache.ts` 与 `tests/responses-prefix-cache.test.ts`，为 Responses 全量重发轮次引入字节级 SHA-256 前缀缓存快路径。
- **`7a38b39db`** — 将预转发持久化 ingest 与上游 RTT 重叠执行，新增 `checkOnly` 分叉探针并缓存 CCR/orphans ingest。
- **`34f2d621a`** — 修复插件链路前导空行雪球累积问题，并剥离畸形全角 DSML 泄漏片段。
- **`36aff14bb`** — 时间片化 shadow 校验，跳过已稳定分叉解析与父会话冷加载，缓存 delta block 快照并延长 hints TTL。
- **`7165a0186`** — 在 `dsh-native` `beforeSend` recall 与增量检查中跳过对 `/responses` 请求体的全量 `JSON.parse`。
- **`ccae316ed`** — 在 Fork CI 中接入 Android 长会话基线与 kernel 单元测试套件。
- **`35eb642cc` / `f5c55bf36`** — 内核 `responsesToCore` canonical-id 复用切换为无 sidecar `CanonicalBaseHint` 槽位；跨 `foldResidentTail` 保持单会话 TTL 提示并缓存负向 rebase 查找。
- **`cccf505a4` / `bccc0fd0e` / `49f4c5f77`** — 对账支持位移位置副本对齐、word bags/负向记忆化与块/账本序列化缓存；向 `responsesToCore` 透传上一轮消息并惰性化 multipart map；内核优化 `remintCoveredLiveIds`、`resolveBlockSpan`、`recommend` 共享与 canonical-id 复用。
- **`9dc5663ae` / `3bc20aa61` / `3ac5071b4`** — 跨 `assignRefs` map 克隆按 session id 缓存 `buildRefRows`；优化内核 `protected` / `turn-integrity` / `boundaries` / `refs` / `sync` 热路径；缓存未变动元数据 JSON、在 durability skip 中钉住 `cacheLedger` 并复用增量 `RefRow` 数组。
- **`27978f482` / `029e8de67`** — 惰性化对账 continuity/majority 检查并复用未变动折叠锚点与覆盖范围；消除 `estimateRawBodyTokens` 中的临时元组与 `Object.entries` 分配。

### 2026-11-23
- **`b6eb97eff` / `96d75d49b` / `07bf39b23`** — 规划器单次序列化消息身份并将冷加载视图批量扩大至 256；`chainFingerprintAsync` 按 `neutralTurns` 分块让出事件循环消除流式抖动；复写图片索引直接复用 prepare 投影，避免二次 `responsesToCore`。
- **`994b73265` / `30fa55d9e` / `99c33c7b3` / `6cc61bf6e`** — 规划器避免重复解析消息并复用全量 ingest 行；记忆化未变动 block 视图与 delta 边界/ref 前缀；加速中性规范化并将大型会话 `chainFingerprint` 移出 TTFT；延迟预发送校验采样并在 durability skip 中跳过折叠元数据重复序列化。
- **`82091dd1a` / `c7a9c36ec` / `7f2d2d6b5` / `1bbc94d4c` / `09bf378e7` / `6eb97fdb4`** — 无活跃 hold 时跳过 `early-hold` 请求体扫描；复用 SQLite 热 ingest 语句；跳过不可用父快照历史处理；仅哈希保留项与分片产物；零拷贝解码消息 BLOB；限定密集 block coverage 扫描。
- **`bc9a19b1d` / `3613f25b4` / `24e331ec4` / `71bb7e310`** — 保留 Android 长会话恢复基线；新增 DSH 打开会话预加载 BC 历史；建立 Fork 独立 Android 基线 CI 构建与打包冒烟流程。

---

## 合并上游提示

为降低后续同步上游时的合并成本，本 Fork 的增量代码分为独立文件与高频交汇点两类：
- **独立文件（零冲突）**：`FORK_CHANGELOG.md`、`.github/workflows/fork-android-long-session.yml`、`src/server/responses-prefix-cache.ts`、`tests/responses-prefix-cache.test.ts`。
- **高频交汇接线点（合上游时重点留意）**：
  - `src/server/prepare-responses.ts`、`src/server/handle.ts`、`src/fold-reconcile.ts`、`src/prefix-affinity.ts`
  - `src/storage/store.ts`、`src/storage/ingest.ts`、`src/storage/shadow.ts`、`src/persist.ts`
  - `src/agent/dsh-native.ts`、`src/agent/fork-adopt.ts`、`src/agent/dsh-compaction-disable.ts`、`src/agent/dsh-compaction-gate.ts`
  - `kernel/src/{compress,unit-projection,refs,sync,boundaries,protected,turn-integrity,recommend,block-map,instance-reid,pipeline}.ts` 与 `kernel/src/wire/{identity,responses}.ts`
- **内核版本纪律**：合并上游并保留 `kernel/src/**` 改动时，需确保 `kernel/package.json` 版本号高于上游基线版本，且合并后依次验证 `npm run typecheck`、`npm run build`、`npm --prefix kernel test` 与 `tests/verif-subset-guard.test.ts`。
