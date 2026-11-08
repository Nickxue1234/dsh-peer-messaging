# dsh-peer-messaging

让一个 dsh agent 给**其他工作区**里的 agent 发消息的 DeepSeek Harness 插件。

dsh 自带的 Agent Teams 明确不支持「不同工作目录」和「跨进程」协作；这个插件补上这块：同一个 workspace（session）里的 agent 可以调用 `peer_send`，把消息投递给另一个 workspace 里活着的 agent。收件 agent 会像收到用户消息一样看到它，并可以继续干活。

- **零运行时依赖**：不 import 任何 `@deepseek-ai/*` 包，安装时不会往 profile 的 pnpm 依赖树里加东西。
- **只装在 profile 里**：`dsh plugin add link:...` 只改 profile 的 `package.json` / `dsh.profile.bundles`，不碰全局 npm、不碰 dsh 安装目录。
- **同进程即时、跨进程也能用**：同一 dsh 进程里的所有 session 通过 `ctx.agents` 直接投递；不同 dsh 进程（例如两个 `dsh web`）通过 `$DSH_HOME/peer-messaging/` 下的文件总线互通。

## 安装

```bash
cd /path/to/dsh-peer-messaging

# 本包没有依赖；安装只为拿到 pnpm-lock.yaml（沙箱里 store 不可写时加 --store-dir "$PWD/.pnpm-store"）
pnpm install

# 装进 web profile：只写 ~/.dsh/profiles/web/package.json + bundles，随之执行 profile 内 pnpm
dsh plugin --profile web add link:$PWD

# 重启 dsh web 后生效。若沙箱里 pnpm store 不可写，追加：
#   --store-dir ~/.dsh/pnpm-store
```

插件包声明了 `"dsh": { "bundle": { "patch": "./cordis.patch.yml" } }`，plugin-manager 在 install 后会自动把 `dsh-peer-messaging` 追加进 `dsh.profile.bundles` 并校验 patch，所以一条命令即可。

卸载：

```bash
dsh plugin --profile web remove dsh-peer-messaging
# 若 bundles 里仍残留，删掉 ~/.dsh/profiles/web/package.json 的 dsh.profile.bundles 中对应项再
dsh plugin --profile web install
```

## 配置

默认值即可工作。要改就在 profile 的覆盖层 `~/.dsh/profiles/<name>/cordis.patch.yml` 里按 id 覆盖（覆盖层在 bundle 之后应用）：

```yaml
- id: peer-messaging
  config:
    pollMs: 500              # 收件箱轮询间隔，50–60000
    heartbeatMs: 2000        # presence 心跳，250–600000
    presenceTtlMs: 15000     # 超过这个时间没心跳的进程视为已退出，1000–86400000
    maxMessageBytes: 65536   # 单条消息上限（UTF-8 字节），256–4MiB
    ackWaitMs: 900           # peer_send 等投递回执的时间，0–30000；0 = 只入队
    includeSubagentsInList: false  # peer_agents 默认是否列 subagent
    includeColdInList: true        # peer_agents 默认是否列"没打开窗口"的会话
    coldListTtlMs: 5000            # 关闭会话目录的缓存时间，250–600000
    resumeColdTargets: true        # 允许给关闭的会话发消息时按需唤醒它
    interject: true                # 忙的收件方用 steer 插话（进当前 turn 的下一个 step）；false = 排到下一轮
    rescueOrphans: true            # 有邮件却没人处理的会话，是否自动 resume 去投递
    rescueGraceMs: 2000            # 信封至少躺这么久才抢救，0–300000
    rescueBackoffMs: 15000         # 同一会话两次抢救的最小间隔，1000–1800000
    pairLimit: 10                  # 同一对会话在窗口内最多几条，0 = 关闭限流
    pairWindowMs: 60000            # 限流的滑动窗口，1000–3600000
    # stateDir: /absolute/shared/dir   # 默认 $DSH_HOME/peer-messaging
    # enabled: false                  # 关掉插件（不注册工具、不跑循环）
```

所有数值越界会被 clamp、类型不对会回退默认值并打 warning，不会让 dsh 启动失败。

## 工具

### `peer_agents`

列出可达 agent：本进程所有 live session（按 workspace 分组）+ 其他 dsh 进程通过 presence 上报的 session + **没有打开窗口的已持久化会话**（`status: "closed"`、`live: false`）。

参数：`workspace?`（路径或标题子串过滤）、`includeSubagents?`、`includeStale?`（是否包含心跳过期的疑似死进程）、`includeCold?`（是否包含关闭的会话，默认跟随配置）。返回每个 agent 的 `sessionId`、`workspace`、`workspaceTitle`、`status`、`kind`、`live`、`hostId`、`local`、`stale`、`self`。

> **为什么需要 `closed` 这一类**：dsh 只在会话被打开（或被 resume）时才给它挂一个 Agent。没打开的历史会话只有持久化日志、没有 Agent，`ctx.agents` 里根本看不到它们——只按 live agent 枚举的话，就只能给"已经开着窗口"的会话发消息。本插件用 `ctx.sessionQuery.listSessions()` 把这些会话也列出来，发送时再按需 resume。

### `peer_send`

```json
{ "target": "session:01J...", "message": "帮我在 beta 工作区跑一下测试", "wake": true }
```

- `target`：完整 session id、≥8 位的 session id 前缀、workspace 路径、workspace 目录名或标题（支持 `session:` / `workspace:` 前缀限定）。含糊（命中多个）会返回 `ambiguous` + `candidates`，不会乱发。
- `wake`（默认 `true`）：空闲时开新一轮；**忙的时候用 `steer()` 插话**——消息进入当前 turn 的下一个 step，收件方当场就能反应，不用等这一轮跑完（见 `interject` 配置）；`false` 时只 `inject` 上下文、**不唤醒**——对方如果当时空闲，会一直躺在它的 next-step 里，直到它因为别的原因再跑一轮才被看到；这种情况返回值是 `delivery: "injected"`（只代表已放进上下文，不代表已读）。
- 返回 `delivery`：`delivered`（`ackWaitMs` 内已确认**被收件会话真正消费**）/ `queued`（已入队或还在对方队列里等当前 turn 结束）/ `failed`（投递失败，附 `error`）。`queued` 之后对方仍可能稍后消费，不会静默丢。同进程目标还会附带 `queueState`：`queued`（还排在收件人 inbox 里）、`handed-off`（已交给 agent，等它下一轮记进会话日志）、`unknown`。
- 目标是 `closed` 会话时，会先 resume 它（web 组合走 `sessionController.resolveAgent`，否则 `ctx.agents.resume()` 并带上 profile 的默认模型），成功后再投递；resume 失败返回 `unavailable` 错误而不是静默丢弃。同一会话的并发 resume 会被合并成一次。
- 禁止发给自己（`self` 错误）。

### `peer_whoami`

返回自己的 `sessionId`、workspace、`hostId`、`stateDir`、`inboxPending`（还有几条消息在等自己）、`reachableAgents`。把这个 id 告诉对方，对方就能回复。

## 在 dsh web 里试用

1. `dsh plugin --profile web add link:$PWD` 后重启 `dsh web`。
2. 在 GUI 里开两个工作区（两个目录），各开一个对话，让两个 agent 都处于 idle。
3. 在 A 工作区的对话里说：*先调用 `peer_whoami`，再调用 `peer_agents` 列出所有 agent*——应该能看到 B 工作区的 session。
4. 接着说：*用 `peer_send` 给 B 工作区的 session 发消息，让它报一下自己所在目录*。
5. B 工作区的 agent 会被唤醒并收到 `[peer-message] ...` 消息；它可以直接用 `peer_send`（`target: session:<A 的 id>`）回复。

两个 dsh 进程（例如桌面 `dsh web` + `dsh --profile headless`）只要 `DSH_HOME` 相同（或配置同一个 `stateDir`），也能互相发现。

## 投递语义

1. 发送方把信封原子写入 `inbox/<目标 session>/<messageId>.json`。
2. 收件方进程按 `pollMs` 扫描本地 live agent 的收件箱；领取用**单次 `rename()`** 从 `inbox/` 抢到 `processing/`，所以同一个信封不会被两个 poller 同时投递。
3. 交给 agent 用 `steer()`（dsh 的插话路径）：idle 时直接开新 turn；running 时消息进入**当前 turn 的下一个 step**，收件方当场看到，不用等这一轮结束。turn 循环在每个 step 边界都会重读 inbox，看到排队的 steering 就再跑一步；`cancel()` 清空 inbox 时由第 4 条重投兜底。**只有收件会话把它作为 user 消息写进日志后，才写 `processed/` 并回 `delivered`**。想退回"排到下一轮"的旧语义就把 `interject` 设为 `false`（内部改回 `followup()`）。
4. 收件方如果丢弃了排队消息（用户点了停止、会话被归档等，dsh 会清空 inbox），插件监听 `agent/inbox/discarded`，把信封从 `processing/` 放回 `inbox/` 并重新投递，最多 3 次；仍失败才写 `failed/`。定时器每 10 秒检查一次未确认的 hand-off：还在对方队列里就继续等（上限 30 分钟），不在队列里也没写进日志才判定丢失并重投。
5. 跨进程目标无法在本进程确认消费：对方进程领取后写 `processed/` 即视为送达（至多一次），`wake:false` 的注入也是尽力而为、交给 agent 即记 `delivered`。
6. 目标会话没有任何进程打开时，本进程先 resume 出 Agent（约 0.1–1s）再按上面第 2–4 条投递。心跳只说"另一个进程声称它活着"：**心跳过期（`stale`）的记录不算 live**，发送方会在自己进程里 resume，而不是把信丢进一个没人扫的收件箱。
7. **孤儿抢救**：信封躺在 `inbox/` 里而那个会话在本进程和任何新鲜 presence 里都没有 Agent（典型场景：原 host 进程已退出），本进程每 4 个 poll（默认约 2s）扫一次收件箱，跳过刚发出的（`rescueGraceMs`）、已归档的、以及被新鲜心跳认领的，其余 resume 后立刻投递，并对同一会话做 `rescueBackoffMs` 退避。这样消息会自愈，不会等到用户手动打开那个对话框才动。若此刻另一个进程正持有该会话（内核 `session.lock` 抢不到），本进程不抢，消息留队由那边投递，`peer_send` 返回 `queued` 而不是报错。
8. 收件方看到的是一条 `role: 'user'` 的消息，`source.kind === 'peer-message'`、`form: 'relay'`，正文形如：

   ```
   [peer-message] from /home/me/projects/alpha (session 01J...)
   <正文>
   Reply with the peer_send tool (target: session:01J...) if a response is needed.
   ```

投递不依赖任何 GUI 窗口：agent 的 `steer()`/`followup()` 会直接把会话推起来跑 turn，客户端只是观察者。真正会卡住消息的只有"没有进程持有该 Agent"，这时由第 6、7 条负责唤醒（resume）并投递。

## 状态目录

默认 `$DSH_HOME/peer-messaging/`，同一次 dsh 安装的所有 profile / 进程共享：

| 路径 | 内容 |
| --- | --- |
| `hosts/<hostId>.json` | 每个 dsh 进程的 presence（pid、hostname、会话列表、心跳时间） |
| `inbox/<session>/` | 待投递信封 |
| `processing/<session>/` | 已被某个 poller 领取、正在投递 |
| `processed/<session>/` | 已投递标记（发送方回执） |
| `failed/<session>/` | 投递失败记录 |
| `tmp/` | 原子写入中转 |

`hostId` = `主机名-pid-随机串`；presence 心跳超过 `presenceTtlMs` 即视为死进程，`peer_agents` 默认不列出。

## 安全边界

- 消息以明文 JSON 落在 `stateDir`，任何能写这个目录的进程都能读、能伪造。请只给同一用户/同一信任域使用，不要把 `stateDir` 指到共享盘。
- 插件不改变 dsh 的权限模型：能调用 `peer_send` 的 agent 本来就能读写本机文件。
- 消息正文里不允许出现 `<peer-message>` 保留标签，防止有人伪造消息头的发送方。
- 同一对会话双向合计在 `pairWindowMs` 内最多 `pairLimit` 条，挡住两个 agent 互相刷屏；超过返回 `rate_limited` 并告诉重试秒数。

## 开发

```bash
pnpm test                  # node:test 单元测试（config / store / service / tools）
pnpm verify:schema         # 用已安装 dsh 里真实的 dsh-tools 校验手写 schema（找不到 dsh 时自动跳过）
pnpm smoke                 # 单元测试 + 两个真实进程之间的投递往返
pnpm verify:cross-process  # 只跑双进程投递

# pnpm 自己的 store/cache 不可写时（沙箱、只读 HOME），等价的直调方式：
node --test test/*.test.js && node scripts/verify-schema.mjs && node scripts/cross-process-smoke.mjs
```

代码结构：

| 文件 | 职责 |
| --- | --- |
| `lib/index.js` | cordis 插件入口：`name` / `inject` / `apply` |
| `lib/config.js` | 配置默认值、边界、`$DSH_HOME` 解析、校验 |
| `lib/store.js` | 文件总线：原子写、inbox 抢占、回执、presence、清理 |
| `lib/service.js` | 进程内单例：presence 心跳、轮询投递、目标解析、发送、关闭会话的 resume |
| `lib/tools.js` | 三个工具的 JSON Schema 与实现 |

工具 schema 手写为 dsh 支持的 JSON Schema 子集（单标量 `type`、不支持 `type` 数组/`pattern`/`minLength`），因此不用引 `zod` 或 `@deepseek-ai/dsh-tools`。`inject: ['tools', 'agents']` 是仅有的两个必需服务；`workingDirectory`、`workspaceRegistry` 用 `ctx.get()` 可选读取，缺失时退化为 session header 的 cwd / 目录名。

## 已知限制

- 只支持文本消息，没有附件/线程/回复关联；回复就是再发一条 `peer_send`。
- 会话没被任何进程打开时会被 resume 出来收消息，但这会让该会话在 GUI 里变成 active；如果不想这样，把 `resumeColdTargets` 设为 `false`（此时给 `closed` 会话发消息会返回 `unavailable`）。
- 只列本 `DSH_HOME` 下的持久化会话；别的机器/别的 `DSH_HOME` 的关闭会话看不到。
- 归档（archived）的会话不出现在列表里，也不能作为目标；要发消息先取消归档。
- 唤醒关闭会话时会尽量恢复它原来的 preset（`agentPreset` 或最后一次 `agent-preset/selected`）；preset 已被删除时退回默认组合并打 warning。
- 限流窗口、抢救退避都只在内存里，重启就清空。
- 本插件通过 `link:` 安装，代码改动要**重启一次 `dsh web`** 才会加载（ESM 模块被进程缓存）。
- 跨进程发现依赖共享 `stateDir`；两个 dsh 进程如果 `DSH_HOME` 不同，就互相看不见（可显式配同一个 `stateDir`）。
- 没有图形界面层：消息在 GUI 里显示为 dsh 原生的“来自会话 session-xxx”的自定义来源消息，没有可点击跳转到发送方的卡片。

## 与 dsh-agent-message 的关系

同类的现成插件是 npm 上的 [`dsh-agent-message`](https://www.npmjs.com/package/dsh-agent-message)（MIT）。它同样让会话之间互相发消息，但**只在同一个 dsh 进程内**：README 明确把「跨进程/跨机器」列为范围外。本插件在它之上多一层文件总线，因此两个 `dsh web` 之间也能发；反过来，它有两个本插件没有的能力——浏览器端 UI（Agent 消息卡片 + 点击跳转发送方 + 设置项）和 `check_delivery` 式的按 `messageId` 事后查回执。

以下几处设计是参考它之后加的：归档会话过滤、冷会话中的 subagent 过滤、唤醒时恢复原 preset、resume handle 的释放、会话标题（`readTitleSnapshots`）、正文保留标签防伪、同一对会话的限流。
