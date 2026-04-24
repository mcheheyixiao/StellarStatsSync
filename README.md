# StellarStatsSync - Internal README

---

## 🧠 插件定位

根据代码推断：

- 插件核心用途是：采集在线玩家统计并同步到 MySQL，同时通过 WebSocket 将服务器实时状态推送到网站侧服务。
- 与网站关系是“双通道”：
- 数据库通道：`users` 表用于校验“是否网站已注册玩家”，`player_stats` 表用于写入统计数据（UPSERT）。
- WebSocket 通道：向网站/网关发送 `auth`、`stats_update`、`players_delta`、`chat_message`、`heartbeat`、`snapshot_request`、`sync_state`。
- 在服务器架构中角色是：Minecraft 服务器与网站后端之间的数据桥接插件（统计同步 + 实时状态广播）。

---

## 🧱 核心架构

### 插件生命周期

- `onEnable`
- `saveDefaultConfig()` 载入默认配置。
- 读取 `debug` 开关。
- 初始化 `DatabaseManager`（失败则禁用插件）。
- 启动定时同步任务 `SyncTask`（`runTaskTimer`）。
- 注册 `PlayerQuitListener`（退服时立即异步写库）。
- 初始化 `WebSocketSyncManager`，若启用则启动并注册 `RealtimeSyncListener`。
- 注册 `/statsync sync` 命令。
- `onDisable`
- 标记 `isShuttingDown=true`，阻止新事件继续发送。
- 关闭 WebSocket 管理器。
- 执行一次停服同步（同步阻塞写库）。
- 关闭数据库连接池。

### 模块划分

- `StellarStatsSync`：插件入口、生命周期编排、配置读取。
- `DatabaseManager`：HikariCP 连接池、异步写库、注册用户校验。
- `SyncTask`：玩家统计采集与周期同步。
- `PlayerQuitListener`：玩家退服即时同步。
- `WebSocketSyncManager`：WebSocket 连接、鉴权、消息队列、心跳、重连。
- `RealtimeSyncListener`：监听加入/退出/聊天并推送到 WebSocket。
- `StatsyncCommand`：手动触发同步命令。

---

## 📡 WebSocket 通信（重点）

### 连接信息

- 启用开关：`websocket.enabled`。
- URL 优先级：
- 优先使用 `websocket.ws_url`（默认 `ws://127.0.0.1:3001/plugin`）。
- 若为空则由 `server_url + path` 拼接。
- 仅接受 `ws/wss` 协议，不合法会直接禁用 WebSocket 模块。
- 连接时机：`onEnable -> webSocketSyncManager.start() -> connectAsync("initial_start")`，即插件启用后立即连接。

### 生命周期

- 连接建立：
- `HttpClient.newWebSocketBuilder().buildAsync(...)` 发起连接。
- 成功后记录 `Connected to WebSocket server.`，然后发送 `auth`。
- 鉴权完成：
- `auth` 发送成功后 `authDelivered=true`，并立即触发一次 `stats_update`。
- 日常发送：
- 所有业务消息先入队列，再串行 `sendText` 发送。
- 接收消息：
- `onText` 支持轻量入站处理：识别 `ack` 并按 `requestId` 清理 pending。
- 断开处理：
- `onClose` 与 `onError` 均清理连接状态并触发重连。
- 关闭插件：
- 尝试 `sendClose(NORMAL_CLOSURE, "plugin_disable")`，异常则 `abort()`。

### 日志行为

- 默认日志（非 debug）：
- 正常抖动场景（如偶发 `1006`）默认静默，不输出 `Socket closed / Scheduling reconnect`。
- 当连续失败达到 `reconnect_warn_threshold` 时输出 `WARN`。
- 当连续失败达到 `reconnect_error_threshold` 时输出 `ERROR`。
- Debug 日志（`websocket.debug=true`、全局 `debug=true` 或 `websocket.log.debug_verbose=true`）：
- `[WebSocket][Debug] Connecting to ...`
- `[WebSocket][Debug] Connected to WebSocket server.`
- `[WebSocket][Debug] Scheduling reconnect in Xs (...)`
- `[WebSocket][Debug] Outbound envelope: {type=..., requestId=..., size=... bytes}`
- `[WebSocket][Debug] Socket opened.`
- `[WebSocket][Debug] Auth delivered.`
- `[WebSocket][Debug] Inbound message: ...`

---

## 🔄 重连机制（重点）

- 存在自动重连。
- 触发条件：
- 首次连接失败（`connect_failure`）。
- 发送失败（`send_failure` / `direct_send_failure_*`）。
- `onClose`（被动断连）。
- `onError`（异常）。
- 鉴权时发现无可用 socket（`missing_socket_for_auth`）。
- 重连间隔：固定间隔，来自 `websocket.reconnect_interval_seconds`（默认 5 秒）。
- 去重机制：`AtomicBoolean reconnectScheduled` 防止重复调度。
- 指数退避：当前未实现（固定间隔，无抖动、无上限退避策略）。

---

## ❤️ 心跳机制（如果存在）

- 已实现“应用层心跳”：
- 定时任务 `runTaskTimerAsynchronously`，间隔 `heartbeat_interval_seconds`（默认 20 秒）。
- 发送消息类型：`heartbeat`，payload 含 `queuedMessages`。
- 当前未实现 WebSocket 原生 `ping/pong` 探活。
- 当前未实现基于心跳应答的超时判定与主动断线恢复。

---

## 📦 数据结构

分析发送/接收的数据：

## WebSocket 协议（统一版）

```json
{
  "type": "stats_update",
  "success": true,
  "code": 0,
  "message": "ok",
  "requestId": "uuid",
  "timestamp": 1710000000000,
  "data": {},
  "payload": {}
}
```

### 字段说明

| 字段 | 含义 |
| --- | --- |
| type | 消息类型 |
| success | 是否成功 |
| code | 错误码 |
| message | 描述 |
| requestId | 请求追踪ID |
| timestamp | 毫秒时间戳 |
| data | 新结构 |
| payload | 兼容旧结构 |

### 兼容策略

- 新系统使用 `data`。
- 旧系统使用 `payload`。
- 两者始终一致（`data == payload`）。

### 类型列表

- `stats_update`
- `players_delta`
- `chat_message`
- `plugins_update`
- `server_status`
- `heartbeat`
- `auth`
- `snapshot_request`
- `sync_state`
- `ack`（入站）

### 示例（鉴权）

```json
{
  "type": "auth",
  "success": true,
  "code": 0,
  "message": "ok",
  "requestId": "uuid",
  "timestamp": 1710000000000,
  "data": {
    "serverId": "default",
    "serverName": "default",
    "platform": "bukkit",
    "version": "插件版本"
  },
  "payload": {
    "serverId": "default",
    "serverName": "default",
    "platform": "bukkit",
    "version": "插件版本"
  }
}
```

### 示例（玩家在线列表变更）

```json
{
  "type": "players_delta",
  "success": true,
  "code": 0,
  "message": "ok",
  "requestId": "uuid",
  "timestamp": 1710000000000,
  "data": {
    "added": [
      {
        "name": "PlayerA",
        "uuid": "xxxx-xxxx",
        "world": "world",
        "ping": 42,
        "version": 3
      }
    ],
    "removed": [],
    "updated": [],
    "onlineCount": 1
  },
  "payload": {
    "added": [
      {
        "name": "PlayerA",
        "uuid": "xxxx-xxxx",
        "world": "world",
        "ping": 42,
        "version": 3
      }
    ],
    "removed": [],
    "updated": [],
    "onlineCount": 1
  }
}
```

### 示例（聊天同步）

```json
{
  "type": "chat_message",
  "success": true,
  "code": 0,
  "message": "ok",
  "requestId": "uuid",
  "timestamp": 1710000000000,
  "data": {
    "channel": "global",
    "playerName": "PlayerA",
    "message": "hello",
    "level": "info"
  },
  "payload": {
    "channel": "global",
    "playerName": "PlayerA",
    "message": "hello",
    "level": "info"
  }
}
```

### 示例（心跳）

```json
{
  "type": "heartbeat",
  "success": true,
  "code": 0,
  "message": "ok",
  "requestId": "uuid",
  "timestamp": 1710000000000,
  "data": {
    "queuedMessages": 3
  },
  "payload": {
    "queuedMessages": 3
  }
}
```

### 示例（断线恢复与版本同步）

```json
{
  "type": "snapshot_request",
  "success": true,
  "code": 0,
  "message": "ok",
  "requestId": "uuid",
  "timestamp": 1710000000000,
  "data": {
    "reason": "connection_ready",
    "pendingAck": 4
  },
  "payload": {
    "reason": "connection_ready",
    "pendingAck": 4
  }
}
```

```json
{
  "type": "sync_state",
  "success": true,
  "code": 0,
  "message": "ok",
  "requestId": "uuid",
  "timestamp": 1710000000000,
  "data": {
    "versionMap": {
      "xxxx-uuid": 12
    },
    "trackedPlayers": 1,
    "pendingAck": 4
  },
  "payload": {
    "versionMap": {
      "xxxx-uuid": 12
    },
    "trackedPlayers": 1,
    "pendingAck": 4
  }
}
```

### ACK 处理说明（轻量）

- 客户端发送后会按 `requestId` 记录 pending（受 `queue_limit` 限制）。
- 收到 `type=ack` 时，按 `requestId` 调用 `removeFromPending` 清理。
- 重连成功后会把未 ACK 的消息回放到发送队列，再继续发送。

说明：

- 数据来源：
- Bukkit 玩家统计（`Statistic`）、在线列表、服务器状态（TPS/MSPT/CPU/内存/在线人数）。
- 数据用途：
- `player_stats` 入库用于网站统计展示。
- WebSocket 推送用于网站实时监控与跨端状态展示。
- DTO/记录结构：
- `PlayerStatsSnapshot`（采集快照）
- `SyncResult`（同步结果）
- `DbSyncResult`（数据库批处理结果）

---

## 🧩 事件与数据采集

- 玩家事件：
- `PlayerJoinEvent` -> 推送 `players_delta`（added/removed/updated）。
- `PlayerQuitEvent` -> 推送 `players_delta` + 立即异步写库（单玩家快照）。
- `AsyncPlayerChatEvent` -> 推送 `chat_message`（若异步事件则切回主线程调用发送）。
- 定时任务（scheduler）：
- `SyncTask.runTaskTimer`：周期采集在线玩家统计并异步写 MySQL。
- `heartbeatTask.runTaskTimerAsynchronously`：周期发送心跳。
- `reportTask.runTaskTimer`：周期发送 `stats_update`（默认每 5 秒）。
- 数据采集方式：
- 遍历在线玩家，读取 `PLAY_ONE_MINUTE/Fly/Deaths/Fish/PlayerKills`。
- 遍历 `Material.values()` 计算挖掘/放置方块总量。

---

## ⚙️ 配置系统

- 顶层配置项：
- `debug`：全局调试日志开关。
- `sync_interval_ticks`：MySQL 周期同步间隔（默认 `12000` tick）。
- `mysql`：
- `host` `port` `database` `user` `password` `maximumPoolSize`。
- `websocket`：
- `enabled`
- `ws_url` / `server_url` / `path`
- `server_id` / `server_name`
- `reconnect_interval_seconds`
- `heartbeat_interval_seconds`
- `report_interval_seconds`
- `sync_chat`
- `sync_player_join_quit`
- `debug`
- `log.suppress_normal_reconnect`
- `log.reconnect_warn_threshold`
- `log.reconnect_error_threshold`
- `log.debug_verbose`
- `queue_limit`
- `connect_timeout_seconds`
- 兼容字段：`auth_token`、`sync_plugin_status` 目前未在核心发送逻辑中使用。

## WebSocket 日志控制

新增配置：

```yaml
websocket:
  log:
    suppress_normal_reconnect: true
    reconnect_warn_threshold: 5
    reconnect_error_threshold: 15
    debug_verbose: false
```

说明：

- 正常网络抖动（如 `1006`）默认静默。
- 连续失败达到阈值后才输出 `WARN`，超过错误阈值输出 `ERROR`。
- `debug` 或 `debug_verbose` 开启时会输出完整重连细节，避免排查盲区。
- 默认减少 `Connecting/Scheduling reconnect/Connected` 这类日志刷屏。

---

## 📊 当前实现进度（非常重要）

根据代码推断：

已实现

- ✔ WebSocket 连接与鉴权发送（`auth`）
- ✔ 实时状态上报（`stats_update`）
- ✔ 聊天与玩家上下线同步（`chat_message`/`players_delta`）
- ✔ 消息队列与限流丢弃策略（超限丢弃并报警）
- ✔ 自动重连（固定间隔）
- ✔ 断线恢复基础链路（`snapshot_request` + pending 回放）
- ✔ ACK 轻量处理（`type=ack` -> pending 清理）
- ✔ MySQL 异步同步 + 停服前同步收尾
- ✔ 注册用户过滤（仅同步网站已注册玩家）

未实现 / 不完善

- ⚠️ WebSocket 入站协议处理（仅实现 ACK 轻量处理）
- ❌ 指数退避与抖动重连
- ❌ ping/pong 级别心跳与超时检测
- ⚠️ 断线原因指标化上报（已实现日志分级与失败阈值，仍未做 metrics 上报）
- ❌ 数据压缩 / 批处理上报策略（WebSocket 层）
- ❌ 配置项 `sync_plugin_status` 的实际业务逻辑

---

## ⚠️ 已知问题（必须分析）

- 断链问题（`code=1006`）风险：
- 代码会记录 close code 并重连，但无 ping/pong 与超时判定，遇到网络抖动/代理回收长连接时，异常关闭可频发。
- 网络异常处理不足：
- 当前已支持原因分层与失败计数阈值日志，但仍缺少退避、熔断与监控指标。
- 阻塞风险：
- `SyncTask` 在主线程遍历玩家与全部方块类型统计，玩家多时可能拉高 tick 开销。
- `onDisable` 执行同步数据库写入，停服阶段可能被数据库延迟拖慢。
- 消息丢失风险：
- 队列超限会主动丢弃旧/新消息（按场景），在长时断线下会发生业务数据缺口。

---

## 🧩 技术债

- 数据协议使用 `Map<String,Object>` 动态拼装，缺少强类型消息对象与协议版本管理。
- WebSocket 入站消息目前仅覆盖 ACK 轻量处理，仍缺少完整业务处理层。
- 注册校验为逐玩家 `SELECT`，高并发场景可能出现 N+1 查询开销。
- 构建体系同时存在 Maven 与 Gradle，长期可能产生依赖/打包配置漂移。
- 部分调试日志与注释出现历史编码痕迹，跨环境查看时可读性不稳定。

---

## 🧭 后续开发方向

- 网络稳定性增强：
- 引入 ping/pong、超时判定、退避重连（指数+抖动）、连接质量指标。
- 数据结构标准化：
- 定义强类型消息 DTO、协议版本字段、错误码与 ACK 语义。
- 插件与网站协议规范：
- 输出统一协议文档（消息类型、字段、时序、重试规则、幂等策略）。
- 数据同步性能优化：
- 统计采集拆分预算、按需采样、数据库批处理与注册缓存策略。
- 可观测性建设：
- 将关键状态（连接次数、重连次数、队列长度、丢弃量）指标化上报。

---

## 📝 总结

- 稳定性：中等。具备基础重连与队列保护，但缺少高质量长连接保活与精细化故障恢复。
- 可维护性：中等。模块边界清晰，但协议与数据结构偏动态，后续演进成本会逐步上升。
- 扩展性：中上。现有模块已具备扩展基础，若补齐协议规范与可观测性，可快速演进为稳定的“服务器实时同步中间层”。


