# StellarStatsSync 项目文档

## 1. 插件架构

StellarStatsSync 由四个核心模块组成：

- `SyncTask`：在 Bukkit 主线程采集在线玩家统计快照。
- `DatabaseManager`：通过 HikariCP 执行异步 MySQL 写入。
- `WebSocketSyncManager`：维护 WebSocket 长连接、消息队列、ACK 追踪与重连调度。
- `StatsyncCommand`：提供运维命令入口（`sync`、`status`）。

事件监听器负责将玩家加入、离开、聊天事件转换为实时同步消息。插件生命周期由 `StellarStatsSync` 统一托管。

## 2. WebSocket 通信模型

连接阶段采用固定顺序：

1. 建立 WebSocket 连接。
2. 发送 `auth` 声明服务端身份信息。
3. 认证完成后投递 `snapshot_request`、`sync_state`、`stats_update`、`players_delta`。

运行阶段采用双向保活：

- 客户端发送 `heartbeat`。
- 接收 `heartbeat` 时返回 `pong`。
- 接收 `pong` 时刷新链路活跃时间。

协议细节日志（入站原文、出站 envelope、标准错误 envelope）仅在 `websocket.log.debug_verbose=true` 时输出。

## 3. 队列模型（Pending / ACK）

### 3.1 发送队列

- 队列对象：`pendingMessages`（FIFO）。
- 队列上限：`queue_limit`。
- 普通入队使用 `addLast`，超限时丢弃最旧消息。
- 发送失败回插使用 `addFirst`，超限时丢弃最新尾部消息。

### 3.2 ACK 队列

- 队列对象：`pendingAckMessages`。
- ACK 追踪范围：仅 `snapshot_request`。
- ACK 识别字段：入站 envelope 顶层 `requestId`。
- ACK 命中后清理 pending ACK 与待发队列中同 `requestId` 消息。

### 3.3 过期与恢复

- TTL：30 秒。
- 清理触发点：ACK 追踪前、重连发起前、pending ACK 恢复前。
- 恢复批次上限：单次最多 32 条。
- 恢复写入方式：`addLast`，避免旧消息长期压住新消息。

### 3.4 告警限流

- `Pending ACK cache exceeded limit`：60 秒最多一条 WARN。
- `Message queue exceeded limit` 与恢复阶段队列溢出告警：60 秒最多一条 WARN。
- 被限流的同类事件在 debug 日志中保留抑制说明。

## 4. Reconnect 策略

重连场景包括：

- 初始连接失败。
- 发送失败。
- 远端关闭连接（`onClose`）。
- 连接错误（`onError`）。

调度机制：

- 通过 `reconnect_interval_seconds` 固定间隔调度重连。
- 使用失败计数进行日志分级：
  - 到达 `reconnect_warn_threshold` 输出 WARN。
  - 到达 `reconnect_error_threshold` 输出 ERROR。
- 运行状态中保留最近一次重连原因字段，便于排障定位。

## 5. 命令说明

### `/statsync sync`

- 用途：手动触发玩家统计同步。
- 执行者：控制台、具备权限的玩家。
- 权限：`stellarstatsync.admin`。

### `/statsync status`

- 用途：查看 WebSocket 运行状态快照。
- 执行者：控制台、具备权限的玩家。
- 权限：`stellarstatsync.admin`。
- 输出项：
  - WebSocket 连接状态。
  - Endpoint（`token` / `auth_token` 参数脱敏）。
  - 发送队列长度。
  - Pending ACK 数量。
  - 最近一次 `pong` 时间（友好时间格式）。
  - 重连失败次数。
  - 最近一次重连原因。
  - `lastKnownPlayersVersion`。

## 6. 配置说明

### 6.1 MySQL

- `mysql.host`
- `mysql.port`
- `mysql.database`
- `mysql.user`
- `mysql.password`
- `mysql.maximumPoolSize`

### 6.2 定时同步

- `sync_interval_ticks`：玩家统计周期同步间隔。

### 6.3 WebSocket

- `websocket.enabled`
- `websocket.ws_url`
- `websocket.server_url`
- `websocket.path`
- `websocket.server_id`
- `websocket.server_name`
- `websocket.auth_token`（兼容字段）
- `websocket.reconnect_interval_seconds`
- `websocket.heartbeat_interval_seconds`
- `websocket.report_interval_seconds`
- `websocket.sync_chat`
- `websocket.sync_player_join_quit`
- `websocket.queue_limit`
- `websocket.connect_timeout_seconds`
- `websocket.log.suppress_normal_reconnect`
- `websocket.log.reconnect_warn_threshold`
- `websocket.log.reconnect_error_threshold`
- `websocket.log.debug_verbose`

## 7. Realtime default notes

- Default `websocket.ws_url`: `ws://127.0.0.1:3001/ws/plugin`
- Default `websocket.path`: `/ws/plugin`
- `websocket.enabled` remains `false` by default.
- To make StellarWorld backend show Plugin Online, set `websocket.enabled=true` and set `websocket.auth_token` to one of StellarRealtime `PLUGIN_TOKENS`.

## 8. 性能策略

- Bukkit 统计采集在主线程执行，数据库 I/O 在异步线程执行。
- WebSocket 发送链路使用异步串行发送，避免并发写 socket。
- 队列长度受 `queue_limit` 约束，防止内存持续膨胀。
- ACK 追踪仅覆盖必要控制类消息，降低高频业务消息缓存压力。
- `status` 命令读取现有内存状态，不执行阻塞网络或数据库调用。
