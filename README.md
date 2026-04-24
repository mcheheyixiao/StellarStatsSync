# StellarStatsSync 官方文档

## 1. 插件功能说明

StellarStatsSync 用于在 Paper/Leaf 服务器侧提供两类同步能力：

- 持续上报服务器运行状态（TPS、MSPT、CPU、内存、在线人数等）。
- 事件驱动上报在线玩家变化与聊天信息。

插件通过 WebSocket 与外部网关通信，并保持协议字段 `data/payload` 同步输出，确保多端兼容。

---

## 2. WebSocket 通信模型

### 2.1 连接流程

1. 客户端建立 WebSocket 连接。
2. 发送 `auth` 完成身份声明。
3. 认证成功后，发送顺序固定为：
   - `snapshot_request`
   - `sync_state`
   - `stats_update`
   - `players_delta`

### 2.2 运行阶段

- `stats_update` 按固定周期发送（默认 5 秒）。
- `players_delta` 按事件发送（加入/离开触发）。
- `heartbeat` 双向保活，接收端可回 `pong`。
- `ack` 用于确认 `snapshot_request`、`sync_state`。

---

## 3. 消息类型说明

| 类型 | 方向 | 说明 |
| --- | --- | --- |
| `auth` | Client -> Server | 身份声明 |
| `snapshot_request` | Client -> Server | 请求全量快照 |
| `snapshot` | Server -> Client | 快照响应 |
| `sync_state` | Client <-> Server | 版本状态同步 |
| `stats_update` | Client -> Server | 服务器状态上报 |
| `players_delta` | Client <-> Server | 玩家增量变化 |
| `chat_message` | Client -> Server | 聊天同步 |
| `heartbeat` | 双向 | 链路存活探测 |
| `pong` | 双向 | 心跳应答 |
| `ack` | Server -> Client | 请求确认 |
| `error` | 双向 | 错误回执 |

---

## 4. 数据结构说明

### 4.1 Envelope

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

### 4.2 Players（players_delta）

```json
{
  "type": "players_delta",
  "data": {
    "add": [{ "name": "Alice", "uuid": "u1", "world": "world", "ping": 45, "version": 3 }],
    "remove": [],
    "update": [],
    "onlineCount": 1
  },
  "payload": {
    "add": [{ "name": "Alice", "uuid": "u1", "world": "world", "ping": 45, "version": 3 }],
    "remove": [],
    "update": [],
    "onlineCount": 1
  }
}
```

### 4.3 Stats（stats_update）

```json
{
  "type": "stats_update",
  "data": {
    "metrics": {
      "onlinePlayers": 12,
      "maxPlayers": 100,
      "tps": 19.98,
      "mspt": 10.2,
      "cpuUsage": 24.5,
      "memoryUsedMb": 850,
      "memoryMaxMb": 2048,
      "uptimeSeconds": 3600
    },
    "serverInfo": {
      "motd": "Stellar Server",
      "world": "world",
      "address": "0.0.0.0:25565"
    }
  }
}
```

### 4.4 Chat（chat_message）

```json
{
  "type": "chat_message",
  "data": {
    "channel": "global",
    "playerName": "Alice",
    "message": "hello",
    "level": "info"
  }
}
```

### 4.5 版本状态（sync_state）

```json
{
  "type": "sync_state",
  "data": {
    "players_version": 42
  },
  "payload": {
    "players_version": 42
  }
}
```

---

## 5. 配置文件说明（websocket.yml）

> 实际部署可放在主配置文件 `config.yml` 的 `websocket` 节点，本文按 `websocket.yml` 展示。

```yaml
enabled: true
ws_url: "ws://127.0.0.1:3001/plugin"
server_id: "default"
server_name: "default"
reconnect_interval_seconds: 5
heartbeat_interval_seconds: 20
report_interval_seconds: 5
sync_chat: true
sync_player_join_quit: true
queue_limit: 1024
connect_timeout_seconds: 10
debug: false
log:
  suppress_normal_reconnect: true
  reconnect_warn_threshold: 5
  reconnect_error_threshold: 15
  debug_verbose: false
```

字段说明：

- `enabled`：WebSocket 功能开关。
- `ws_url`：完整连接地址。
- `reconnect_interval_seconds`：重连间隔。
- `heartbeat_interval_seconds`：心跳间隔。
- `report_interval_seconds`：状态上报间隔。
- `queue_limit`：发送队列长度上限。
- `sync_chat` / `sync_player_join_quit`：业务事件开关。

---

## 6. 性能策略

- 异步发送：WebSocket 发送流程在异步链路中串行执行，避免主线程阻塞。
- 队列控制：超过 `queue_limit` 后按策略丢弃超出部分，维持内存稳定。
- reconnect 策略：固定间隔重连，按失败次数输出分级日志。

---

## 7. 故障处理

- 自动重连：连接关闭或发送异常后进入调度重连。
- ACK 机制：`snapshot_request`、`sync_state` 进入 pending，收到 `ack` 后按 `requestId` 清理。
- fallback 行为：
  - 连接未就绪时消息进入队列等待发送。
  - 队列达到上限时按规则裁剪，保持服务连续可用。
  - 心跳收发保持链路活性，接收 `heartbeat` 时回 `pong`。

---

## 协议提示

- `snapshot` 入站消息中读取 `players.version`。
- `players_delta` 入站消息中读取 `players_version`。
- `sync_state` 入站消息中读取 `players_version`。
- 客户端据此维护 `lastKnownPlayersVersion`，并在 `sync_state` 出站时回传。
