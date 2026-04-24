# StellarStatsSync WebSocket 运行规范

## 1. 模块定位

StellarStatsSync 在 Paper/Leaf 服务端通过 WebSocket 输出状态与事件数据。消息 envelope 同时提供 `data` 与 `payload` 字段，便于不同消费端按统一协议解析。

## 2. WebSocket 队列

- 发送队列：`pendingMessages`（FIFO）。
- ACK 待确认缓存：`pendingAckMessages`（按写入顺序维护）。
- 出站消息在连接未就绪时进入发送队列，连接就绪后按顺序发送。
- 发送失败时，当前消息回插至队列头部以保证重试连续性。
- ACK 追踪范围仅包含 `snapshot_request`。

## 3. ACK 生命周期

1. 客户端发送 `snapshot_request`，消息进入 pending ACK 缓存并记录创建时间。
2. 服务端返回 `ack` 后，客户端仅从 envelope 顶层 `requestId` 识别确认对象。
3. 命中时移除 pending ACK 记录，并清理发送队列中同 `requestId` 的待发项。
4. 未命中时输出调试日志，不触发告警日志。
5. `sync_state`、`stats_update`、`players_delta`、`chat_message`、`heartbeat` 不进入 pending ACK 缓存。

## 4. 队列上限

- 配置项：`queue_limit`，同时约束发送队列与 pending ACK 缓存。
- 发送队列超限：
  - 常规入队（`addLast`）时丢弃最旧消息；
  - 失败回插（`addFirst`）时丢弃最新尾部消息。
- pending ACK 缓存超限时丢弃最旧记录。
- `Pending ACK cache exceeded limit` 告警按 60 秒频率控制；调试日志保留完整细节。

## 5. 过期策略

- pending ACK TTL：30 秒。
- 过期清理触发点：
  - 进入 ACK 追踪前；
  - 重连发起前；
  - pending ACK 恢复入队前。
- 超过 TTL 的 pending ACK 记录直接丢弃，不进入恢复流程。

## 6. 故障恢复策略

- 连接关闭、发送异常、握手失败时按重连间隔进行调度重连。
- 认证完成后按固定顺序投递：
  1. `snapshot_request`
  2. `sync_state`
  3. `stats_update`
  4. `players_delta`
- pending ACK 恢复约束：
  - 恢复前执行过期过滤；
  - 单次最多恢复 32 条；
  - 采用 `addLast` 追加到队列尾部，避免旧消息长期压住新消息；
  - 队列触达上限时按发送队列裁剪策略执行。

## 7. 协议兼容性边界

- WebSocket URL 与鉴权字段由配置项提供。
- `MessageEnvelope` 结构固定为：

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

- `players_delta` 与 `stats_update` 的消息语义、字段定义与出站路径保持协议一致。
