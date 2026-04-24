package org.stellarvan.stellarstatssync;

import com.google.gson.Gson;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitTask;
import org.stellarvan.stellarstatssync.websocket.dto.MessageEnvelope;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public class WebSocketSyncManager {

    private static final long TICKS_PER_SECOND = 20L;
    private static final String TYPE_STATS_UPDATE = "stats_update";
    private static final String TYPE_PLAYERS_DELTA = "players_delta";
    private static final String TYPE_CHAT_MESSAGE = "chat_message";
    private static final String TYPE_PLUGINS_UPDATE = "plugins_update";
    private static final String TYPE_SERVER_STATUS = "server_status";
    private static final String TYPE_HEARTBEAT = "heartbeat";
    private static final String TYPE_PONG = "pong";
    private static final String TYPE_AUTH = "auth";
    private static final String TYPE_ACK = "ack";
    private static final String TYPE_SNAPSHOT = "snapshot";
    private static final String TYPE_SNAPSHOT_REQUEST = "snapshot_request";
    private static final String TYPE_SYNC_STATE = "sync_state";
    private static final String TYPE_ERROR = "error";
    private static final long QUEUE_WARN_INTERVAL_MILLIS = 60_000L;
    private static final long PENDING_ACK_TTL_MILLIS = 30_000L;
    private static final long PENDING_ACK_WARN_INTERVAL_MILLIS = 60_000L;
    private static final int PENDING_ACK_RESTORE_LIMIT = 32;

    private final StellarStatsSync plugin;
    private final Gson gson;
    private final HttpClient httpClient;

    private final boolean enabled;
    private final URI endpoint;
    private final String serverId;
    private final String serverName;
    private final long reconnectIntervalTicks;
    private final long heartbeatIntervalTicks;
    private final long reportIntervalTicks;
    private final boolean syncChat;
    private final boolean syncPlayerJoinQuit;
    private final boolean wsDebug;
    private final boolean suppressNormalReconnect;
    private final int reconnectWarnThreshold;
    private final int reconnectErrorThreshold;
    private final boolean debugVerbose;
    private final int queueLimit;
    private final int connectTimeoutSeconds;

    private final Object queueLock = new Object();
    private final ArrayDeque<MessageEnvelope> pendingMessages = new ArrayDeque<>();
    private final Object pendingAckLock = new Object();
    private final LinkedHashMap<String, MessageEnvelope> pendingAckMessages = new LinkedHashMap<>();
    private final LinkedHashMap<String, Long> pendingAckCreatedAt = new LinkedHashMap<>();
    private final Object playerStateLock = new Object();
    private final Map<String, Map<String, Object>> previousPlayers = new LinkedHashMap<>();
    private final Map<String, Integer> versionMap = new LinkedHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private final AtomicBoolean flushRunning = new AtomicBoolean(false);

    private volatile WebSocket webSocket;
    private volatile boolean authDelivered;
    private volatile int lastKnownPlayersVersion = 0;
    private volatile long lastPongAt = 0L;
    private volatile int reconnectFailures = 0;
    private volatile long lastConnectedAt = 0L;
    private volatile String lastReconnectReason = "none";
    private volatile long lastQueueWarnAt = 0L;
    private volatile long lastPendingAckCacheWarnAt = 0L;
    private volatile BukkitTask heartbeatTask;
    private volatile BukkitTask reportTask;

    public WebSocketSyncManager(StellarStatsSync plugin) {
        this.plugin = plugin;
        this.gson = new Gson();

        ConfigurationSection ws = plugin.getConfig().getConfigurationSection("websocket");
        boolean enabledFlag = ws != null && ws.getBoolean("enabled", false);
        String configuredServerId = ws != null ? ws.getString("server_id", "default") : "default";
        //noinspection ConstantConditions
        this.serverId = configuredServerId == null || configuredServerId.isBlank() ? "default" : configuredServerId.trim();
        String configuredServerName = ws != null ? ws.getString("server_name", "default") : "default";
        //noinspection ConstantConditions
        this.serverName = configuredServerName == null || configuredServerName.isBlank() ? "default" : configuredServerName.trim();

        String wsUrl = ws != null ? ws.getString("ws_url", "").trim() : "";
        if (wsUrl.isEmpty()) {
            String serverUrl = ws != null ? ws.getString("server_url", "ws://127.0.0.1:3001").trim() : "ws://127.0.0.1:3001";
            String path = ws != null ? ws.getString("path", "/plugin").trim() : "/plugin";
            wsUrl = buildWsUrl(serverUrl, path);
        }

        URI parsedEndpoint = URI.create("ws://127.0.0.1:3001/plugin");
        if (enabledFlag) {
            try {
                parsedEndpoint = URI.create(wsUrl);
                String scheme = parsedEndpoint.getScheme();
                if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
                    throw new IllegalArgumentException("Unsupported WebSocket scheme: " + scheme);
                }
            } catch (Exception ex) {
                enabledFlag = false;
                plugin.getLogger().severe("[WebSocket] Invalid endpoint, websocket module disabled: " + ex.getMessage());
            }
        }
        this.enabled = enabledFlag;
        this.endpoint = parsedEndpoint;

        long reconnectSeconds = ws != null ? ws.getLong("reconnect_interval_seconds", 5L) : 5L;
        long heartbeatSeconds = ws != null ? ws.getLong("heartbeat_interval_seconds", 20L) : 20L;
        long reportSeconds = ws != null ? ws.getLong("report_interval_seconds", 5L) : 5L;

        this.reconnectIntervalTicks = Math.max(1L, reconnectSeconds) * TICKS_PER_SECOND;
        this.heartbeatIntervalTicks = Math.max(1L, heartbeatSeconds) * TICKS_PER_SECOND;
        this.reportIntervalTicks = Math.max(5L, reportSeconds) * TICKS_PER_SECOND;

        this.syncChat = ws == null || ws.getBoolean("sync_chat", true);
        this.syncPlayerJoinQuit = ws == null || ws.getBoolean("sync_player_join_quit", true);
        this.wsDebug = ws != null && ws.getBoolean("debug", false);
        ConfigurationSection wsLog = ws != null ? ws.getConfigurationSection("log") : null;
        this.suppressNormalReconnect = wsLog == null || wsLog.getBoolean("suppress_normal_reconnect", true);
        int configuredWarnThreshold = wsLog != null ? wsLog.getInt("reconnect_warn_threshold", 5) : 5;
        int configuredErrorThreshold = wsLog != null ? wsLog.getInt("reconnect_error_threshold", 15) : 15;
        this.reconnectWarnThreshold = Math.max(1, configuredWarnThreshold);
        this.reconnectErrorThreshold = Math.max(this.reconnectWarnThreshold + 1, configuredErrorThreshold);
        this.debugVerbose = wsLog != null && wsLog.getBoolean("debug_verbose", false);
        this.queueLimit = Math.max(32, ws != null ? ws.getInt("queue_limit", 1024) : 1024);
        this.connectTimeoutSeconds = Math.max(1, ws != null ? ws.getInt("connect_timeout_seconds", 10) : 10);

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
                .build();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isSyncChatEnabled() {
        return enabled && syncChat;
    }

    @SuppressWarnings("unused")
    public boolean isSyncPlayerJoinQuitEnabled() {
        return enabled && syncPlayerJoinQuit;
    }

    public int getQueueSize() {
        return queuedSize();
    }

    public int getPendingAckSize() {
        return pendingAckSize();
    }

    public long getLastPongAt() {
        return lastPongAt;
    }

    public int getReconnectFailures() {
        return reconnectFailures;
    }

    public int getLastKnownPlayersVersion() {
        return lastKnownPlayersVersion;
    }

    public boolean isConnected() {
        return this.webSocket != null;
    }

    public String getLastReconnectReason() {
        String reason = lastReconnectReason;
        return reason == null || reason.isBlank() ? "none" : reason;
    }

    public String getMaskedEndpoint() {
        return maskSensitiveQueryParams(endpoint.toString());
    }

    public void start() {
        if (!enabled || !started.compareAndSet(false, true)) {
            return;
        }

        this.heartbeatTask = Bukkit.getScheduler().runTaskTimerAsynchronously(
                plugin,
                this::sendHeartbeat,
                heartbeatIntervalTicks,
                heartbeatIntervalTicks
        );

        this.reportTask = Bukkit.getScheduler().runTaskTimer(
                plugin,
                this::sendStatsSnapshot,
                reportIntervalTicks,
                reportIntervalTicks
        );

        connectAsync("initial_start");
    }

    public void shutdown() {
        if (!enabled || !started.compareAndSet(true, false)) {
            return;
        }

        cancelTask(heartbeatTask);
        cancelTask(reportTask);
        heartbeatTask = null;
        reportTask = null;

        reconnectScheduled.set(false);
        connecting.set(false);
        flushRunning.set(false);
        clearRuntimeCaches();

        WebSocket ws = this.webSocket;
        this.webSocket = null;
        this.authDelivered = false;

        if (ws != null) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "plugin_disable");
            } catch (Exception ex) {
                ws.abort();
            }
        }
    }

    public void sendChatMessage(@SuppressWarnings("unused") String playerUuid, String playerName, String message) {
        // playerUuid reserved for future protocol fields.
        if (!enabled || !syncChat || playerUuid == null || playerName == null || message == null) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("channel", "global");
        payload.put("playerName", playerName);
        payload.put("message", message);
        payload.put("level", "info");
        enqueueMessage(createSuccessEnvelope(TYPE_CHAT_MESSAGE, payload));
    }

    public void sendPlayerJoin(@SuppressWarnings("unused") String playerUuid, String playerName) {
        // playerUuid reserved for future protocol fields.
        if (!enabled || !syncPlayerJoinQuit || playerName == null) {
            return;
        }
        sendPlayersDelta();
    }

    public void sendPlayerQuit(@SuppressWarnings("unused") String playerUuid, String playerName) {
        // playerUuid reserved for future protocol fields.
        if (!enabled || !syncPlayerJoinQuit || playerName == null) {
            return;
        }
        sendPlayersDelta();
    }

    private void sendPlayersDelta() {
        if (!enabled || !started.get()) {
            return;
        }

        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();
        Map<String, Map<String, Object>> currentPlayers = new LinkedHashMap<>(onlinePlayers.size());
        for (Player player : onlinePlayers) {
            Map<String, Object> playerDto = toPlayerDto(player);
            String playerKey = resolvePlayerKey(playerDto);
            if (playerKey != null && !playerKey.isBlank()) {
                currentPlayers.put(playerKey, playerDto);
            }
        }

        PlayersDelta delta = computePlayersDelta(currentPlayers);
        if (delta.isEmpty()) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("add", delta.add());
        payload.put("remove", delta.remove());
        payload.put("update", delta.update());
        payload.put("onlineCount", currentPlayers.size());
        enqueueMessage(createSuccessEnvelope(TYPE_PLAYERS_DELTA, payload));
    }

    @SuppressWarnings("unused")
    // Reserved API for future outbound plugin state sync.
    public void sendPluginsUpdate(Map<String, Object> payload) {
        if (!enabled || payload == null) {
            return;
        }
        enqueueMessage(createSuccessEnvelope(TYPE_PLUGINS_UPDATE, payload));
    }

    @SuppressWarnings("unused")
    // Reserved API for future outbound server status sync.
    public void sendServerStatus(Map<String, Object> payload) {
        if (!enabled || payload == null) {
            return;
        }
        enqueueMessage(createSuccessEnvelope(TYPE_SERVER_STATUS, payload));
    }

    private void connectAsync(String reason) {
        if (!enabled || !started.get() || plugin.isShuttingDown()) {
            return;
        }
        cleanupExpiredPendingAckMessages();
        if (webSocket != null || !connecting.compareAndSet(false, true)) {
            return;
        }

        logDebug("Connecting to " + endpoint + " (" + reason + ")");

        httpClient.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(connectTimeoutSeconds))
                .buildAsync(endpoint, new WsListener())
                .whenComplete((ws, throwable) -> {
                    connecting.set(false);
                    if (!started.get() || plugin.isShuttingDown()) {
                        if (ws != null) {
                            ws.abort();
                        }
                        return;
                    }

                    if (throwable != null) {
                        recordReconnectFailure("connect_failure", -1, throwable.getMessage());
                        logVerboseThrowable(throwable);
                        scheduleReconnect("connect_failure");
                        return;
                    }

                    this.webSocket = ws;
                    this.authDelivered = false;
                    int previousFailures = markConnectionRecovered();
                    if (previousFailures >= reconnectWarnThreshold) {
                        logInfo("WebSocket reconnected after " + previousFailures + " failures.");
                    }
                    logDebug("Connected to WebSocket server.");
                    sendAuth();
                });
    }

    @SuppressWarnings("deprecation")
    private void sendAuth() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("serverId", serverId);
        payload.put("serverName", serverName);
        payload.put("platform", "bukkit");
        payload.put("version", plugin.getDescription().getVersion());

        // Fixed values ("auth", true) keep auth-send semantics stable for protocol compatibility.
        sendDirect(createSuccessEnvelope(TYPE_AUTH, payload), "auth", true);
    }

    private void sendHeartbeat() {
        //noinspection ConstantConditions
        //noinspection NegatedIfCondition
        if (!enabled || !started.get() || !isConnectionReady()) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("queuedMessages", queuedSize());
        payload.put("lastPongAt", lastPongAt);
        enqueueMessage(createSuccessEnvelope(TYPE_HEARTBEAT, payload));
    }

    @SuppressWarnings("deprecation")
    private void sendStatsSnapshot() {
        if (!enabled || !started.get()) {
            return;
        }

        Map<String, Object> metrics = new LinkedHashMap<>();

        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();
        metrics.put("onlinePlayers", onlinePlayers.size());
        metrics.put("maxPlayers", Bukkit.getMaxPlayers());

        List<Double> tpsValues = resolveTpsValues();
        if (!tpsValues.isEmpty()) {
            metrics.put("tps", tpsValues.getFirst());
        }

        Double mspt = resolveAverageMspt();
        if (mspt != null) {
            metrics.put("mspt", mspt);
        }
        Double cpuUsage = resolveCpuUsagePercent();
        if (cpuUsage != null) {
            metrics.put("cpuUsage", cpuUsage);
        }

        Map<String, Object> jvmMemory = collectJvmMemory();
        Object memoryUsedMb = jvmMemory.get("used_mb");
        if (memoryUsedMb instanceof Number value) {
            metrics.put("memoryUsedMb", value.longValue());
        }
        Object memoryMaxMb = jvmMemory.get("max_mb");
        if (memoryMaxMb instanceof Number value) {
            metrics.put("memoryMaxMb", value.longValue());
        }
        metrics.put("uptimeSeconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000L);

        Map<String, Object> serverInfo = new LinkedHashMap<>();
        serverInfo.put("motd", Bukkit.getMotd());
        if (!Bukkit.getWorlds().isEmpty()) {
            serverInfo.put("world", Bukkit.getWorlds().getFirst().getName());
        }
        String host = Bukkit.getIp();
        //noinspection ConstantConditions
        String addressHost = (host == null || host.isBlank()) ? "0.0.0.0" : host;
        serverInfo.put("address", addressHost + ":" + Bukkit.getPort());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("metrics", metrics);
        payload.put("serverInfo", serverInfo);

        enqueueMessage(createSuccessEnvelope(TYPE_STATS_UPDATE, payload));
    }

    private Map<String, Object> collectJvmMemory() {
        Runtime runtime = Runtime.getRuntime();
        long max = runtime.maxMemory();
        long total = runtime.totalMemory();
        long free = runtime.freeMemory();
        long used = total - free;

        Map<String, Object> memory = new LinkedHashMap<>();
        memory.put("used_mb", bytesToMb(used));
        memory.put("free_mb", bytesToMb(free));
        memory.put("total_mb", bytesToMb(total));
        memory.put("max_mb", bytesToMb(max));
        return memory;
    }

    private void enqueueMessage(MessageEnvelope message) {
        if (!enabled || !started.get() || message == null) {
            return;
        }

        int dropped = 0;
        synchronized (queueLock) {
            pendingMessages.addLast(message);
            while (pendingMessages.size() > queueLimit) {
                pendingMessages.pollFirst();
                dropped++;
            }
        }

        if (dropped > 0) {
            logQueueWarnWithRateLimit("Message queue exceeded limit (" + queueLimit + "), dropped oldest messages: " + dropped);
        }

        flushQueue();
    }

    private void enqueueMessageFirst(MessageEnvelope message) {
        if (message == null) {
            return;
        }

        int dropped = 0;
        synchronized (queueLock) {
            pendingMessages.addFirst(message);
            while (pendingMessages.size() > queueLimit) {
                pendingMessages.pollLast();
                dropped++;
            }
        }

        if (dropped > 0) {
            logQueueWarnWithRateLimit("Message queue exceeded limit (" + queueLimit + "), dropped newest messages: " + dropped);
        }
    }

    private void flushQueue() {
        //noinspection ConstantConditions
        //noinspection NegatedIfCondition
        if (!enabled || !started.get() || !isConnectionReady()) {
            return;
        }
        if (!flushRunning.compareAndSet(false, true)) {
            return;
        }

        sendNextQueuedMessage();
    }

    private void sendNextQueuedMessage() {
        //noinspection ConstantConditions
        //noinspection NegatedIfCondition
        if (!enabled || !started.get() || !isConnectionReady()) {
            flushRunning.set(false);
            return;
        }

        MessageEnvelope message;
        synchronized (queueLock) {
            message = pendingMessages.pollFirst();
        }

        if (message == null) {
            flushRunning.set(false);
            return;
        }

        WebSocket ws = this.webSocket;
        if (ws == null) {
            enqueueMessageFirst(message);
            flushRunning.set(false);
            return;
        }

        send(ws, message).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                enqueueMessageFirst(message);
                flushRunning.set(false);
                logSendFailureEnvelope(message.requestId);
                recordReconnectFailure("send_failure", -1, throwable.getMessage());
                logVerboseThrowable(throwable);
                forceCloseCurrentSocket();
                scheduleReconnect("send_failure");
                return;
            }

            trackPendingAck(message);
            sendNextQueuedMessage();
        });
    }

    @SuppressWarnings("SameParameterValue")
    private void sendDirect(MessageEnvelope message, String purpose, boolean markAuthDeliveredOnSuccess) {
        // parameters intentionally fixed for protocol stage
        //noinspection ConstantConditions
        if (!enabled || !started.get()) {
            return;
        }

        WebSocket ws = this.webSocket;
        if (ws == null) {
            if (markAuthDeliveredOnSuccess) {
                scheduleReconnect("missing_socket_for_auth");
            } else {
                enqueueMessage(message);
            }
            return;
        }

        send(ws, message).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                logSendFailureEnvelope(message.requestId);
                recordReconnectFailure("direct_send_failure_" + purpose, -1, throwable.getMessage());
                logVerboseThrowable(throwable);
                if (!markAuthDeliveredOnSuccess) {
                    enqueueMessageFirst(message);
                }
                forceCloseCurrentSocket();
                scheduleReconnect("direct_send_failure_" + purpose);
                return;
            }

            if (markAuthDeliveredOnSuccess) {
                authDelivered = true;
                logDebug("Auth delivered.");
                Bukkit.getScheduler().runTask(plugin, () -> {
                    sendSnapshotRequest();
                    sendSyncState();
                    sendStatsSnapshot();
                    sendPlayersDelta();
                    restorePendingAckMessagesToQueue();
                });
            } else {
                trackPendingAck(message);
            }

            flushQueue();
        });
    }

    @SuppressWarnings("unused")
    // Reserved internal direct-send path for controlled shutdown/future fast-path.
    private void trySendDirect(MessageEnvelope message) {
        //noinspection NegatedIfCondition
        if (!isConnectionReady()) {
            return;
        }
        WebSocket ws = this.webSocket;
        if (ws != null) {
            try {
                send(ws, message);
            } catch (Exception ex) {
                logWarn("Failed to send shutdown message: " + ex.getMessage());
            }
        }
    }

    private CompletionStage<WebSocket> send(WebSocket ws, MessageEnvelope message) {
        return ws.sendText(serializeEnvelope(message), true);
    }

    private String serializeEnvelope(MessageEnvelope message) {
        message.ensureCompatibility();
        String json = gson.toJson(message);
        logOutboundEnvelope(message, json);
        return json;
    }

    private void logOutboundEnvelope(MessageEnvelope message, String json) {
        if (!isProtocolDetailLoggingEnabled()) {
            return;
        }
        int size = json.getBytes(StandardCharsets.UTF_8).length;
        logDebug("Outbound envelope: {type=" + message.type + ", requestId=" + message.requestId + ", size=" + size + " bytes}");
    }

    private void logSendFailureEnvelope(String requestId) {
        if (!isProtocolDetailLoggingEnabled()) {
            return;
        }
        // Fixed values kept intentionally for protocol-consistent error reporting.
        MessageEnvelope errorEnvelope = createErrorEnvelope(4000, "WebSocket send failed", requestId);
        logDebug("Standard error envelope: {type=" + errorEnvelope.type
                + ", code=" + errorEnvelope.code
                + ", requestId=" + errorEnvelope.requestId
                + ", message=" + errorEnvelope.message + "}");
    }

    private void scheduleReconnect(String cause) {
        lastReconnectReason = cause == null || cause.isBlank() ? "unknown" : cause;
        //noinspection ConstantConditions
        if (!enabled || !started.get() || plugin.isShuttingDown()) {
            return;
        }
        if (!reconnectScheduled.compareAndSet(false, true)) {
            return;
        }

        logDebug("Scheduling reconnect in " + (reconnectIntervalTicks / TICKS_PER_SECOND) + "s (" + cause + ")");

        Bukkit.getScheduler().runTaskLaterAsynchronously(plugin, () -> {
            reconnectScheduled.set(false);
            //noinspection ConstantConditions
            if (!enabled || !started.get() || plugin.isShuttingDown()) {
                return;
            }
            connectAsync("reconnect_" + cause);
        }, reconnectIntervalTicks);
    }

    private boolean isConnectionReady() {
        return this.webSocket != null && this.authDelivered;
    }

    private int queuedSize() {
        synchronized (queueLock) {
            return pendingMessages.size();
        }
    }

    private int pendingAckSize() {
        cleanupExpiredPendingAckMessages();
        synchronized (pendingAckLock) {
            return pendingAckMessages.size();
        }
    }

    private void forceCloseCurrentSocket() {
        WebSocket ws = this.webSocket;
        this.webSocket = null;
        this.authDelivered = false;
        if (ws != null) {
            try {
                ws.abort();
            } catch (Exception ignored) {
            }
        }
    }

    private List<Double> resolveTpsValues() {
        Object tpsObj = tryInvokeNoArgMethod(Bukkit.getServer(), "getTPS");
        if (tpsObj instanceof double[] values && values.length > 0) {
            return toRoundedList(values);
        }

        Object spigotObj = tryInvokeNoArgMethod(Bukkit.getServer(), "spigot");
        if (spigotObj != null) {
            Object spigotTps = tryInvokeNoArgMethod(spigotObj, "getTPS");
            if (spigotTps instanceof double[] values && values.length > 0) {
                return toRoundedList(values);
            }
        }

        return List.of();
    }

    private Double resolveAverageMspt() {
        Object msptObj = tryInvokeNoArgMethod(Bukkit.getServer(), "getAverageTickTime");
        if (msptObj instanceof Number number) {
            return round(number.doubleValue());
        }
        return null;
    }

    private Double resolveCpuUsagePercent() {
        try {
            Object osBean = ManagementFactory.getOperatingSystemMXBean();
            Object value = tryInvokeNoArgMethod(osBean, "getSystemCpuLoad");
            if (value == null) {
                value = tryInvokeNoArgMethod(osBean, "getCpuLoad");
            }
            if (value instanceof Number number) {
                double raw = number.doubleValue();
                if (raw >= 0D) {
                    return round(Math.min(100D, raw * 100D));
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private Object tryInvokeNoArgMethod(Object target, String methodName) {
        if (target == null) {
            return null;
        }
        try {
            Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (Exception ignored) {
            return null;
        }
    }

    private List<Double> toRoundedList(double[] values) {
        int length = Math.min(3, values.length);
        List<Double> result = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            result.add(round(values[i]));
        }
        return result;
    }

    private MessageEnvelope createSuccessEnvelope(String type, Object data) {
        // fixed value for protocol consistency: null requestId triggers auto-generation.
        return createSuccessEnvelope(type, data, null);
    }

    @SuppressWarnings("SameParameterValue")
    private MessageEnvelope createSuccessEnvelope(String type, Object data, String requestId) {
        return MessageEnvelope.success(type, data, requestId);
    }

    @SuppressWarnings("SameParameterValue")
    private MessageEnvelope createErrorEnvelope(int code, String errorMessage, String requestId) {
        return MessageEnvelope.error(TYPE_ERROR, code, errorMessage, requestId);
    }

    private Map<String, Object> toPlayerDto(Player player) {
        Map<String, Object> dto = new LinkedHashMap<>();
        dto.put("name", player.getName());
        try {
            dto.put("uuid", player.getUniqueId().toString());
        } catch (Exception ignored) {
        }
        try {
            //noinspection ConstantConditions
            dto.put("world", player.getWorld() != null ? player.getWorld().getName() : null);
        } catch (Exception ignored) {
        }
        try {
            Method getPing = player.getClass().getMethod("getPing");
            Object pingValue = getPing.invoke(player);
            if (pingValue instanceof Number number) {
                dto.put("ping", number.intValue());
            }
        } catch (Exception ignored) {
        }
        return dto;
    }

    private String resolvePlayerKey(Map<String, Object> playerDto) {
        Object uuidValue = playerDto.get("uuid");
        if (uuidValue instanceof String uuid && !uuid.isBlank()) {
            return uuid;
        }

        Object nameValue = playerDto.get("name");
        if (nameValue instanceof String name && !name.isBlank()) {
            return "name:" + name;
        }
        return null;
    }

    private PlayersDelta computePlayersDelta(Map<String, Map<String, Object>> currentPlayers) {
        List<Map<String, Object>> add = new ArrayList<>();
        List<Map<String, Object>> remove = new ArrayList<>();
        List<Map<String, Object>> update = new ArrayList<>();

        synchronized (playerStateLock) {
            for (Map.Entry<String, Map<String, Object>> entry : currentPlayers.entrySet()) {
                String playerKey = entry.getKey();
                Map<String, Object> currentDto = copyPlayerDto(entry.getValue());
                Map<String, Object> previousDto = previousPlayers.get(playerKey);
                if (previousDto == null) {
                    add.add(withVersion(currentDto, nextVersionLocked(playerKey)));
                    continue;
                }
                if (!Objects.equals(previousDto, currentDto)) {
                    update.add(withVersion(currentDto, nextVersionLocked(playerKey)));
                }
            }

            for (Map.Entry<String, Map<String, Object>> previousEntry : previousPlayers.entrySet()) {
                String playerKey = previousEntry.getKey();
                if (!currentPlayers.containsKey(playerKey)) {
                    remove.add(withVersion(previousEntry.getValue(), nextVersionLocked(playerKey)));
                }
            }

            previousPlayers.clear();
            for (Map.Entry<String, Map<String, Object>> entry : currentPlayers.entrySet()) {
                previousPlayers.put(entry.getKey(), copyPlayerDto(entry.getValue()));
            }
        }

        return new PlayersDelta(add, remove, update);
    }

    private int nextVersionLocked(String playerKey) {
        int nextVersion = versionMap.getOrDefault(playerKey, 0) + 1;
        versionMap.put(playerKey, nextVersion);
        return nextVersion;
    }

    private Map<String, Object> withVersion(Map<String, Object> source, int version) {
        Map<String, Object> dto = copyPlayerDto(source);
        dto.put("version", version);
        return dto;
    }

    private Map<String, Object> copyPlayerDto(Map<String, Object> source) {
        return new LinkedHashMap<>(source);
    }

    private void sendSnapshotRequest() {
        if (!enabled || !started.get() || !isConnectionReady()) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("reason", "connection_ready");
        payload.put("pendingAck", pendingAckSize());
        enqueueMessage(createSuccessEnvelope(TYPE_SNAPSHOT_REQUEST, payload));
    }

    private void sendSyncState() {
        if (!enabled || !started.get() || !isConnectionReady()) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("players_version", lastKnownPlayersVersion);
        enqueueMessage(createSuccessEnvelope(TYPE_SYNC_STATE, payload));
    }

    private void trackPendingAck(MessageEnvelope message) {
        if (!shouldTrackAck(message)) {
            return;
        }

        cleanupExpiredPendingAckMessages();
        message.ensureCompatibility();
        String requestId = message.requestId;
        if (requestId == null || requestId.isBlank()) {
            return;
        }

        int dropped = 0;
        synchronized (pendingAckLock) {
            pendingAckMessages.put(requestId, message);
            pendingAckCreatedAt.putIfAbsent(requestId, System.currentTimeMillis());
            while (pendingAckMessages.size() > queueLimit) {
                String oldestRequestId = pendingAckMessages.entrySet().iterator().next().getKey();
                pendingAckMessages.remove(oldestRequestId);
                pendingAckCreatedAt.remove(oldestRequestId);
                dropped++;
            }
            pendingAckCreatedAt.keySet().removeIf(key -> !pendingAckMessages.containsKey(key));
        }

        if (dropped > 0) {
            logPendingAckCacheExceeded(dropped);
        }
    }

    private boolean shouldTrackAck(MessageEnvelope message) {
        if (message == null || message.type == null) {
            return false;
        }
        return TYPE_SNAPSHOT_REQUEST.equals(message.type);
    }

    private void restorePendingAckMessagesToQueue() {
        cleanupExpiredPendingAckMessages();
        List<MessageEnvelope> pendingAckSnapshot;
        synchronized (pendingAckLock) {
            if (pendingAckMessages.isEmpty()) {
                return;
            }
            pendingAckSnapshot = new ArrayList<>(pendingAckMessages.values());
        }

        int restored = 0;
        int dropped = 0;
        synchronized (queueLock) {
            Set<String> queuedRequestIds = new HashSet<>();
            for (MessageEnvelope queued : pendingMessages) {
                if (queued != null && queued.requestId != null) {
                    queuedRequestIds.add(queued.requestId);
                }
            }

            for (MessageEnvelope pending : pendingAckSnapshot) {
                if (restored >= PENDING_ACK_RESTORE_LIMIT) {
                    break;
                }
                if (pending == null) {
                    continue;
                }
                pending.ensureCompatibility();
                if (pending.requestId == null || pending.requestId.isBlank()) {
                    continue;
                }
                if (queuedRequestIds.contains(pending.requestId)) {
                    continue;
                }
                pendingMessages.addLast(pending);
                queuedRequestIds.add(pending.requestId);
                restored++;
            }

            while (pendingMessages.size() > queueLimit) {
                pendingMessages.pollFirst();
                dropped++;
            }
        }

        if (restored > 0) {
            logDebug("Restored pending ACK messages to queue: " + restored);
        }
        if (dropped > 0) {
            logQueueWarnWithRateLimit("Queue limit reached while restoring pending ACK messages, dropped: " + dropped);
        }
    }

    private void removeFromPending(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            logDebug("ACK ignored: missing requestId.");
            return;
        }

        boolean removed;
        synchronized (pendingAckLock) {
            MessageEnvelope removedEnvelope = pendingAckMessages.remove(requestId);
            Long removedCreatedAt = pendingAckCreatedAt.remove(requestId);
            removed = removedEnvelope != null || removedCreatedAt != null;
        }

        if (removed) {
            synchronized (queueLock) {
                pendingMessages.removeIf(message -> requestId.equals(message.requestId));
            }
            logDebug("ACK received: requestId=" + requestId);
            return;
        }
        logDebug("ACK ignored: requestId=" + requestId + " not found in pending cache.");
    }

    private void clearRuntimeCaches() {
        synchronized (queueLock) {
            pendingMessages.clear();
        }
        synchronized (pendingAckLock) {
            pendingAckMessages.clear();
            pendingAckCreatedAt.clear();
            lastQueueWarnAt = 0L;
            lastPendingAckCacheWarnAt = 0L;
        }
        synchronized (playerStateLock) {
            previousPlayers.clear();
            versionMap.clear();
        }
        lastKnownPlayersVersion = 0;
        lastPongAt = 0L;
    }

    private void handleInboundMessage(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            return;
        }

        Map<?, ?> envelope;
        try {
            envelope = gson.fromJson(rawPayload, Map.class);
        } catch (Exception ex) {
            logDebug("Failed to parse inbound message: " + ex.getMessage());
            return;
        }

        if (envelope == null) {
            return;
        }

        String messageType = asNonBlankString(envelope.get("type"));
        if (messageType == null) {
            return;
        }

        updateLastKnownPlayersVersion(messageType, envelope);

        if (TYPE_ACK.equalsIgnoreCase(messageType)) {
            String requestId = asNonBlankString(envelope.get("requestId"));
            removeFromPending(requestId);
            return;
        }

        if (TYPE_HEARTBEAT.equalsIgnoreCase(messageType)) {
            lastPongAt = System.currentTimeMillis();
            sendPong();
            return;
        }

        if (TYPE_PONG.equalsIgnoreCase(messageType)) {
            lastPongAt = System.currentTimeMillis();
        }
    }

    private void updateLastKnownPlayersVersion(String messageType, Map<?, ?> envelope) {
        Integer version = null;
        if (TYPE_SNAPSHOT.equalsIgnoreCase(messageType)) {
            version = extractSnapshotPlayersVersion(envelope);
        } else if (TYPE_PLAYERS_DELTA.equalsIgnoreCase(messageType)
                || TYPE_SYNC_STATE.equalsIgnoreCase(messageType)) {
            version = extractPlayersVersion(envelope);
        }

        if (version != null && version >= 0) {
            lastKnownPlayersVersion = version;
        }
    }

    private Integer extractSnapshotPlayersVersion(Map<?, ?> envelope) {
        Object playersObject = extractEnvelopeValue(envelope, "players");
        if (playersObject instanceof Map<?, ?> playersMap) {
            return asInteger(playersMap.get("version"));
        }
        return null;
    }

    private Integer extractPlayersVersion(Map<?, ?> envelope) {
        return asInteger(extractEnvelopeValue(envelope, "players_version"));
    }

    private Object extractEnvelopeValue(Map<?, ?> envelope, String key) {
        if (envelope == null || key == null || key.isBlank()) {
            return null;
        }

        Object data = envelope.get("data");
        if (data instanceof Map<?, ?> dataMap && dataMap.containsKey(key)) {
            return dataMap.get(key);
        }

        Object payload = envelope.get("payload");
        if (payload instanceof Map<?, ?> payloadMap && payloadMap.containsKey(key)) {
            return payloadMap.get(key);
        }

        return envelope.get(key);
    }

    private void cleanupExpiredPendingAckMessages() {
        long now = System.currentTimeMillis();
        Set<String> expiredRequestIds = new HashSet<>();

        synchronized (pendingAckLock) {
            if (pendingAckMessages.isEmpty() && pendingAckCreatedAt.isEmpty()) {
                return;
            }

            Set<String> requestIds = new HashSet<>(pendingAckMessages.keySet());
            requestIds.addAll(pendingAckCreatedAt.keySet());
            for (String requestId : requestIds) {
                Long createdAt = pendingAckCreatedAt.get(requestId);
                if (createdAt == null || now - createdAt > PENDING_ACK_TTL_MILLIS) {
                    pendingAckMessages.remove(requestId);
                    pendingAckCreatedAt.remove(requestId);
                    expiredRequestIds.add(requestId);
                }
            }
        }

        if (!expiredRequestIds.isEmpty()) {
            synchronized (queueLock) {
                pendingMessages.removeIf(message ->
                        message != null
                                && message.requestId != null
                                && expiredRequestIds.contains(message.requestId));
            }
            logDebug("Expired pending ACK records removed: " + expiredRequestIds.size());
        }
    }

    private void logPendingAckCacheExceeded(int dropped) {
        long now = System.currentTimeMillis();
        boolean shouldWarn;
        synchronized (pendingAckLock) {
            shouldWarn = now - lastPendingAckCacheWarnAt >= PENDING_ACK_WARN_INTERVAL_MILLIS;
            if (shouldWarn) {
                lastPendingAckCacheWarnAt = now;
            }
        }

        String message = "Pending ACK cache exceeded limit (" + queueLimit + "), dropped oldest records: " + dropped;
        if (shouldWarn) {
            logWarn(message);
        } else {
            logDebug(message + " (suppressed by 60s rate limit)");
        }
    }

    private void logQueueWarnWithRateLimit(String message) {
        long now = System.currentTimeMillis();
        boolean shouldWarn;
        synchronized (queueLock) {
            shouldWarn = now - lastQueueWarnAt >= QUEUE_WARN_INTERVAL_MILLIS;
            if (shouldWarn) {
                lastQueueWarnAt = now;
            }
        }
        if (shouldWarn) {
            logWarn(message);
        } else {
            logDebug(message + " (suppressed by 60s rate limit)");
        }
    }

    private String maskSensitiveQueryParams(String url) {
        if (url == null || url.isBlank()) {
            return "";
        }
        return url.replaceAll("(?i)([?&](token|auth_token)=)[^&]*", "$1****");
    }

    private void sendPong() {
        if (!enabled || !started.get() || !isConnectionReady()) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("queuedMessages", queuedSize());
        payload.put("receivedAt", System.currentTimeMillis());
        enqueueMessage(createSuccessEnvelope(TYPE_PONG, payload));
    }

    private Integer asInteger(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String raw && !raw.isBlank()) {
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException ignored) {
            }
        }
        return null;
    }

    private String asNonBlankString(Object value) {
        if (value instanceof String string && !string.isBlank()) {
            return string;
        }
        return null;
    }

    private static String buildWsUrl(String serverUrl, String path) {
        String normalizedServerUrl = serverUrl.endsWith("/") ? serverUrl.substring(0, serverUrl.length() - 1) : serverUrl;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return normalizedServerUrl + normalizedPath;
    }

    private static double round(double value) {
        return Math.round(value * 100.0D) / 100.0D;
    }

    private static long bytesToMb(long value) {
        return value / (1024L * 1024L);
    }

    private void cancelTask(BukkitTask task) {
        if (task != null) {
            task.cancel();
        }
    }

    private synchronized int markConnectionRecovered() {
        int previousFailures = reconnectFailures;
        reconnectFailures = 0;
        lastConnectedAt = System.currentTimeMillis();
        return previousFailures;
    }

    private synchronized void recordReconnectFailure(String source, int statusCode, String detail) {
        reconnectFailures++;

        if (isVerboseLoggingEnabled()) {
            StringBuilder debugMessage = new StringBuilder("Reconnect event: ");
            debugMessage.append(source).append(", failures=").append(reconnectFailures);
            if (statusCode >= 0) {
                debugMessage.append(", code=").append(statusCode);
            }
            if (detail != null && !detail.isBlank()) {
                debugMessage.append(", detail=").append(detail);
            }
            if (lastConnectedAt > 0L) {
                debugMessage.append(", last_connected_ago_ms=")
                        .append(Math.max(0L, System.currentTimeMillis() - lastConnectedAt));
            }
            logDebug(debugMessage.toString());
        }

        if (reconnectFailures >= reconnectErrorThreshold) {
            logError("WebSocket reconnect failed too many times: " + reconnectFailures + " (source=" + source + ")");
            return;
        }

        if (reconnectFailures >= reconnectWarnThreshold) {
            logWarn("WebSocket reconnect attempt " + reconnectFailures + " (source=" + source + ")");
            return;
        }

        if (!suppressNormalReconnect) {
            logInfo("WebSocket reconnect attempt " + reconnectFailures + " (source=" + source + ")");
        }
    }

    private void logVerboseThrowable(Throwable throwable) {
        if (throwable != null && isVerboseLoggingEnabled()) {
            plugin.getLogger().log(Level.SEVERE, "[WebSocket] Exception occurred", throwable);
        }
    }

    private boolean isVerboseLoggingEnabled() {
        return debugVerbose || wsDebug || StellarStatsSync.isDebug();
    }

    private boolean isProtocolDetailLoggingEnabled() {
        return debugVerbose;
    }

    private void logInfo(String message) {
        plugin.getLogger().info("[WebSocket] " + message);
    }

    private void logWarn(String message) {
        plugin.getLogger().warning("[WebSocket] " + message);
    }

    private void logError(String message) {
        plugin.getLogger().severe("[WebSocket] " + message);
    }

    private void logDebug(String message) {
        if (isVerboseLoggingEnabled()) {
            plugin.getLogger().info("[WebSocket][Debug] " + message);
        }
    }

    private record PlayersDelta(
            List<Map<String, Object>> add,
            List<Map<String, Object>> remove,
            List<Map<String, Object>> update
    ) {
        private boolean isEmpty() {
            return add.isEmpty() && remove.isEmpty() && update.isEmpty();
        }
    }

    private final class WsListener implements WebSocket.Listener {

        private final StringBuilder frameBuffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
            logDebug("Socket opened.");
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            frameBuffer.append(data);
            if (last) {
                String payload = frameBuffer.toString();
                frameBuffer.setLength(0);
                if (isProtocolDetailLoggingEnabled()) {
                    logDebug("Inbound message: " + payload);
                }
                handleInboundMessage(payload);
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            recordReconnectFailure("on_close", statusCode, reason);
            WebSocketSyncManager.this.webSocket = null;
            WebSocketSyncManager.this.authDelivered = false;
            if (started.get() && !plugin.isShuttingDown()) {
                scheduleReconnect("on_close");
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            String detail = error == null ? "unknown" : error.getMessage();
            recordReconnectFailure("on_error", -1, detail);
            logVerboseThrowable(error);
            forceCloseCurrentSocket();
            if (started.get() && !plugin.isShuttingDown()) {
                scheduleReconnect("on_error");
            }
        }
    }
}
