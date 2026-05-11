package org.stellarvan.stellarstatssync;

import com.google.gson.Gson;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.gson.GsonComponentSerializer;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;
import org.bukkit.scheduler.BukkitTask;
import org.stellarvan.stellarstatssync.websocket.dto.MessageEnvelope;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final int MAX_PLUGINS_PER_UPDATE = 300;
    private static final long PLUGINS_UPDATE_DEBOUNCE_TICKS = 40L;
    private static final long QUEUE_WARN_INTERVAL_MILLIS = 60_000L;
    private static final long PENDING_ACK_TTL_MILLIS = 30_000L;
    private static final long PENDING_ACK_WARN_INTERVAL_MILLIS = 60_000L;
    private static final int PENDING_ACK_RESTORE_LIMIT = 32;
    private static final long MAX_MINIMOTD_CONFIG_BYTES = 128L * 1024L;
    private static final long MINIMOTD_CACHE_TTL_MILLIS = 30_000L;
    private static final long MINIMOTD_WARN_INTERVAL_MILLIS = 60_000L;
    private static final long STATUS_FALLBACK_WARN_INTERVAL_MILLIS = 60_000L;
    private static final long STATUS_HTTP_WARN_INTERVAL_MILLIS = 60_000L;
    private static final int STATUS_RESPONSE_PREVIEW_LIMIT = 160;
    private static final PlainTextComponentSerializer PLAIN_TEXT_SERIALIZER = PlainTextComponentSerializer.plainText();
    private static final LegacyComponentSerializer LEGACY_COMPONENT_SERIALIZER = LegacyComponentSerializer.legacySection();
    private static final GsonComponentSerializer GSON_COMPONENT_SERIALIZER = GsonComponentSerializer.gson();
    private static final Pattern MINI_MESSAGE_TAG_PATTERN = Pattern.compile("<[^>]*>");
    private static final Pattern LEGACY_SECTION_COLOR_PATTERN = Pattern.compile("(?i)\\u00A7[0-9A-FK-ORX]");
    private static final Pattern LEGACY_AMPERSAND_COLOR_PATTERN = Pattern.compile("(?i)&[0-9A-FK-ORX]");
    private static final Pattern MULTI_SPACE_PATTERN = Pattern.compile("\\s{2,}");

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
    private final boolean syncPluginStatus;
    private final long pluginsReportIntervalTicks;
    private final boolean includePluginDescription;
    private final boolean includePluginAuthors;
    private final boolean wsDebug;
    private final boolean suppressNormalReconnect;
    private final int reconnectWarnThreshold;
    private final int reconnectErrorThreshold;
    private final boolean debugVerbose;
    private final int queueLimit;
    private final int connectTimeoutSeconds;
    private final boolean statusSyncEnabled;
    private final boolean exposePlayerList;
    private final boolean includePlayerUuid;
    private final boolean includeMotd;
    private final boolean includeTps;
    private final String statusServerId;
    private final String statusServerName;
    private final String publicHost;
    private final int publicPort;
    private final int fallbackOnlinePlayers;
    private final int fallbackMaxPlayers;
    private final String statusToken;
    private final boolean statusHttpEnabled;
    private final URI statusHttpEndpoint;
    private final int statusHttpRequestTimeoutSeconds;

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
    private final AtomicBoolean miniMotdReloading = new AtomicBoolean(false);

    private volatile WebSocket webSocket;
    private volatile boolean authDelivered;
    private volatile int lastKnownPlayersVersion = 0;
    private volatile long lastPongAt = 0L;
    private volatile int reconnectFailures = 0;
    private volatile long lastConnectedAt = 0L;
    private volatile String lastReconnectReason = "none";
    private volatile long lastQueueWarnAt = 0L;
    private volatile long lastPendingAckCacheWarnAt = 0L;
    private volatile long lastMiniMotdReadWarnAt = 0L;
    private volatile String cachedMiniMotdText = "";
    private volatile long cachedMiniMotdReadAt = 0L;
    private volatile BukkitTask heartbeatTask;
    private volatile BukkitTask reportTask;
    private volatile BukkitTask pluginsReportTask;
    private volatile BukkitTask pluginsDebounceTask;
    private volatile long lastStatusPushAt = 0L;
    private volatile long lastStatusResultAt = 0L;
    private volatile String lastStatusResult = "never";
    private volatile long lastStatusFallbackWarnAt = 0L;
    private volatile long lastStatusHttpWarnAt = 0L;
    private volatile String lastStatusWebSocketResult = "disabled";
    private volatile long lastStatusWebSocketResultAt = 0L;
    private volatile String lastStatusHttpResult = "disabled";
    private volatile long lastStatusHttpResultAt = 0L;

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

        String wsUrlRaw = ws != null ? ws.getString("ws_url", "") : "";
        String wsUrl = wsUrlRaw == null ? "" : wsUrlRaw.trim();
        if (wsUrl.isEmpty()) {
            String serverUrlRaw = ws != null ? ws.getString("server_url", "ws://127.0.0.1:3001") : "ws://127.0.0.1:3001";
            String serverUrl = serverUrlRaw == null || serverUrlRaw.isBlank() ? "ws://127.0.0.1:3001" : serverUrlRaw.trim();
            String pathRaw = ws != null ? ws.getString("path", "/ws/plugin") : "/ws/plugin";
            String path = pathRaw == null || pathRaw.isBlank() ? "/ws/plugin" : pathRaw.trim();
            wsUrl = buildWsUrl(serverUrl, path);
        }
        String authTokenRaw = ws != null ? ws.getString("auth_token", "") : "";
        String authToken = authTokenRaw == null ? "" : authTokenRaw.trim();
        if (!authToken.isBlank()) {
            wsUrl = appendTokenQueryParam(wsUrl, authToken);
        }

        URI parsedEndpoint = URI.create("ws://127.0.0.1:3001/ws/plugin");
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
        ConfigurationSection status = plugin.getConfig().getConfigurationSection("status");
        this.statusSyncEnabled = status == null || status.getBoolean("enabled", true);
        long statusReportSeconds = status != null
                ? status.getLong("interval-seconds", reportSeconds)
                : reportSeconds;
        this.reportIntervalTicks = Math.max(5L, statusReportSeconds) * TICKS_PER_SECOND;
        this.exposePlayerList = status == null || status.getBoolean("expose-player-list", true);
        this.includePlayerUuid = status == null || status.getBoolean("include-player-uuid", true);
        this.includeMotd = status == null || status.getBoolean("include-motd", true);
        this.includeTps = status == null || status.getBoolean("include-tps", true);
        String statusServerIdRaw = status != null ? status.getString("server-id", this.serverId) : this.serverId;
        this.statusServerId = statusServerIdRaw == null || statusServerIdRaw.isBlank()
                ? this.serverId
                : statusServerIdRaw.trim();
        String statusServerNameRaw = status != null ? status.getString("server-name", this.serverName) : this.serverName;
        this.statusServerName = statusServerNameRaw == null || statusServerNameRaw.isBlank()
                ? this.serverName
                : statusServerNameRaw.trim();
        String defaultPublicHost = resolveDefaultPublicHost(parsedEndpoint);
        String publicHostRaw = status != null ? status.getString("public-host", defaultPublicHost) : defaultPublicHost;
        this.publicHost = publicHostRaw == null || publicHostRaw.isBlank() ? defaultPublicHost : publicHostRaw.trim();
        int defaultPublicPort;
        try {
            int bukkitPort = Bukkit.getPort();
            defaultPublicPort = bukkitPort > 0 ? bukkitPort : 25565;
        } catch (Exception ignored) {
            defaultPublicPort = 25565;
        }
        int configuredPublicPort = status != null ? status.getInt("public-port", defaultPublicPort) : defaultPublicPort;
        this.publicPort = configuredPublicPort > 0 ? configuredPublicPort : defaultPublicPort;
        this.fallbackOnlinePlayers = Math.max(0, status != null ? status.getInt("fallback-online-players", 0) : 0);
        this.fallbackMaxPlayers = Math.max(0, status != null ? status.getInt("fallback-max-players", 0) : 0);
        this.statusToken = resolveStatusToken(status, ws, plugin.getConfig());
        ConfigurationSection statusHttp = status != null ? status.getConfigurationSection("http") : null;
        String statusHttpEndpointRaw = resolveStatusHttpEndpoint(status, statusHttp, plugin.getConfig());
        URI parsedStatusHttpEndpoint = null;
        boolean statusHttpEnabledFlag = statusHttp != null && statusHttp.getBoolean("enabled", false);
        if (statusHttpEnabledFlag) {
            try {
                if (statusHttpEndpointRaw == null || statusHttpEndpointRaw.isBlank()) {
                    throw new IllegalArgumentException("status.http.endpoint is blank");
                }
                parsedStatusHttpEndpoint = URI.create(statusHttpEndpointRaw.trim());
                String httpScheme = parsedStatusHttpEndpoint.getScheme();
                if (!"http".equalsIgnoreCase(httpScheme) && !"https".equalsIgnoreCase(httpScheme)) {
                    throw new IllegalArgumentException("Unsupported status HTTP scheme: " + httpScheme);
                }
            } catch (Exception ex) {
                statusHttpEnabledFlag = false;
                plugin.getLogger().warning("[Status][HTTP] Disabled: " + sanitizeConfigError(ex.getMessage()));
            }
        }
        this.statusHttpEnabled = statusHttpEnabledFlag;
        this.statusHttpEndpoint = parsedStatusHttpEndpoint;
        this.statusHttpRequestTimeoutSeconds = Math.max(1, statusHttp != null
                ? statusHttp.getInt("request-timeout-seconds", 8)
                : 8);

        this.syncChat = ws == null || ws.getBoolean("sync_chat", true);
        this.syncPlayerJoinQuit = ws == null || ws.getBoolean("sync_player_join_quit", true);
        ConfigurationSection realtime = plugin.getConfig().getConfigurationSection("realtime");
        ConfigurationSection pluginsStatus = realtime != null ? realtime.getConfigurationSection("plugins-status") : null;
        boolean legacySyncPluginStatus = ws == null || ws.getBoolean("sync_plugin_status", true);
        this.syncPluginStatus = pluginsStatus != null
                ? pluginsStatus.getBoolean("enabled", legacySyncPluginStatus)
                : legacySyncPluginStatus;
        long pluginsIntervalSeconds = pluginsStatus != null
                ? pluginsStatus.getLong("interval-seconds", 60L)
                : 60L;
        this.pluginsReportIntervalTicks = Math.max(30L, pluginsIntervalSeconds) * TICKS_PER_SECOND;
        this.includePluginDescription = pluginsStatus != null && pluginsStatus.getBoolean("include-description", false);
        this.includePluginAuthors = pluginsStatus != null && pluginsStatus.getBoolean("include-authors", false);
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

    public String getServerId() {
        return serverId;
    }

    public boolean shouldStart() {
        return enabled || (statusSyncEnabled && statusHttpEnabled);
    }

    public boolean isSyncChatEnabled() {
        return enabled && syncChat;
    }

    public boolean isStatusSyncEnabled() {
        return statusSyncEnabled && hasStatusTransportEnabled();
    }

    public boolean isPlayerListExposed() {
        return exposePlayerList;
    }

    public long getLastStatusPushAt() {
        return lastStatusPushAt;
    }

    public long getLastStatusResultAt() {
        return lastStatusResultAt;
    }

    public String getLastStatusResult() {
        refreshLastStatusSummary();
        String result = lastStatusResult;
        return result == null || result.isBlank() ? "never" : result;
    }

    public boolean isStatusHttpEnabled() {
        return statusHttpEnabled;
    }

    public String getMaskedStatusHttpEndpoint() {
        if (!statusHttpEnabled || statusHttpEndpoint == null) {
            return "";
        }
        return maskSensitiveQueryParams(statusHttpEndpoint.toString());
    }

    @SuppressWarnings("unused")
    public boolean isSyncPlayerJoinQuitEnabled() {
        return enabled && syncPlayerJoinQuit;
    }

    public boolean isPluginStatusSyncEnabled() {
        return enabled && syncPluginStatus;
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
        if (!enabled) {
            return "";
        }
        return maskSensitiveQueryParams(endpoint.toString());
    }

    public void start() {
        if (!shouldStart() || !started.compareAndSet(false, true)) {
            return;
        }

        if (enabled) {
            this.heartbeatTask = Bukkit.getScheduler().runTaskTimerAsynchronously(
                    plugin,
                    this::sendHeartbeat,
                    heartbeatIntervalTicks,
                    heartbeatIntervalTicks
            );
        }

        if (statusSyncEnabled && hasStatusTransportEnabled()) {
            this.reportTask = Bukkit.getScheduler().runTaskTimer(
                    plugin,
                    this::sendStatsSnapshot,
                    reportIntervalTicks,
                    reportIntervalTicks
            );
            if (!enabled) {
                sendStatsSnapshot();
            }
        }
        if (enabled && syncPluginStatus) {
            this.pluginsReportTask = Bukkit.getScheduler().runTaskTimer(
                    plugin,
                    this::sendPluginsSnapshot,
                    TICKS_PER_SECOND * 10L,
                    pluginsReportIntervalTicks
            );
        }

        if (enabled) {
            connectAsync("initial_start");
        }
    }

    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        cancelTask(heartbeatTask);
        cancelTask(reportTask);
        cancelTask(pluginsReportTask);
        cancelTask(pluginsDebounceTask);
        heartbeatTask = null;
        reportTask = null;
        pluginsReportTask = null;
        pluginsDebounceTask = null;

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
        if (!started.get() || playerName == null) {
            return;
        }
        if (enabled && syncPlayerJoinQuit) {
            sendPlayersDelta();
        }
        scheduleStatusSnapshotRefresh();
    }

    public void sendPlayerQuit(@SuppressWarnings("unused") String playerUuid, String playerName) {
        // playerUuid reserved for future protocol fields.
        if (!started.get() || playerName == null) {
            return;
        }
        if (enabled && syncPlayerJoinQuit) {
            sendPlayersDelta();
        }
        scheduleStatusSnapshotRefresh();
    }

    private void sendPlayersDelta() {
        if (!enabled || !started.get()) {
            return;
        }
        if (!Bukkit.isPrimaryThread()) {
            Bukkit.getScheduler().runTask(plugin, this::sendPlayersDelta);
            return;
        }
        if (!exposePlayerList) {
            clearTrackedPlayers();
            return;
        }

        OnlineSnapshot onlineSnapshot = collectOnlineSnapshot();
        Map<String, Map<String, Object>> currentPlayers = buildCurrentPlayersState(onlineSnapshot.onlinePlayers());

        PlayersDelta delta = computePlayersDelta(currentPlayers);
        if (delta.isEmpty()) {
            return;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("add", delta.add());
        payload.put("remove", delta.remove());
        payload.put("update", delta.update());
        payload.put("onlineCount", onlineSnapshot.onlineCount());
        payload.put("maxPlayers", onlineSnapshot.maxPlayers());
        payload.put("list_available", onlineSnapshot.onlineCount() == 0 || !currentPlayers.isEmpty());
        payload.put("exposed", true);
        payload.put("playerList", buildLegacyPlayerList(new ArrayList<>(currentPlayers.values())));
        payload.put("timestamp", System.currentTimeMillis());
        enqueueMessage(createSuccessEnvelope(TYPE_PLAYERS_DELTA, payload));
    }

    private void scheduleStatusSnapshotRefresh() {
        if (!started.get() || !statusSyncEnabled || !hasStatusTransportEnabled()) {
            return;
        }
        if (Bukkit.isPrimaryThread()) {
            sendStatsSnapshot();
            return;
        }
        Bukkit.getScheduler().runTask(plugin, this::sendStatsSnapshot);
    }

    public void schedulePluginsUpdateDebounced() {
        schedulePluginsUpdateDebounced(PLUGINS_UPDATE_DEBOUNCE_TICKS);
    }

    public void schedulePluginsUpdateDebounced(long delayTicks) {
        if (!enabled || !syncPluginStatus || !started.get()) {
            return;
        }

        long safeDelay = Math.max(1L, delayTicks);
        Runnable scheduleTask = () -> {
            BukkitTask existingTask = pluginsDebounceTask;
            if (existingTask != null) {
                existingTask.cancel();
            }
            pluginsDebounceTask = Bukkit.getScheduler().runTaskLater(plugin, () -> {
                pluginsDebounceTask = null;
                sendPluginsSnapshot();
            }, safeDelay);
        };

        if (Bukkit.isPrimaryThread()) {
            scheduleTask.run();
        } else {
            Bukkit.getScheduler().runTask(plugin, scheduleTask);
        }
    }

    private void sendPluginsSnapshot() {
        if (!enabled || !started.get() || !syncPluginStatus) {
            return;
        }
        if (!Bukkit.isPrimaryThread()) {
            Bukkit.getScheduler().runTask(plugin, this::sendPluginsSnapshot);
            return;
        }
        if (!isConnectionReady()) {
            return;
        }

        List<Map<String, Object>> plugins = collectPluginStatuses();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("plugins", plugins);
        payload.put("timestamp", System.currentTimeMillis());
        sendPluginsUpdate(payload);
        logDebug("Sent plugins_update: " + plugins.size() + " plugins");
    }

    private List<Map<String, Object>> collectPluginStatuses() {
        Plugin[] installedPlugins = Bukkit.getPluginManager().getPlugins();
        List<Plugin> sortedPlugins = new ArrayList<>(installedPlugins.length);
        for (Plugin pluginEntry : installedPlugins) {
            if (pluginEntry != null) {
                sortedPlugins.add(pluginEntry);
            }
        }
        sortedPlugins.sort(Comparator.comparing(pluginEntry -> pluginEntry.getName().toLowerCase(Locale.ROOT)));

        int limit = Math.min(MAX_PLUGINS_PER_UPDATE, sortedPlugins.size());
        List<Map<String, Object>> pluginStatuses = new ArrayList<>(limit);
        for (int i = 0; i < limit; i++) {
            Plugin pluginEntry = sortedPlugins.get(i);
            Map<String, Object> pluginStatus = new LinkedHashMap<>();
            pluginStatus.put("name", pluginEntry.getName());
            pluginStatus.put("version", resolvePluginVersion(pluginEntry));
            pluginStatus.put("enabled", pluginEntry.isEnabled());

            if (includePluginDescription) {
                String description = resolvePluginDescription(pluginEntry);
                if (description != null && !description.isBlank()) {
                    pluginStatus.put("description", description);
                }
            }
            if (includePluginAuthors) {
                List<String> authors = resolvePluginAuthors(pluginEntry);
                if (authors != null && !authors.isEmpty()) {
                    pluginStatus.put("authors", new ArrayList<>(authors));
                }
            }
            pluginStatuses.add(pluginStatus);
        }
        return pluginStatuses;
    }

    private String resolvePluginVersion(Plugin pluginEntry) {
        String version = pluginEntry.getPluginMeta().getVersion();
        if (version == null || version.isBlank()) {
            return "unknown";
        }
        return version;
    }

    private String resolvePluginDescription(Plugin pluginEntry) {
        String description = pluginEntry.getPluginMeta().getDescription();
        return description == null ? "" : description;
    }

    private List<String> resolvePluginAuthors(Plugin pluginEntry) {
        return pluginEntry.getPluginMeta().getAuthors();
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

    public void sendCustomEnvelope(String type, boolean success, int code, String message, Object data, String requestId) {
        if (!enabled || type == null || type.isBlank()) {
            return;
        }
        enqueueMessage(createEnvelope(type, success, code, message, data, requestId));
    }

    private void connectAsync(String reason) {
        if (!enabled || !started.get() || plugin.isShuttingDown()) {
            return;
        }
        cleanupExpiredPendingAckMessages();
        if (webSocket != null || !connecting.compareAndSet(false, true)) {
            return;
        }

        logDebug("Connecting to " + getMaskedEndpoint() + " (" + reason + ")");

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

    private void sendAuth() {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("serverId", serverId);
        payload.put("serverName", serverName);
        payload.put("platform", "bukkit");
        payload.put("version", resolvePluginVersion(plugin));

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

    private void sendStatsSnapshot() {
        if (!started.get() || !statusSyncEnabled || !hasStatusTransportEnabled()) {
            return;
        }
        if (!Bukkit.isPrimaryThread()) {
            Bukkit.getScheduler().runTask(plugin, this::sendStatsSnapshot);
            return;
        }

        OnlineSnapshot onlineSnapshot = collectOnlineSnapshot();
        int onlineCount = onlineSnapshot.onlineCount();
        int maxPlayers = onlineSnapshot.maxPlayers();
        List<Player> onlinePlayers = onlineSnapshot.onlinePlayers();

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("onlinePlayers", onlineCount);
        metrics.put("maxPlayers", maxPlayers);

        Double primaryTps = includeTps ? resolvePrimaryTps() : null;
        if (includeTps && primaryTps == null) {
            logDebug("TPS unavailable, reporting null in status payload.");
        }
        metrics.put("tps", primaryTps);

        Double mspt = resolveAverageMspt();
        metrics.put("mspt", mspt != null ? mspt : 0.0D);
        Double cpuUsage = resolveCpuUsagePercent();
        metrics.put("cpuUsage", cpuUsage != null ? cpuUsage : 0.0D);

        Map<String, Object> jvmMemory = collectJvmMemory();
        metrics.put("memoryUsedMb", asLongValue(jvmMemory.get("used_mb")));
        metrics.put("memoryMaxMb", asLongValue(jvmMemory.get("max_mb")));
        metrics.put("uptimeSeconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000L);

        Map<String, Object> motdPayload = includeMotd
                ? resolveMotdPayload(onlineCount, maxPlayers)
                : buildDisabledMotdPayload();
        String motdClean = asStringValue(motdPayload.get("clean"));
        String motdSource = asStringValue(motdPayload.get("source"));

        List<Map<String, Object>> playersList = buildPublicPlayersList(onlinePlayers);
        boolean listAvailable = exposePlayerList && (onlineCount == 0 || !playersList.isEmpty());

        Map<String, Object> playersPayload = new LinkedHashMap<>();
        playersPayload.put("online", onlineCount);
        playersPayload.put("max", maxPlayers);
        playersPayload.put("list", playersList);
        playersPayload.put("list_available", listAvailable);
        playersPayload.put("exposed", exposePlayerList);

        String serverVersionName = resolveServerVersionName();
        String bukkitVersion = safeGetBukkitVersion();

        Map<String, Object> versionPayload = new LinkedHashMap<>();
        versionPayload.put("name", serverVersionName);
        versionPayload.put("name_clean", resolveVersionNameClean(bukkitVersion, serverVersionName));
        versionPayload.put("bukkit", bukkitVersion);

        Map<String, Object> serverPayload = new LinkedHashMap<>();
        serverPayload.put("id", statusServerId);
        serverPayload.put("name", statusServerName);
        serverPayload.put("host", publicHost);
        serverPayload.put("port", publicPort);
        serverPayload.put("version", serverVersionName);

        Map<String, Object> worldPayload = collectWorldPayload();

        Map<String, Object> serverInfo = new LinkedHashMap<>();
        serverInfo.put("motd", motdClean);
        Map<String, Object> motdObject = buildMotdObject(motdPayload, motdSource, motdClean);
        serverInfo.put("motdObject", motdObject);
        serverInfo.put("world", asStringValue(worldPayload.get("primary")));
        serverInfo.put("worlds", worldPayload);
        serverInfo.put("host", publicHost);
        serverInfo.put("port", publicPort);
        serverInfo.put("address", formatServerAddress(publicHost, publicPort));

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("token", statusToken);
        payload.put("type", "status");
        payload.put("online", true);
        payload.put("server", serverPayload);
        payload.put("version", versionPayload);
        payload.put("players", playersPayload);
        payload.put("stats", metrics);
        payload.put("motd", motdPayload);
        payload.put("world", worldPayload);
        payload.put("timestamp", System.currentTimeMillis());

        // Legacy compatibility fields.
        payload.put("onlinePlayers", onlineCount);
        payload.put("maxPlayers", maxPlayers);
        payload.put("playerList", buildLegacyPlayerList(playersList));
        payload.put("playerListAvailable", listAvailable);
        payload.put("playerListExposed", exposePlayerList);
        payload.put("metrics", metrics);
        payload.put("serverInfo", serverInfo);

        enqueueStatusPayload(payload);
    }

    private OnlineSnapshot collectOnlineSnapshot() {
        List<Player> onlinePlayers = new ArrayList<>();
        int onlineCount = fallbackOnlinePlayers;
        boolean usedFallback = false;

        try {
            Collection<? extends Player> current = Bukkit.getOnlinePlayers();
            if (current != null) {
                for (Player player : current) {
                    if (player != null) {
                        onlinePlayers.add(player);
                    }
                }
                onlineCount = onlinePlayers.size();
            } else {
                usedFallback = true;
                logStatusFallbackWithRateLimit(
                        "Bukkit.getOnlinePlayers returned null, using status.fallback-online-players=" + fallbackOnlinePlayers + "."
                );
            }
        } catch (Exception ex) {
            usedFallback = true;
            logStatusFallbackWithRateLimit(
                    "Failed to read online players from Bukkit API, using status.fallback-online-players="
                            + fallbackOnlinePlayers + ". reason=" + sanitizeStatusError(ex.getMessage())
            );
        }

        int maxPlayers = fallbackMaxPlayers;
        try {
            maxPlayers = Bukkit.getMaxPlayers();
            if (maxPlayers <= 0) {
                throw new IllegalStateException("Bukkit max players <= 0");
            }
        } catch (Exception ex) {
            usedFallback = true;
            if (fallbackMaxPlayers > 0) {
                maxPlayers = fallbackMaxPlayers;
                logStatusFallbackWithRateLimit(
                        "Failed to read max players from Bukkit API, using status.fallback-max-players="
                                + fallbackMaxPlayers + ". reason=" + sanitizeStatusError(ex.getMessage())
                );
            } else {
                maxPlayers = Math.max(onlineCount, 1);
                logStatusFallbackWithRateLimit(
                        "Failed to read max players from Bukkit API, status.fallback-max-players is not set; using derived value "
                                + maxPlayers + ". reason=" + sanitizeStatusError(ex.getMessage())
                );
            }
        }

        if (maxPlayers < onlineCount) {
            maxPlayers = onlineCount;
        }

        return new OnlineSnapshot(List.copyOf(onlinePlayers), onlineCount, maxPlayers, usedFallback);
    }

    private Double resolvePrimaryTps() {
        List<Double> tpsValues = resolveTpsValues();
        if (tpsValues.isEmpty()) {
            return null;
        }
        return tpsValues.getFirst();
    }

    private List<Map<String, Object>> buildPublicPlayersList(List<Player> onlinePlayers) {
        if (!exposePlayerList || onlinePlayers == null || onlinePlayers.isEmpty()) {
            return List.of();
        }

        List<Map<String, Object>> players = new ArrayList<>(onlinePlayers.size());
        for (Player player : onlinePlayers) {
            if (player == null) {
                continue;
            }
            Map<String, Object> dto = toPlayerDto(player);
            players.add(dto);
        }
        return players;
    }

    private Map<String, Map<String, Object>> buildCurrentPlayersState(List<Player> onlinePlayers) {
        Map<String, Map<String, Object>> currentPlayers = new LinkedHashMap<>();
        if (onlinePlayers == null || onlinePlayers.isEmpty()) {
            return currentPlayers;
        }

        for (Player player : onlinePlayers) {
            if (player == null) {
                continue;
            }
            Map<String, Object> playerDto = toPlayerDto(player);
            String playerKey = resolvePlayerKey(playerDto);
            if (playerKey != null && !playerKey.isBlank()) {
                currentPlayers.put(playerKey, playerDto);
            }
        }
        return currentPlayers;
    }

    private List<String> buildLegacyPlayerList(List<Map<String, Object>> playersList) {
        if (playersList == null || playersList.isEmpty()) {
            return List.of();
        }
        List<String> names = new ArrayList<>(playersList.size());
        for (Map<String, Object> player : playersList) {
            String name = asStringValue(player.get("name"));
            if (!name.isBlank()) {
                names.add(name);
            }
        }
        return names;
    }

    private Map<String, Object> collectWorldPayload() {
        List<String> worldNames = new ArrayList<>();
        Bukkit.getWorlds().forEach(world -> {
            if (world != null && world.getName() != null && !world.getName().isBlank()) {
                worldNames.add(world.getName());
            }
        });

        Map<String, Object> worldPayload = new LinkedHashMap<>();
        worldPayload.put("primary", worldNames.isEmpty() ? "" : worldNames.getFirst());
        worldPayload.put("count", worldNames.size());
        worldPayload.put("names", worldNames);
        return worldPayload;
    }

    private Map<String, Object> buildMotdObject(Map<String, Object> motdPayload, String motdSource, String motdClean) {
        Map<String, Object> motdObject = new LinkedHashMap<>();
        motdObject.put("source", motdSource);
        motdObject.put("clean", motdClean);
        motdObject.put("format", asStringValue(motdPayload.get("format")));
        motdObject.put("plain", asStringValue(motdPayload.get("plain")));
        motdObject.put("miniMessage", asStringValue(motdPayload.get("miniMessage")));
        motdObject.put("html", asStringValue(motdPayload.get("html")));
        Object linesPayload = motdPayload.get("lines");
        motdObject.put("lines", linesPayload instanceof List<?> ? linesPayload : List.of());
        String componentJson = asStringValue(motdPayload.get("componentJson"));
        if (!componentJson.isBlank()) {
            motdObject.put("componentJson", componentJson);
        }
        return motdObject;
    }

    private Map<String, Object> buildDisabledMotdPayload() {
        Map<String, Object> motdPayload = new LinkedHashMap<>();
        motdPayload.put("source", "disabled");
        motdPayload.put("format", "plain");
        motdPayload.put("clean", "");
        motdPayload.put("plain", "");
        motdPayload.put("miniMessage", "");
        motdPayload.put("lines", List.of());
        motdPayload.put("html", "");
        return motdPayload;
    }

    private Component resolveBukkitMotdComponent() {
        try {
            Method staticMethod = Bukkit.class.getMethod("motd");
            Object staticValue = staticMethod.invoke(null);
            if (staticValue instanceof Component component) {
                return component;
            }
        } catch (Exception ignored) {
        }
        Object serverValue = tryInvokeNoArgMethod(Bukkit.getServer(), "motd");
        if (serverValue instanceof Component component) {
            return component;
        }
        return null;
    }

    private String resolveServerVersionName() {
        String bukkitName = Bukkit.getName();
        String minecraftVersion = resolveMinecraftVersion();
        if (bukkitName != null && !bukkitName.isBlank() && minecraftVersion != null && !minecraftVersion.isBlank()) {
            return bukkitName.trim() + " " + minecraftVersion.trim();
        }
        String bukkitVersion = safeGetBukkitVersion();
        if (!bukkitVersion.isBlank()) {
            return bukkitName == null || bukkitName.isBlank()
                    ? bukkitVersion
                    : bukkitName.trim() + " " + bukkitVersion;
        }
        String raw = Bukkit.getVersion();
        return raw == null || raw.isBlank() ? "unknown" : raw.trim();
    }

    private String resolveMinecraftVersion() {
        Object versionObject = tryInvokeNoArgMethod(Bukkit.getServer(), "getMinecraftVersion");
        if (versionObject instanceof String version && !version.isBlank()) {
            return version.trim();
        }
        String bukkitVersion = safeGetBukkitVersion();
        if (bukkitVersion.isBlank()) {
            return "";
        }
        int separatorIndex = bukkitVersion.indexOf('-');
        String candidate = separatorIndex >= 0 ? bukkitVersion.substring(0, separatorIndex) : bukkitVersion;
        return candidate.trim();
    }

    private String safeGetBukkitVersion() {
        try {
            String version = Bukkit.getBukkitVersion();
            return version == null ? "" : version.trim();
        } catch (Exception ignored) {
            return "";
        }
    }

    private String resolveVersionNameClean(String bukkitVersion, String serverVersionName) {
        if (bukkitVersion != null && !bukkitVersion.isBlank()) {
            int separatorIndex = bukkitVersion.indexOf('-');
            String candidate = separatorIndex >= 0 ? bukkitVersion.substring(0, separatorIndex) : bukkitVersion;
            if (!candidate.isBlank()) {
                return candidate.trim();
            }
        }
        if (serverVersionName == null || serverVersionName.isBlank()) {
            return "";
        }
        Matcher matcher = Pattern.compile("(\\d+\\.\\d+(?:\\.\\d+)?)").matcher(serverVersionName);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return serverVersionName.trim();
    }

    private void markStatusPushQueued() {
        long now = System.currentTimeMillis();
        lastStatusPushAt = now;
        if (enabled) {
            lastStatusWebSocketResult = "queued";
            lastStatusWebSocketResultAt = now;
        }
        if (statusHttpEnabled) {
            lastStatusHttpResult = "pending";
            lastStatusHttpResultAt = now;
        }
        refreshLastStatusSummary();
    }

    private void enqueueStatusPayload(Map<String, Object> payload) {
        if (payload == null) {
            return;
        }
        markStatusPushQueued();
        if (statusHttpEnabled) {
            dispatchStatusHttpPayload(payload);
        }
        if (enabled) {
            enqueueMessage(createSuccessEnvelope(TYPE_SERVER_STATUS, payload));
            enqueueMessage(createSuccessEnvelope(TYPE_STATS_UPDATE, payload));
        }
    }

    private void markStatusPushSent() {
        long now = System.currentTimeMillis();
        lastStatusWebSocketResult = "sent";
        lastStatusWebSocketResultAt = now;
        refreshLastStatusSummary();
    }

    private void markStatusPushFailed(Throwable throwable) {
        long now = System.currentTimeMillis();
        String message = throwable == null ? "unknown" : throwable.getMessage();
        lastStatusWebSocketResult = "send_failed: " + sanitizeStatusError(message);
        lastStatusWebSocketResultAt = now;
        refreshLastStatusSummary();
    }

    private void markStatusHttpSent(int statusCode) {
        lastStatusHttpResult = "HTTP " + statusCode;
        lastStatusHttpResultAt = System.currentTimeMillis();
        refreshLastStatusSummary();
    }

    private void markStatusHttpFailed(String detail) {
        lastStatusHttpResult = detail == null || detail.isBlank() ? "http_failed" : detail;
        lastStatusHttpResultAt = System.currentTimeMillis();
        refreshLastStatusSummary();
    }

    private void refreshLastStatusSummary() {
        List<String> parts = new ArrayList<>(2);
        long resultAt = lastStatusPushAt;
        if (enabled) {
            parts.add("ws=" + normalizeStatusResultValue(lastStatusWebSocketResult));
            resultAt = Math.max(resultAt, lastStatusWebSocketResultAt);
        }
        if (statusHttpEnabled) {
            parts.add("http=" + normalizeStatusResultValue(lastStatusHttpResult));
            resultAt = Math.max(resultAt, lastStatusHttpResultAt);
        }
        lastStatusResult = parts.isEmpty() ? "disabled" : String.join(", ", parts);
        lastStatusResultAt = resultAt;
    }

    private String normalizeStatusResultValue(String value) {
        return value == null || value.isBlank() ? "never" : value;
    }

    private void dispatchStatusHttpPayload(Map<String, Object> payload) {
        if (!statusHttpEnabled || statusHttpEndpoint == null || payload == null) {
            return;
        }

        HttpRequest request = HttpRequest.newBuilder(statusHttpEndpoint)
                .timeout(Duration.ofSeconds(statusHttpRequestTimeoutSeconds))
                .header("Accept", "application/json")
                .header("Content-Type", "application/json; charset=utf-8")
                .header("User-Agent", "StellarStatsSync")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(payload), StandardCharsets.UTF_8))
                .build();

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .whenComplete((response, throwable) -> {
                    if (throwable != null) {
                        markStatusHttpFailed("send_failed: " + sanitizeStatusError(throwable.getMessage()));
                        logStatusHttpWarnWithRateLimit("Status HTTP push failed: " + sanitizeStatusError(throwable.getMessage()));
                        return;
                    }

                    if (response == null) {
                        markStatusHttpFailed("empty_response");
                        logStatusHttpWarnWithRateLimit("Status HTTP push failed: empty response.");
                        return;
                    }

                    int statusCode = response.statusCode();
                    if (statusCode >= 200 && statusCode < 300) {
                        markStatusHttpSent(statusCode);
                        return;
                    }

                    String preview = previewResponseBody(response.body());
                    markStatusHttpFailed("HTTP " + statusCode);
                    StringBuilder message = new StringBuilder("Status HTTP push failed with HTTP ");
                    message.append(statusCode);
                    if (!preview.isBlank()) {
                        message.append(": ").append(sanitizeStatusError(preview));
                    }
                    logStatusHttpWarnWithRateLimit(message.toString());
                });
    }

    private boolean isStatusEnvelope(MessageEnvelope message) {
        if (message == null || message.type == null) {
            return false;
        }
        return TYPE_STATS_UPDATE.equals(message.type) || TYPE_SERVER_STATUS.equals(message.type);
    }

    private void logStatusFallbackWithRateLimit(String message) {
        long now = System.currentTimeMillis();
        boolean shouldWarn = now - lastStatusFallbackWarnAt >= STATUS_FALLBACK_WARN_INTERVAL_MILLIS;
        if (shouldWarn) {
            lastStatusFallbackWarnAt = now;
            logWarn(message);
            return;
        }
        logDebug(message + " (suppressed by 60s rate limit)");
    }

    private void logStatusHttpWarnWithRateLimit(String message) {
        long now = System.currentTimeMillis();
        boolean shouldWarn = now - lastStatusHttpWarnAt >= STATUS_HTTP_WARN_INTERVAL_MILLIS;
        if (shouldWarn) {
            lastStatusHttpWarnAt = now;
            plugin.getLogger().warning("[Status][HTTP] " + message);
            return;
        }
        logDebug("[Status][HTTP] " + message + " (suppressed by 60s rate limit)");
    }

    private String sanitizeStatusError(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        String sanitized = value.trim();
        sanitized = sanitized.replaceAll("(?i)(token|auth_token|password|authorization)\\s*[=:]\\s*[^\\s,;]+", "$1=***");
        if (sanitized.length() > 120) {
            sanitized = sanitized.substring(0, 120) + "...";
        }
        return sanitized;
    }

    private String previewResponseBody(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String normalized = value.replace('\r', ' ').replace('\n', ' ').trim();
        if (normalized.length() <= STATUS_RESPONSE_PREVIEW_LIMIT) {
            return normalized;
        }
        return normalized.substring(0, STATUS_RESPONSE_PREVIEW_LIMIT) + "...";
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

    private Map<String, Object> resolveMotdPayload(int onlinePlayers, int maxPlayers) {
        ConfigurationSection realtime = plugin.getConfig().getConfigurationSection("realtime");
        ConfigurationSection motdConfig = realtime != null ? realtime.getConfigurationSection("motd") : null;

        String configuredPlain = applyMotdPlaceholders(resolveConfiguredMotdPlain(motdConfig), onlinePlayers, maxPlayers);
        String configuredMiniMessage = applyMotdPlaceholders(resolveConfiguredMiniMessage(motdConfig), onlinePlayers, maxPlayers);
        String sourceMode = resolveMotdSourceMode(motdConfig);

        if ("bukkit".equals(sourceMode)) {
            return buildBukkitMotdPayload(onlinePlayers, maxPlayers);
        }

        if ("config".equals(sourceMode)) {
            if (!configuredPlain.isBlank() || !configuredMiniMessage.isBlank()) {
                return buildMotdPayload("config", configuredPlain, configuredMiniMessage, "");
            }
            return buildBukkitMotdPayload(onlinePlayers, maxPlayers);
        }

        if ("minimotd".equals(sourceMode)) {
            String miniMotdText = applyMotdPlaceholders(resolveMiniMotdMiniMessage(motdConfig), onlinePlayers, maxPlayers);
            if (!miniMotdText.isBlank()) {
                return buildMotdPayload("minimotd", "", miniMotdText, "");
            }
            return buildBukkitMotdPayload(onlinePlayers, maxPlayers);
        }

        if (!configuredPlain.isBlank() || !configuredMiniMessage.isBlank()) {
            return buildMotdPayload("config", configuredPlain, configuredMiniMessage, "");
        }

        String miniMotdText = applyMotdPlaceholders(resolveMiniMotdMiniMessage(motdConfig), onlinePlayers, maxPlayers);
        if (!miniMotdText.isBlank()) {
            return buildMotdPayload("minimotd", "", miniMotdText, "");
        }
        return buildBukkitMotdPayload(onlinePlayers, maxPlayers);
    }

    private Map<String, Object> buildBukkitMotdPayload(int onlinePlayers, int maxPlayers) {
        Component motdComponent = resolveBukkitMotdComponent();
        String plainText = "";
        String legacyText = "";
        String componentJson = "";

        if (motdComponent != null) {
            plainText = PLAIN_TEXT_SERIALIZER.serialize(motdComponent);
            legacyText = LEGACY_COMPONENT_SERIALIZER.serialize(motdComponent);
            try {
                componentJson = GSON_COMPONENT_SERIALIZER.serialize(motdComponent);
            } catch (Exception ignored) {
            }
        } else {
            Component motd = Bukkit.motd();
            legacyText = LegacyComponentSerializer.legacySection().serialize(motd);
            plainText = stripMiniMessageToPlain(legacyText);
        }

        plainText = applyMotdPlaceholders(plainText, onlinePlayers, maxPlayers);
        legacyText = applyMotdPlaceholders(legacyText, onlinePlayers, maxPlayers);
        String preferred = !legacyText.isBlank() ? legacyText : plainText;
        return buildMotdPayload("bukkit", preferred, "", componentJson);
    }

    private Map<String, Object> buildMotdPayload(
            String source,
            String plainCandidate,
            String miniMessageCandidate,
            String componentJsonCandidate
    ) {
        String safeSource = source == null || source.isBlank() ? "bukkit" : source;
        String safePlainCandidate = plainCandidate == null ? "" : plainCandidate.trim();
        String safeMiniMessage = miniMessageCandidate == null ? "" : miniMessageCandidate.trim();
        String safeComponentJson = componentJsonCandidate == null ? "" : componentJsonCandidate.trim();
        String format = resolveMotdFormat(safePlainCandidate, safeMiniMessage);
        String reportedMiniMessage = safeMiniMessage;
        if (reportedMiniMessage.isBlank() && "legacy".equals(format)) {
            reportedMiniMessage = safePlainCandidate;
        }

        String cleanPlain = !safePlainCandidate.isBlank()
                ? stripMiniMessageToPlain(safePlainCandidate)
                : stripMiniMessageToPlain(safeMiniMessage);
        List<Map<String, Object>> lines = buildMotdLines(safePlainCandidate, reportedMiniMessage, format);

        Map<String, Object> motdPayload = new LinkedHashMap<>();
        motdPayload.put("source", safeSource);
        motdPayload.put("format", format);
        motdPayload.put("clean", cleanPlain);
        motdPayload.put("plain", cleanPlain);
        motdPayload.put("miniMessage", reportedMiniMessage);
        motdPayload.put("lines", lines);
        motdPayload.put("html", "");
        if (!safeComponentJson.isBlank()) {
            motdPayload.put("componentJson", safeComponentJson);
        }
        return motdPayload;
    }

    private String resolveMotdSourceMode(ConfigurationSection motdConfig) {
        String configured = motdConfig != null ? motdConfig.getString("source", "auto") : "auto";
        if (configured == null) {
            return "auto";
        }
        String normalized = configured.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "config", "minimotd", "bukkit" -> normalized;
            default -> "auto";
        };
    }

    private String resolveMotdFormat(String plainCandidate, String miniMessageCandidate) {
        if (miniMessageCandidate != null && !miniMessageCandidate.isBlank()) {
            return "minimessage";
        }
        if (plainCandidate != null && containsLegacyColorCode(plainCandidate)) {
            return "legacy";
        }
        return "plain";
    }

    private boolean containsLegacyColorCode(String input) {
        if (input == null || input.isBlank()) {
            return false;
        }
        return LEGACY_SECTION_COLOR_PATTERN.matcher(input).find()
                || LEGACY_AMPERSAND_COLOR_PATTERN.matcher(input).find();
    }

    private List<Map<String, Object>> buildMotdLines(String plainCandidate, String miniMessageCandidate, String format) {
        String source = "minimessage".equals(format) ? miniMessageCandidate : plainCandidate;
        if (source == null || source.isBlank()) {
            source = miniMessageCandidate == null ? "" : miniMessageCandidate;
        }

        List<String> rawLines = splitMotdLines(source);
        List<Map<String, Object>> lines = new ArrayList<>(rawLines.size());
        for (String rawLine : rawLines) {
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("clean", stripMiniMessageToPlain(rawLine));
            line.put("miniMessage", rawLine);
            lines.add(line);
        }
        return lines;
    }

    private List<String> splitMotdLines(String input) {
        if (input == null || input.isBlank()) {
            return List.of();
        }
        String normalized = input.replace("\r\n", "\n").replace('\r', '\n').trim();
        if (normalized.isBlank()) {
            return List.of();
        }

        String[] parts = normalized.split("\n", -1);
        List<String> lines = new ArrayList<>(2);
        for (String part : parts) {
            if (lines.size() >= 2) {
                break;
            }
            if (part == null) {
                continue;
            }
            String trimmed = part.trim();
            if (!trimmed.isBlank()) {
                lines.add(trimmed);
            }
        }
        return lines;
    }

    private String resolveConfiguredMotdPlain(ConfigurationSection motdConfig) {
        if (motdConfig == null) {
            return "";
        }
        String configured = motdConfig.getString("plain", "");
        return configured == null ? "" : configured.trim();
    }

    private String resolveConfiguredMiniMessage(ConfigurationSection motdConfig) {
        if (motdConfig == null) {
            return "";
        }
        String configured = motdConfig.getString("mini-message", "");
        return configured == null ? "" : configured.trim();
    }

    private String resolveMiniMotdMiniMessage(ConfigurationSection motdConfig) {
        if (motdConfig != null && !motdConfig.getBoolean("read-minimotd-config", true)) {
            return "";
        }
        if (!isMiniMotdInstalled()) {
            return "";
        }
        String selection = motdConfig != null ? motdConfig.getString("minimotd-selection", "first") : "first";
        if (selection != null && !selection.trim().equalsIgnoreCase("first")) {
            logDebug("Unsupported realtime.motd.minimotd-selection value '" + selection + "', using 'first'.");
        }

        String cached = cachedMiniMotdText;
        long now = System.currentTimeMillis();
        if (!cached.isBlank() && now - cachedMiniMotdReadAt <= MINIMOTD_CACHE_TTL_MILLIS) {
            return cached;
        }

        if (Bukkit.isPrimaryThread()) {
            scheduleMiniMotdCacheRefresh();
            return cached;
        }
        return refreshMiniMotdCache();
    }

    private void scheduleMiniMotdCacheRefresh() {
        if (!miniMotdReloading.compareAndSet(false, true)) {
            return;
        }
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                refreshMiniMotdCache();
            } finally {
                miniMotdReloading.set(false);
            }
        });
    }

    private String refreshMiniMotdCache() {
        String parsed = readFirstMiniMotdFromMainConf();
        cachedMiniMotdText = parsed == null ? "" : parsed;
        cachedMiniMotdReadAt = System.currentTimeMillis();
        if (cachedMiniMotdText.isBlank()) {
            logMiniMotdWarnWithRateLimit("MiniMOTD detected but MOTD config could not be parsed; falling back to Bukkit MOTD.");
        }
        return cachedMiniMotdText;
    }

    private void logMiniMotdWarnWithRateLimit(String message) {
        long now = System.currentTimeMillis();
        boolean shouldWarn = now - lastMiniMotdReadWarnAt >= MINIMOTD_WARN_INTERVAL_MILLIS;
        if (shouldWarn) {
            lastMiniMotdReadWarnAt = now;
            logWarn(message);
            return;
        }
        logDebug(message + " (suppressed by 60s rate limit)");
    }

    private boolean isMiniMotdInstalled() {
        Plugin miniMotd = Bukkit.getPluginManager().getPlugin("MiniMOTD");
        return miniMotd != null && miniMotd.isEnabled();
    }

    private String readFirstMiniMotdFromMainConf() {
        List<Path> candidatePaths = List.of(
                Path.of("plugins", "MiniMOTD", "main.conf"),
                Path.of("plugins", "MiniMOTD", "motd.conf")
        );

        for (Path candidatePath : candidatePaths) {
            try {
                if (!Files.isRegularFile(candidatePath)) {
                    continue;
                }
                long size = Files.size(candidatePath);
                if (size <= 0L || size > MAX_MINIMOTD_CONFIG_BYTES) {
                    continue;
                }

                String content = Files.readString(candidatePath, StandardCharsets.UTF_8);
                String parsed = extractFirstMotdFromConfContent(content);
                if (!parsed.isBlank()) {
                    return parsed;
                }
            } catch (Exception ex) {
                logMiniMotdWarnWithRateLimit(
                        "Failed to read MiniMOTD config from "
                                + candidatePath
                                + ", falling back to Bukkit MOTD."
                );
                logDebug("MiniMOTD read exception: " + ex.getMessage());
            }
        }
        return "";
    }

    private String extractFirstMotdFromConfContent(String content) {
        if (content == null || content.isBlank()) {
            return "";
        }

        String motdsBlock = firstGroup(content, "(?is)\\bmotds\\s*=\\s*\\[(.*?)]");
        if (!motdsBlock.isBlank()) {
            String firstEntry = firstGroup(motdsBlock, "(?is)\\{(.*?)}");
            if (!firstEntry.isBlank()) {
                String parsed = extractMotdTextFromScope(firstEntry);
                if (!parsed.isBlank()) {
                    return parsed;
                }
            }
        }

        return extractMotdTextFromScope(content);
    }

    private String extractMotdTextFromScope(String scope) {
        String line1 = extractConfStringValue(scope, "line1");
        String line2 = extractConfStringValue(scope, "line2");
        if (!line1.isBlank() && !line2.isBlank()) {
            return line1 + "\n" + line2;
        }
        if (!line1.isBlank()) {
            return line1;
        }
        if (!line2.isBlank()) {
            return line2;
        }
        return "";
    }

    private String extractConfStringValue(String content, String key) {
        if (content == null || content.isBlank() || key == null || key.isBlank()) {
            return "";
        }

        Pattern quotedPattern = Pattern.compile("(?is)\\b" + Pattern.quote(key) + "\\s*=\\s*\"((?:\\\\.|[^\"\\\\])*)\"");
        Matcher quotedMatcher = quotedPattern.matcher(content);
        if (quotedMatcher.find()) {
            return decodeConfStringValue(quotedMatcher.group(1));
        }

        Pattern plainPattern = Pattern.compile("(?im)^\\s*" + Pattern.quote(key) + "\\s*=\\s*([^\\r\\n#]+)");
        Matcher plainMatcher = plainPattern.matcher(content);
        if (plainMatcher.find()) {
            String value = plainMatcher.group(1);
            if (value == null) {
                return "";
            }
            value = value.trim().replaceAll("[,\\]}\\s]+$", "");
            return decodeConfStringValue(value);
        }
        return "";
    }

    private String firstGroup(String input, String regex) {
        if (input == null || input.isBlank() || regex == null || regex.isBlank()) {
            return "";
        }
        Matcher matcher = Pattern.compile(regex).matcher(input);
        if (matcher.find()) {
            String group = matcher.group(1);
            return group == null ? "" : group.trim();
        }
        return "";
    }

    private String decodeConfStringValue(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String decoded = value
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\");
        return decoded.trim();
    }

    private String stripMiniMessageToPlain(String input) {
        if (input == null || input.isBlank()) {
            return "";
        }
        String normalized = input.replace("\r\n", "\n").replace('\r', '\n');
        String withoutTags = MINI_MESSAGE_TAG_PATTERN.matcher(normalized).replaceAll("");
        withoutTags = LEGACY_SECTION_COLOR_PATTERN.matcher(withoutTags).replaceAll("");
        withoutTags = LEGACY_AMPERSAND_COLOR_PATTERN.matcher(withoutTags).replaceAll("");
        withoutTags = withoutTags.replace('\n', ' ');
        withoutTags = MULTI_SPACE_PATTERN.matcher(withoutTags).replaceAll(" ");
        return withoutTags.trim();
    }

    private String applyMotdPlaceholders(String input, int onlinePlayers, int maxPlayers) {
        if (input == null || input.isBlank()) {
            return "";
        }
        return input
                .replace("<online>", String.valueOf(onlinePlayers))
                .replace("<max>", String.valueOf(maxPlayers))
                .replace("<players_online>", String.valueOf(onlinePlayers))
                .replace("<players_max>", String.valueOf(maxPlayers))
                .replace("%online%", String.valueOf(onlinePlayers))
                .replace("%max%", String.valueOf(maxPlayers))
                .replace("%players_online%", String.valueOf(onlinePlayers))
                .replace("%players_max%", String.valueOf(maxPlayers))
                .replace("{online}", String.valueOf(onlinePlayers))
                .replace("{max}", String.valueOf(maxPlayers))
                .replace("{players_online}", String.valueOf(onlinePlayers))
                .replace("{players_max}", String.valueOf(maxPlayers))
                .replace("{onlinePlayers}", String.valueOf(onlinePlayers))
                .replace("{maxPlayers}", String.valueOf(maxPlayers));
    }

    private long asLongValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String raw && !raw.isBlank()) {
            try {
                return Long.parseLong(raw);
            } catch (NumberFormatException ignored) {
            }
        }
        return 0L;
    }

    private String asStringValue(Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value);
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
                if (isStatusEnvelope(message)) {
                    markStatusPushFailed(throwable);
                }
                logSendFailureEnvelope(message.requestId);
                recordReconnectFailure("send_failure", -1, throwable.getMessage());
                logVerboseThrowable(throwable);
                forceCloseCurrentSocket();
                scheduleReconnect("send_failure");
                return;
            }

            trackPendingAck(message);
            if (isStatusEnvelope(message)) {
                markStatusPushSent();
            }
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
                    sendPluginsSnapshot();
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

    private MessageEnvelope createEnvelope(String type, boolean success, int code, String message, Object data, String requestId) {
        return MessageEnvelope.of(type, success, code, message, data, requestId);
    }

    @SuppressWarnings("SameParameterValue")
    private MessageEnvelope createErrorEnvelope(int code, String errorMessage, String requestId) {
        return MessageEnvelope.error(TYPE_ERROR, code, errorMessage, requestId);
    }

    private Map<String, Object> toPlayerDto(Player player) {
        Map<String, Object> dto = new LinkedHashMap<>();
        String playerName = player.getName() == null ? "" : player.getName();
        dto.put("name", playerName);
        dto.put("name_clean", stripMiniMessageToPlain(playerName));
        if (includePlayerUuid) {
            try {
                dto.put("uuid", player.getUniqueId().toString());
            } catch (Exception ignored) {
            }
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

    private void clearTrackedPlayers() {
        synchronized (playerStateLock) {
            previousPlayers.clear();
            versionMap.clear();
        }
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

    private boolean hasStatusTransportEnabled() {
        return enabled || statusHttpEnabled;
    }

    private String asNonBlankString(Object value) {
        if (value instanceof String string && !string.isBlank()) {
            return string;
        }
        return null;
    }

    private static String resolveDefaultPublicHost(URI endpoint) {
        try {
            String bukkitIp = Bukkit.getIp();
            if (bukkitIp != null && !bukkitIp.isBlank()) {
                return bukkitIp.trim();
            }
        } catch (Exception ignored) {
        }

        if (endpoint != null) {
            String host = endpoint.getHost();
            if (host != null && !host.isBlank()
                    && !"127.0.0.1".equals(host)
                    && !"0.0.0.0".equals(host)
                    && !"localhost".equalsIgnoreCase(host)) {
                return host.trim();
            }
        }
        return "";
    }

    private static String resolveStatusToken(
            ConfigurationSection status,
            ConfigurationSection websocket,
            FileConfiguration rootConfig
    ) {
        String resolved = firstNonBlank(
                status == null ? null : status.getString("token"),
                status == null ? null : status.getString("server-token"),
                status == null ? null : status.getString("auth-token"),
                websocket == null ? null : websocket.getString("auth_token"),
                websocket == null ? null : websocket.getString("token"),
                rootConfig == null ? null : rootConfig.getString("SERVER_TOKEN"),
                rootConfig == null ? null : rootConfig.getString("server_token"),
                rootConfig == null ? null : rootConfig.getString("token")
        );
        return resolved == null ? "" : resolved;
    }

    private static String resolveStatusHttpEndpoint(
            ConfigurationSection status,
            ConfigurationSection statusHttp,
            FileConfiguration rootConfig
    ) {
        String resolved = firstNonBlank(
                statusHttp == null ? null : statusHttp.getString("endpoint"),
                statusHttp == null ? null : statusHttp.getString("url"),
                status == null ? null : status.getString("endpoint"),
                status == null ? null : status.getString("url"),
                status == null ? null : status.getString("status-endpoint"),
                status == null ? null : status.getString("server-status-url"),
                rootConfig == null ? null : rootConfig.getString("SERVER_STATUS_URL"),
                rootConfig == null ? null : rootConfig.getString("server_status_url")
        );
        return resolved == null ? "" : resolved;
    }

    private static String firstNonBlank(String... values) {
        if (values == null || values.length == 0) {
            return "";
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }

    private static String sanitizeConfigError(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        String sanitized = value.trim();
        sanitized = sanitized.replaceAll("(?i)(token|auth_token|password|authorization)\\s*[=:]\\s*[^\\s,;]+", "$1=***");
        if (sanitized.length() > 160) {
            sanitized = sanitized.substring(0, 160) + "...";
        }
        return sanitized;
    }

    private static String buildWsUrl(String serverUrl, String path) {
        String normalizedServerUrl = serverUrl.endsWith("/") ? serverUrl.substring(0, serverUrl.length() - 1) : serverUrl;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return normalizedServerUrl + normalizedPath;
    }

    private static String appendTokenQueryParam(String wsUrl, String token) {
        if (wsUrl == null || wsUrl.isBlank() || token == null || token.isBlank()) {
            return wsUrl;
        }
        String lower = wsUrl.toLowerCase(Locale.ROOT);
        if (lower.contains("?token=") || lower.contains("&token=")
                || lower.contains("?auth_token=") || lower.contains("&auth_token=")) {
            return wsUrl;
        }

        int fragmentIndex = wsUrl.indexOf('#');
        String base = fragmentIndex >= 0 ? wsUrl.substring(0, fragmentIndex) : wsUrl;
        String fragment = fragmentIndex >= 0 ? wsUrl.substring(fragmentIndex) : "";
        String separator = base.contains("?") ? "&" : "?";
        return base + separator + "token=" + URLEncoder.encode(token, StandardCharsets.UTF_8) + fragment;
    }

    private static double round(double value) {
        return Math.round(value * 100.0D) / 100.0D;
    }

    private static long bytesToMb(long value) {
        return value / (1024L * 1024L);
    }

    private static String formatServerAddress(String host, int port) {
        String safeHost = host == null ? "" : host.trim();
        if (safeHost.isBlank()) {
            return Integer.toString(Math.max(0, port));
        }
        return safeHost + ":" + port;
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

    private record OnlineSnapshot(
            List<Player> onlinePlayers,
            int onlineCount,
            int maxPlayers,
            boolean usedFallback
    ) {
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

