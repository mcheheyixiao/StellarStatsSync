package org.stellarvan.stellarstatssync;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;
import org.stellarvan.stellarstatssync.bridge.litesignin.LiteSignInBridge;

import java.util.Locale;
import java.util.logging.Level;

public class StatsyncCommand implements CommandExecutor {

    private static final String ADMIN_PERMISSION = "stellarstatsync.admin";

    private final StellarStatsSync plugin;
    private final SyncTask syncTask;
    private final DatabaseManager databaseManager;
    private final WebSocketSyncManager webSocketSyncManager;
    private final LiteSignInBridge liteSignInBridge;
    private volatile long lastManualSyncAt = 0L;

    public StatsyncCommand(
            StellarStatsSync plugin,
            SyncTask syncTask,
            DatabaseManager databaseManager,
            WebSocketSyncManager webSocketSyncManager,
            LiteSignInBridge liteSignInBridge
    ) {
        this.plugin = plugin;
        this.syncTask = syncTask;
        this.databaseManager = databaseManager;
        this.webSocketSyncManager = webSocketSyncManager;
        this.liteSignInBridge = liteSignInBridge;
    }

    @Override
    public boolean onCommand(
            @NotNull CommandSender sender,
            @NotNull Command command,
            @NotNull String label,
            @NotNull String[] args
    ) {
        if (args.length < 1) {
            sendUsage(sender, label);
            return true;
        }

        String subCommand = args[0].toLowerCase(Locale.ROOT);
        return switch (subCommand) {
            case "sync" -> {
                handleSync(sender);
                yield true;
            }
            case "status" -> {
                handleStatus(sender);
                yield true;
            }
            case "doctor" -> {
                handleDoctor(sender);
                yield true;
            }
            default -> {
                sendUsage(sender, label);
                yield true;
            }
        };
    }

    private void handleSync(CommandSender sender) {
        if (!hasAdminPermission(sender)) {
            sender.sendMessage("You do not have permission to run this command.");
            return;
        }

        sender.sendMessage("Starting player stats sync...");

        // Collecting Bukkit statistics must remain on the main thread; SQL writes stay asynchronous.
        syncTask.performSync().whenComplete((result, throwable) -> Bukkit.getScheduler().runTask(plugin, () -> {
            if (throwable != null) {
                sender.sendMessage("Sync failed: " + throwable.getMessage());
                if (StellarStatsSync.isDebug()) {
                    plugin.getLogger().severe("[Debug] Manual sync failed: " + throwable.getMessage());
                    plugin.getLogger().log(Level.SEVERE, "[Debug] Manual sync exception", throwable);
                }
                return;
            }

            lastManualSyncAt = System.currentTimeMillis();
            sender.sendMessage("Sync completed: online=" + result.onlinePlayers()
                    + ", snapshots=" + result.snapshots()
                    + ", matchedRegistered=" + result.matchedUsers()
                    + ", durationMs=" + result.durationMs());
        }));
    }

    private void handleStatus(CommandSender sender) {
        if (!hasAdminPermission(sender)) {
            sender.sendMessage("You do not have permission to run this command.");
            return;
        }

        WebSocketSyncManager manager = this.webSocketSyncManager;
        String webSocketState = "unavailable";
        boolean webSocketEnabled = false;
        boolean connected = false;
        boolean pluginStatusSyncEnabled = false;
        boolean statusSyncEnabled = false;
        boolean statusHttpEnabled = false;
        boolean playerListExposed = false;
        String endpoint = "-";
        String httpEndpoint = "-";
        int queueSize = 0;
        int pendingAck = 0;
        long lastPongAt = 0L;
        long lastStatusPushAt = 0L;
        String lastStatusResult = "never";
        int reconnectFailures = 0;
        String lastReconnectReason = "none";
        int playersVersion = 0;

        if (manager != null) {
            webSocketEnabled = manager.isEnabled();
            connected = manager.isConnected();
            pluginStatusSyncEnabled = manager.isPluginStatusSyncEnabled();
            statusSyncEnabled = manager.isStatusSyncEnabled();
            statusHttpEnabled = manager.isStatusHttpEnabled();
            playerListExposed = manager.isPlayerListExposed();
            if (!webSocketEnabled) {
                webSocketState = "disabled";
            } else if (connected) {
                webSocketState = "connected";
            } else {
                webSocketState = "disconnected";
            }
            endpoint = manager.getMaskedEndpoint();
            httpEndpoint = manager.getMaskedStatusHttpEndpoint();
            queueSize = manager.getQueueSize();
            pendingAck = manager.getPendingAckSize();
            lastPongAt = manager.getLastPongAt();
            lastStatusPushAt = manager.getLastStatusPushAt();
            lastStatusResult = manager.getLastStatusResult();
            reconnectFailures = manager.getReconnectFailures();
            lastReconnectReason = manager.getLastReconnectReason();
            playersVersion = manager.getLastKnownPlayersVersion();
        }

        int onlinePlayers = Bukkit.getOnlinePlayers().size();
        int maxPlayers = Bukkit.getMaxPlayers();
        boolean endpointConfigured = (webSocketEnabled && endpoint != null && !endpoint.isBlank() && !"-".equals(endpoint))
                || (statusHttpEnabled && httpEndpoint != null && !httpEndpoint.isBlank() && !"-".equals(httpEndpoint));

        sender.sendMessage("[StellarStatsSync]");
        sender.sendMessage("Status sync: " + (statusSyncEnabled ? "enabled" : "disabled"));
        sender.sendMessage("Online: " + onlinePlayers + "/" + maxPlayers);
        sender.sendMessage("Player list exposed: " + playerListExposed);
        sender.sendMessage("Endpoint: " + (endpointConfigured ? "configured" : "not configured"));
        sender.sendMessage("Last push: " + formatDurationAgo(lastStatusPushAt));
        sender.sendMessage("Last result: " + safeValue(lastStatusResult));
        sender.sendMessage("WebSocket: " + webSocketState);
        sender.sendMessage("WebSocket enabled: " + webSocketEnabled);
        sender.sendMessage("Connected: " + connected);
        sender.sendMessage("WebSocket endpoint: " + safeValue(endpoint));
        sender.sendMessage("HTTP status enabled: " + statusHttpEnabled);
        sender.sendMessage("HTTP endpoint: " + safeValue(httpEndpoint));
        sender.sendMessage("Queue size: " + queueSize);
        sender.sendMessage("Pending ACK: " + pendingAck);
        sender.sendMessage("Last pong: " + formatDurationAgo(lastPongAt));
        sender.sendMessage("Reconnect failures: " + reconnectFailures);
        sender.sendMessage("Last reconnect reason: " + (lastReconnectReason == null || lastReconnectReason.isBlank() ? "none" : lastReconnectReason));
        sender.sendMessage("Plugin status sync enabled: " + pluginStatusSyncEnabled);
        sender.sendMessage("Players version: " + playersVersion);
    }

    private void handleDoctor(CommandSender sender) {
        if (!hasAdminPermission(sender)) {
            sender.sendMessage("You do not have permission to run this command.");
            return;
        }

        WebSocketSyncManager manager = this.webSocketSyncManager;
        LiteSignInBridge bridge = this.liteSignInBridge;
        DatabaseManager dbManager = this.databaseManager;

        sender.sendMessage("[StellarStatsSync Doctor]");
        sender.sendMessage("Plugin: " + (plugin.isEnabled() ? "enabled" : "disabled"));
        sender.sendMessage("Debug: " + StellarStatsSync.isDebug());
        sender.sendMessage("Server: " + Bukkit.getName() + " " + Bukkit.getVersion());
        sender.sendMessage("Online players: " + Bukkit.getOnlinePlayers().size() + "/" + Bukkit.getMaxPlayers());

        sender.sendMessage("Database:");
        sender.sendMessage("- configured: " + yesNo(dbManager != null));
        sender.sendMessage("- pool: " + (dbManager != null && dbManager.isPoolAvailable() ? "available" : "unavailable"));
        sender.sendMessage("- ping: pending");
        sender.sendMessage("- latencyMs: unavailable");
        sender.sendMessage("- error: -");

        String webSocketState = "unavailable";
        boolean webSocketEnabled = false;
        boolean connected = false;
        String endpoint = "-";
        boolean statusHttpEnabled = false;
        String httpEndpoint = "-";
        int queueSize = -1;
        int pendingAck = -1;
        long lastPongAt = 0L;
        int reconnectFailures = -1;
        String lastReconnectReason = "-";

        if (manager != null) {
            webSocketEnabled = manager.isEnabled();
            connected = manager.isConnected();
            if (!webSocketEnabled) {
                webSocketState = "disabled";
            } else if (connected) {
                webSocketState = "connected";
            } else {
                webSocketState = "disconnected";
            }
            endpoint = manager.getMaskedEndpoint();
            statusHttpEnabled = manager.isStatusHttpEnabled();
            httpEndpoint = manager.getMaskedStatusHttpEndpoint();
            queueSize = manager.getQueueSize();
            pendingAck = manager.getPendingAckSize();
            lastPongAt = manager.getLastPongAt();
            reconnectFailures = manager.getReconnectFailures();
            lastReconnectReason = manager.getLastReconnectReason();
        }

        sender.sendMessage("WebSocket:");
        sender.sendMessage("- enabled: " + webSocketEnabled);
        sender.sendMessage("- connected: " + connected);
        sender.sendMessage("- endpoint: " + safeValue(endpoint));
        sender.sendMessage("- queueSize: " + formatCount(queueSize));
        sender.sendMessage("- pendingAck: " + formatCount(pendingAck));
        sender.sendMessage("- lastPong: " + formatDurationAgo(lastPongAt));
        sender.sendMessage("- reconnectFailures: " + formatCount(reconnectFailures));
        sender.sendMessage("- lastReconnectReason: " + sanitizeError(safeValue(lastReconnectReason)));
        sender.sendMessage("- state: " + webSocketState);
        sender.sendMessage("HTTP Status:");
        sender.sendMessage("- enabled: " + statusHttpEnabled);
        sender.sendMessage("- endpoint: " + safeValue(httpEndpoint));

        LiteSignInBridge.SignInDoctorSnapshot signInSnapshot =
                bridge == null ? null : bridge.getDoctorSnapshot();
        sender.sendMessage("SignIn:");
        sender.sendMessage("- bridgeEnabled: " + (signInSnapshot != null && signInSnapshot.enabled()));
        sender.sendMessage("- provider: " + (signInSnapshot == null ? "unavailable" : safeValue(signInSnapshot.provider())));
        sender.sendMessage("- liteSignInInstalled: " + (signInSnapshot != null && signInSnapshot.providerAvailable()));
        sender.sendMessage("- liteSignInEnabled: " + (signInSnapshot != null && signInSnapshot.providerEnabled()));
        sender.sendMessage("- eventListening: " + (signInSnapshot != null && signInSnapshot.eventListening()));
        sender.sendMessage("- requirePlayerOnline: " + (signInSnapshot != null && signInSnapshot.requirePlayerOnline()));
        sender.sendMessage("- sendGameUpdates: " + (signInSnapshot != null && signInSnapshot.sendGameSignInUpdates()));
        sender.sendMessage("- requestContextSize: " + (signInSnapshot == null ? "unavailable" : signInSnapshot.requestContextSize()));
        if (signInSnapshot != null && signInSnapshot.disabledReason() != null && !signInSnapshot.disabledReason().isBlank() && !"-".equals(signInSnapshot.disabledReason())) {
            sender.sendMessage("- disabledReason: " + sanitizeError(signInSnapshot.disabledReason()));
        }

        sender.sendMessage("Sync:");
        sender.sendMessage("- intervalTicks: " + plugin.getConfig().getLong("sync_interval_ticks", 12000L));
        sender.sendMessage("- lastManualSync: " + (lastManualSyncAt <= 0L ? "unknown" : formatDurationAgo(lastManualSyncAt)));

        if (dbManager == null) {
            sender.sendMessage("[StellarStatsSync Doctor][Database]");
            sender.sendMessage("- ping: unavailable");
            sender.sendMessage("- latencyMs: unavailable");
            sender.sendMessage("- error: unavailable");
            return;
        }

        dbManager.checkHealthAsync().whenComplete((health, throwable) -> Bukkit.getScheduler().runTask(plugin, () -> {
            sender.sendMessage("[StellarStatsSync Doctor][Database]");

            if (throwable != null) {
                sender.sendMessage("- ping: FAIL");
                sender.sendMessage("- latencyMs: unavailable");
                sender.sendMessage("- error: " + sanitizeError(throwable.getMessage()));
                return;
            }

            if (health == null) {
                sender.sendMessage("- ping: FAIL");
                sender.sendMessage("- latencyMs: unavailable");
                sender.sendMessage("- error: unavailable");
                return;
            }

            sender.sendMessage("- ping: " + (health.ok() ? "OK" : "FAIL"));
            sender.sendMessage("- latencyMs: " + (health.latencyMs() >= 0L ? Long.toString(health.latencyMs()) : "unavailable"));
            sender.sendMessage("- error: " + (health.ok() ? "-" : sanitizeError(health.error())));
        }));
    }

    private void sendUsage(CommandSender sender, String label) {
        sender.sendMessage("Usage: /" + label + " <sync|status|doctor>");
    }

    private boolean hasAdminPermission(CommandSender sender) {
        if (sender instanceof Player) {
            return sender.hasPermission(ADMIN_PERMISSION);
        }
        return true;
    }

    private String formatDurationAgo(long timestampMillis) {
        if (timestampMillis <= 0L) {
            return "never";
        }
        long elapsed = Math.max(0L, System.currentTimeMillis() - timestampMillis);
        return formatDuration(elapsed) + " ago";
    }

    private String formatDuration(long millis) {
        long seconds = Math.max(0L, millis / 1000L);
        if (seconds < 60L) {
            return seconds + "s";
        }
        long minutes = seconds / 60L;
        if (minutes < 60L) {
            return minutes + "m";
        }
        long hours = minutes / 60L;
        return hours + "h";
    }

    private String yesNo(boolean value) {
        return value ? "yes" : "no";
    }

    private String formatCount(int value) {
        return value < 0 ? "unavailable" : Integer.toString(value);
    }

    private String safeValue(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }

    private String sanitizeError(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String sanitized = value.trim();
        sanitized = sanitized.replaceAll("(?i)(password|pwd|token|auth_token)\\s*[=:]\\s*[^\\s,;]+", "$1=***");
        sanitized = sanitized.replaceAll("(?i)(authorization\\s*:\\s*bearer\\s+)[^\\s,;]+", "$1***");
        sanitized = sanitized.replaceAll("(?i)jdbc:mysql://[^\\s,;]+", "jdbc:mysql://***");
        if (sanitized.length() > 160) {
            sanitized = sanitized.substring(0, 160) + "...";
        }
        return sanitized;
    }
}
