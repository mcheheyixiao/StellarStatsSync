package org.stellarvan.stellarstatssync;

import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.jetbrains.annotations.NotNull;

import java.util.Locale;
import java.util.logging.Level;

public class StatsyncCommand implements CommandExecutor {

    private static final String ADMIN_PERMISSION = "stellarstatsync.admin";

    private final StellarStatsSync plugin;
    private final SyncTask syncTask;
    private final WebSocketSyncManager webSocketSyncManager;

    public StatsyncCommand(StellarStatsSync plugin, SyncTask syncTask, WebSocketSyncManager webSocketSyncManager) {
        this.plugin = plugin;
        this.syncTask = syncTask;
        this.webSocketSyncManager = webSocketSyncManager;
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
        String endpoint = "-";
        int queueSize = 0;
        int pendingAck = 0;
        long lastPongAt = 0L;
        int reconnectFailures = 0;
        String lastReconnectReason = "none";
        int playersVersion = 0;

        if (manager != null) {
            if (!manager.isEnabled()) {
                webSocketState = "disabled";
            } else if (manager.isConnected()) {
                webSocketState = "connected";
            } else {
                webSocketState = "disconnected";
            }
            endpoint = manager.getMaskedEndpoint();
            queueSize = manager.getQueueSize();
            pendingAck = manager.getPendingAckSize();
            lastPongAt = manager.getLastPongAt();
            reconnectFailures = manager.getReconnectFailures();
            lastReconnectReason = manager.getLastReconnectReason();
            playersVersion = manager.getLastKnownPlayersVersion();
        }

        sender.sendMessage("[StellarStatsSync]");
        sender.sendMessage("WebSocket: " + webSocketState);
        sender.sendMessage("Endpoint: " + endpoint);
        sender.sendMessage("Queue size: " + queueSize);
        sender.sendMessage("Pending ACK: " + pendingAck);
        sender.sendMessage("Last pong: " + formatDurationAgo(lastPongAt));
        sender.sendMessage("Reconnect failures: " + reconnectFailures);
        sender.sendMessage("Last reconnect reason: " + (lastReconnectReason == null || lastReconnectReason.isBlank() ? "none" : lastReconnectReason));
        sender.sendMessage("Players version: " + playersVersion);
    }

    private void sendUsage(CommandSender sender, String label) {
        sender.sendMessage("Usage: /" + label + " <sync|status>");
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
}
