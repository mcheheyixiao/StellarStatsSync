package org.stellarvan.stellarstatssync;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerChatEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

public class RealtimeSyncListener implements Listener {

    private final StellarStatsSync plugin;
    private final WebSocketSyncManager webSocketSyncManager;

    public RealtimeSyncListener(StellarStatsSync plugin, WebSocketSyncManager webSocketSyncManager) {
        this.plugin = plugin;
        this.webSocketSyncManager = webSocketSyncManager;
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent event) {
        if (plugin.isShuttingDown()) {
            return;
        }

        Player player = event.getPlayer();
        webSocketSyncManager.sendPlayerJoin(player.getUniqueId().toString(), player.getName());
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        if (plugin.isShuttingDown()) {
            return;
        }

        Player player = event.getPlayer();
        webSocketSyncManager.sendPlayerQuit(player.getUniqueId().toString(), player.getName());
    }

    @SuppressWarnings("deprecation")
    @EventHandler(ignoreCancelled = true)
    public void onAsyncPlayerChat(AsyncPlayerChatEvent event) {
        if (plugin.isShuttingDown() || !webSocketSyncManager.isSyncChatEnabled()) {
            return;
        }

        String playerUuid = event.getPlayer().getUniqueId().toString();
        String playerName = event.getPlayer().getName();
        String message = event.getMessage();

        if (event.isAsynchronous()) {
            Bukkit.getScheduler().runTask(plugin, () -> webSocketSyncManager.sendChatMessage(playerUuid, playerName, message));
            return;
        }

        webSocketSyncManager.sendChatMessage(playerUuid, playerName, message);
    }
}
