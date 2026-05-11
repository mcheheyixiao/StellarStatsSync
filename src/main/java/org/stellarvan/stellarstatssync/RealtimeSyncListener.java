package org.stellarvan.stellarstatssync;

import io.papermc.paper.event.player.AsyncChatEvent;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;

public class RealtimeSyncListener implements Listener {

    private static final PlainTextComponentSerializer PLAIN_TEXT_SERIALIZER = PlainTextComponentSerializer.plainText();

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
        String playerUuid = player.getUniqueId().toString();
        String playerName = player.getName();
        // Let Bukkit finish removing the player before rebuilding the online snapshot.
        Bukkit.getScheduler().runTask(plugin, () -> {
            if (plugin.isShuttingDown()) {
                return;
            }
            webSocketSyncManager.sendPlayerQuit(playerUuid, playerName);
        });
    }

    @EventHandler(ignoreCancelled = true)
    public void onAsyncChat(AsyncChatEvent event) {
        if (plugin.isShuttingDown() || !webSocketSyncManager.isSyncChatEnabled()) {
            return;
        }

        String playerUuid = event.getPlayer().getUniqueId().toString();
        String playerName = event.getPlayer().getName();
        String message = PLAIN_TEXT_SERIALIZER.serialize(event.message());

        if (event.isAsynchronous()) {
            Bukkit.getScheduler().runTask(plugin, () -> webSocketSyncManager.sendChatMessage(playerUuid, playerName, message));
            return;
        }

        webSocketSyncManager.sendChatMessage(playerUuid, playerName, message);
    }
}
