package org.stellarvan.stellarstatssync;

import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerQuitEvent;

import java.util.List;

public class PlayerQuitListener implements Listener {

    private final StellarStatsSync plugin;
    private final SyncTask syncTask;
    private final DatabaseManager databaseManager;

    public PlayerQuitListener(StellarStatsSync plugin, SyncTask syncTask, DatabaseManager databaseManager) {
        this.plugin = plugin;
        this.syncTask = syncTask;
        this.databaseManager = databaseManager;
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        if (plugin.isShuttingDown()) {
            return;
        }
        SyncTask.PlayerStatsSnapshot snapshot = syncTask.createSnapshot(event.getPlayer());
        if (snapshot == null) {
            return;
        }
        if (StellarStatsSync.isDebug()) {
            plugin.getLogger().info("[Debug] 玩家退服，异步同步: " + snapshot.username());
        }
        databaseManager.syncPlayerStatsAsync(List.of(snapshot));
    }
}
