package org.stellarvan.stellarstatssync;

import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.server.PluginDisableEvent;
import org.bukkit.event.server.PluginEnableEvent;

public class PluginStatusListener implements Listener {

    private final StellarStatsSync plugin;
    private final WebSocketSyncManager webSocketSyncManager;

    public PluginStatusListener(StellarStatsSync plugin, WebSocketSyncManager webSocketSyncManager) {
        this.plugin = plugin;
        this.webSocketSyncManager = webSocketSyncManager;
    }

    @EventHandler
    public void onPluginEnable(PluginEnableEvent event) {
        if (plugin.isShuttingDown() || !webSocketSyncManager.isPluginStatusSyncEnabled()) {
            return;
        }
        webSocketSyncManager.schedulePluginsUpdateDebounced();
    }

    @EventHandler
    public void onPluginDisable(PluginDisableEvent event) {
        if (plugin.isShuttingDown() || event.getPlugin() == plugin || !webSocketSyncManager.isPluginStatusSyncEnabled()) {
            return;
        }
        webSocketSyncManager.schedulePluginsUpdateDebounced();
    }
}
