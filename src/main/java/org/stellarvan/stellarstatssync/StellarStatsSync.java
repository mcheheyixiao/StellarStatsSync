package org.stellarvan.stellarstatssync;

import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.command.PluginCommand;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.logging.Level;

public class StellarStatsSync extends JavaPlugin {

    private static volatile boolean DEBUG = false;
    private volatile boolean isShuttingDown = false;

    private DatabaseManager databaseManager;
    private SyncTask syncTask;
    private WebSocketSyncManager webSocketSyncManager;

    public static boolean isDebug() {
        return DEBUG;
    }

    public boolean isShuttingDown() {
        return isShuttingDown;
    }

    @Override
    public void onEnable() {
        saveDefaultConfig();

        DEBUG = getConfig().getBoolean("debug", false);

        if (!initDatabaseManager()) {
            getLogger().severe("Failed to initialize database. Disabling plugin...");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        long interval = getConfig().getLong("sync_interval_ticks", 12000L);

        this.syncTask = new SyncTask(this, databaseManager);
        this.syncTask.runTaskTimer(this, 20L, interval);

        getServer().getPluginManager().registerEvents(new PlayerQuitListener(this, syncTask, databaseManager), this);
        this.webSocketSyncManager = new WebSocketSyncManager(this);

        if (webSocketSyncManager.isEnabled()) {
            webSocketSyncManager.start();
            getServer().getPluginManager().registerEvents(new RealtimeSyncListener(this, webSocketSyncManager), this);
            getLogger().info("WebSocket realtime sync enabled.");
        } else {
            getLogger().info("WebSocket realtime sync disabled by config.");
        }

        PluginCommand statsyncCommand = getCommand("statsync");
        if (statsyncCommand != null) {
            statsyncCommand.setExecutor(new StatsyncCommand(this, syncTask));
        } else {
            getLogger().warning("Command 'statsync' not found in plugin.yml");
        }

        getLogger().info("StellarStatsSync enabled.");
    }

    @Override
    public void onDisable() {
        this.isShuttingDown = true;
        if (webSocketSyncManager != null) {
            webSocketSyncManager.shutdown();
        }
        if (syncTask != null) {
            syncTask.performSyncSyncOnDisable();
        }
        if (databaseManager != null) {
            databaseManager.shutdown();
        }
        getLogger().info("StellarStatsSync disabled.");
    }

    private boolean initDatabaseManager() {
        ConfigurationSection mysql = getConfig().getConfigurationSection("mysql");
        if (mysql == null) {
            getLogger().severe("Missing 'mysql' section in config.yml");
            return false;
        }

        String host = mysql.getString("host", "localhost");
        int port = mysql.getInt("port", 3306);
        String database = mysql.getString("database", "minecraft");
        String user = mysql.getString("user", "root");
        String password = mysql.getString("password", "");
        int maximumPoolSize = mysql.getInt("maximumPoolSize", 10);

        try {
            this.databaseManager = new DatabaseManager(
                    this,
                    host,
                    port,
                    database,
                    user,
                    password,
                    maximumPoolSize
            );
            return true;
        } catch (Exception e) {
            getLogger().severe("Error initializing DatabaseManager: " + e.getMessage());
            if (isDebug()) {
                getLogger().log(Level.SEVERE, "[Debug] DatabaseManager initialization failure", e);
            }
            return false;
        }
    }
}
