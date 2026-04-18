package org.stellarvan.stellarstatssync;

import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.plugin.java.JavaPlugin;

public class StellarStatsSync extends JavaPlugin {

    private static volatile boolean DEBUG = false;
    private volatile boolean isShuttingDown = false;

    private DatabaseManager databaseManager;
    private SyncTask syncTask;

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

        if (getCommand("statsync") != null) {
            getCommand("statsync").setExecutor(new StatsyncCommand(this, syncTask));
        } else {
            getLogger().severe("Command 'statsync' is not defined in plugin.yml");
        }

        getLogger().info("StellarStatsSync enabled.");
    }

    @Override
    public void onDisable() {
        this.isShuttingDown = true;
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
                e.printStackTrace();
            }
            return false;
        }
    }
}

