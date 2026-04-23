package org.stellarvan.stellarstatssync;

import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.Statistic;
import org.bukkit.entity.Player;
import org.bukkit.scheduler.BukkitRunnable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;

public class SyncTask extends BukkitRunnable {

    private final StellarStatsSync plugin;
    private final DatabaseManager databaseManager;

    public SyncTask(StellarStatsSync plugin, DatabaseManager databaseManager) {
        this.plugin = plugin;
        this.databaseManager = databaseManager;
    }

    @Override
    public void run() {
        performSync();
    }

    public CompletableFuture<SyncResult> performSync() {
        long startedAt = System.currentTimeMillis();

        if (StellarStatsSync.isDebug()) {
            plugin.getLogger().info("[Debug] 开始同步玩家数据");
        }

        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();
        if (onlinePlayers.isEmpty()) {
            if (StellarStatsSync.isDebug()) {
                plugin.getLogger().info("[Debug] 当前无在线玩家，跳过同步");
            }
            return CompletableFuture.completedFuture(new SyncResult(0, 0, 0, 0, System.currentTimeMillis() - startedAt));
        }

        if (StellarStatsSync.isDebug()) {
            plugin.getLogger().info("[Debug] 在线玩家数量: " + onlinePlayers.size());
        }

        List<PlayerStatsSnapshot> snapshots = new ArrayList<>(onlinePlayers.size());

        for (Player player : onlinePlayers) {
            if (player == null || !player.isOnline()) {
                continue;
            }

            PlayerStatsSnapshot snapshot = createSnapshot(player);
            if (snapshot != null) {
                snapshots.add(snapshot);
            }
        }

        if (StellarStatsSync.isDebug()) {
            plugin.getLogger().info("[Debug] 采集统计完成，准备写入数据库。快照数量: " + snapshots.size());
        }

        if (snapshots.isEmpty()) {
            return CompletableFuture.completedFuture(new SyncResult(onlinePlayers.size(), 0, 0, 0, System.currentTimeMillis() - startedAt));
        }

        return databaseManager.syncPlayerStatsAsyncWithResult(snapshots)
                .thenApply(db -> {
                    if (StellarStatsSync.isDebug()) {
                        plugin.getLogger().info("[Debug] 匹配到注册玩家数量: " + db.matchedUsers() + " / " + db.batchSize());
                        plugin.getLogger().info("[Debug] SQL 执行结果: batchSize=" + db.batchSize()
                                + ", matchedUsers=" + db.matchedUsers()
                                + ", totalAffectedRows=" + db.totalAffectedRows());
                    }
                    return new SyncResult(
                            onlinePlayers.size(),
                            snapshots.size(),
                            db.batchSize(),
                            db.matchedUsers(),
                            System.currentTimeMillis() - startedAt
                    );
                });
    }

    public PlayerStatsSnapshot createSnapshot(Player player) {
        if (player == null) {
            return null;
        }

        UUID uuid = player.getUniqueId();
        String mcUuid = uuid.toString();
        String username = player.getName();

        long playTime = safeGetStatistic(player, Statistic.PLAY_ONE_MINUTE);
        long flyDistance = safeGetStatistic(player, Statistic.FLY_ONE_CM);
        int deaths = safeGetStatisticInt(player, Statistic.DEATHS);
        int fishCaught = safeGetStatisticInt(player, Statistic.FISH_CAUGHT);
        int playerKills = safeGetStatisticInt(player, Statistic.PLAYER_KILLS);

        long blocksMined = calculateBlocksMined(player);
        long blocksPlaced = calculateBlocksPlaced(player);

        return new PlayerStatsSnapshot(
                mcUuid,
                username,
                playTime,
                flyDistance,
                deaths,
                fishCaught,
                playerKills,
                blocksMined,
                blocksPlaced
        );
    }

    public void performSyncSyncOnDisable() {
        Collection<? extends Player> onlinePlayers = Bukkit.getOnlinePlayers();
        List<PlayerStatsSnapshot> snapshots = new ArrayList<>(onlinePlayers.size());

        for (Player player : onlinePlayers) {
            if (player == null || !player.isOnline()) {
                continue;
            }
            PlayerStatsSnapshot snapshot = createSnapshot(player);
            if (snapshot != null) {
                snapshots.add(snapshot);
            }
        }

        if (snapshots.isEmpty()) {
            return;
        }

        try {
            databaseManager.syncPlayerStatsInternal(snapshots);
        } catch (SQLException e) {
            plugin.getLogger().severe("Failed to sync player stats on disable: " + e.getMessage());
            if (StellarStatsSync.isDebug()) {
                plugin.getLogger().log(Level.SEVERE, "[Debug] Disable sync failed", e);
            }
        }
    }

    private long safeGetStatistic(Player player, Statistic statistic) {
        try {
            return player.getStatistic(statistic);
        } catch (IllegalArgumentException ex) {
            plugin.getLogger().warning("Failed to get statistic " + statistic + " for " + player.getName());
            return 0L;
        }
    }

    private int safeGetStatisticInt(Player player, Statistic statistic) {
        try {
            return player.getStatistic(statistic);
        } catch (IllegalArgumentException ex) {
            plugin.getLogger().warning("Failed to get statistic " + statistic + " for " + player.getName());
            return 0;
        }
    }

    private long calculateBlocksMined(Player player) {
        long total = 0L;
        for (Material material : Material.values()) {
            if (!material.isBlock()) {
                continue;
            }
            try {
                int value = player.getStatistic(Statistic.MINE_BLOCK, material);
                if (value > 0) {
                    total += value;
                }
            } catch (IllegalArgumentException ignored) {
            }
        }
        return total;
    }

    private long calculateBlocksPlaced(Player player) {
        long total = 0L;
        for (Material material : Material.values()) {
            if (!material.isBlock()) {
                continue;
            }
            try {
                int value = player.getStatistic(Statistic.USE_ITEM, material);
                if (value > 0) {
                    total += value;
                }
            } catch (IllegalArgumentException ignored) {
            }
        }
        return total;
    }

    public record PlayerStatsSnapshot(
            String mcUuid,
            String username,
            long playTime,
            long flyDistance,
            int deaths,
            int fishCaught,
            int playerKills,
            long blocksMined,
            long blocksPlaced
    ) {
    }

    public record SyncResult(
            int onlinePlayers,
            int snapshots,
            int batchSize,
            int matchedUsers,
            long durationMs
    ) {
    }
}
