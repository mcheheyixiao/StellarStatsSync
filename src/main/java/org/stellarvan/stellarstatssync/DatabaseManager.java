package org.stellarvan.stellarstatssync;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class DatabaseManager {

    private final Plugin plugin;
    private final HikariDataSource dataSource;

    public DatabaseManager(Plugin plugin,
                           String host,
                           int port,
                           String database,
                           String user,
                           String password,
                           int maximumPoolSize) {

        this.plugin = plugin;

        HikariConfig config = new HikariConfig();
        String jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + database
                + "?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC";

        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);

        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(Math.max(1, maximumPoolSize / 4));
        config.setPoolName("StellarStatsSync-HikariPool");

        config.setConnectionTimeout(10000L);
        config.setIdleTimeout(600000L);
        config.setMaxLifetime(1800000L);
        config.setInitializationFailTimeout(-1);

        this.dataSource = new HikariDataSource(config);
    }

    public void shutdown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    public boolean isPoolAvailable() {
        return dataSource != null && !dataSource.isClosed();
    }

    public Connection openConnection() throws SQLException {
        if (!isPoolAvailable()) {
            throw new SQLException("Database connection pool is unavailable.");
        }
        return dataSource.getConnection();
    }

    public CompletableFuture<DatabaseHealth> checkHealthAsync() {
        if (!plugin.isEnabled()) {
            return CompletableFuture.completedFuture(new DatabaseHealth(false, -1L, "plugin disabled"));
        }
        if (plugin instanceof StellarStatsSync stellarStatsSync && stellarStatsSync.isShuttingDown()) {
            return CompletableFuture.completedFuture(new DatabaseHealth(false, -1L, "plugin shutting down"));
        }
        if (!isPoolAvailable()) {
            return CompletableFuture.completedFuture(new DatabaseHealth(false, -1L, "pool unavailable"));
        }

        CompletableFuture<DatabaseHealth> future = new CompletableFuture<>();
        try {
            Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                long startedAtNanos = System.nanoTime();
                try (Connection connection = openConnection();
                     PreparedStatement stmt = connection.prepareStatement("SELECT 1");
                     ResultSet rs = stmt.executeQuery()) {
                    boolean ok = rs.next();
                    long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
                    future.complete(new DatabaseHealth(ok, latencyMs, ok ? "-" : "empty result"));
                } catch (Exception ex) {
                    long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
                    future.complete(new DatabaseHealth(false, latencyMs, sanitizeError(ex.getMessage())));
                }
            });
        } catch (Exception ex) {
            future.complete(new DatabaseHealth(false, -1L, sanitizeError(ex.getMessage())));
        }
        return future;
    }

    @SuppressWarnings("unused")
    public void syncPlayerStatsAsync(SyncTask.PlayerStatsSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        syncPlayerStatsAsync(List.of(snapshot));
    }

    @SuppressWarnings("unused")
    public void syncPlayerStatsAsync(Collection<SyncTask.PlayerStatsSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return;
        }

        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                syncPlayerStatsInternal(snapshots);
            } catch (SQLException e) {
                plugin.getLogger().severe("Failed to sync player stats: " + e.getMessage());
                if (StellarStatsSync.isDebug()) {
                    plugin.getLogger().log(Level.SEVERE, "[Debug] Async sync failed", e);
                }
            }
        });
    }

    public CompletableFuture<DbSyncResult> syncPlayerStatsAsyncWithResult(Collection<SyncTask.PlayerStatsSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return CompletableFuture.completedFuture(new DbSyncResult(0, 0, 0));
        }

        CompletableFuture<DbSyncResult> future = new CompletableFuture<>();
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
            try {
                DbSyncResult result = syncPlayerStatsInternal(snapshots);
                future.complete(result);
            } catch (SQLException e) {
                plugin.getLogger().severe("Failed to sync player stats: " + e.getMessage());
                if (StellarStatsSync.isDebug()) {
                    plugin.getLogger().log(Level.SEVERE, "[Debug] Async sync with result failed", e);
                }
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    @SuppressWarnings("unused")
    public DbSyncResult syncPlayerStatsInternal(SyncTask.PlayerStatsSnapshot snapshot) throws SQLException {
        if (snapshot == null) {
            return new DbSyncResult(0, 0, 0);
        }
        return syncPlayerStatsInternal(List.of(snapshot));
    }

    @SuppressWarnings({"unused", "SqlNoDataSourceInspection"})
    public DbSyncResult syncPlayerStatsInternal(Collection<SyncTask.PlayerStatsSnapshot> snapshots) throws SQLException {
        //noinspection SqlNoDataSourceInspection
        String sql =
                "INSERT INTO player_stats " +
                        "  (mc_uuid, username, play_time_ticks, fly_distance_cm, deaths, fish_caught, player_kills, blocks_mined, blocks_placed) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "  username = VALUES(username), " +
                        "  play_time_ticks = VALUES(play_time_ticks), " +
                        "  fly_distance_cm = VALUES(fly_distance_cm), " +
                        "  deaths = VALUES(deaths), " +
                        "  fish_caught = VALUES(fish_caught), " +
                        "  player_kills = VALUES(player_kills), " +
                        "  blocks_mined = VALUES(blocks_mined), " +
                        "  blocks_placed = VALUES(blocks_placed)";

        try (Connection connection = openConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {

            for (SyncTask.PlayerStatsSnapshot snapshot : snapshots) {
                if (!isUserRegistered(connection, snapshot.mcUuid())) {
                    if (StellarStatsSync.isDebug()) {
                        plugin.getLogger().info("[Debug] 跳过未在网站注册的玩家: " + snapshot.username());
                    }
                    continue;
                }

                stmt.setString(1, snapshot.mcUuid());
                stmt.setString(2, snapshot.username());
                stmt.setLong(3, snapshot.playTime());
                stmt.setLong(4, snapshot.flyDistance());
                stmt.setInt(5, snapshot.deaths());
                stmt.setInt(6, snapshot.fishCaught());
                stmt.setInt(7, snapshot.playerKills());
                stmt.setLong(8, snapshot.blocksMined());
                stmt.setLong(9, snapshot.blocksPlaced());

                stmt.addBatch();
            }

            int[] results = stmt.executeBatch();
            int matchedUsers = 0;
            long totalAffected = 0;

            for (int r : results) {
                if (r == PreparedStatement.SUCCESS_NO_INFO) {
                    matchedUsers++;
                    totalAffected += 1;
                } else if (r > 0) {
                    matchedUsers++;
                    totalAffected += r;
                }
            }

            return new DbSyncResult(results.length, matchedUsers, totalAffected);
        }
    }

    @SuppressWarnings("SqlNoDataSourceInspection")
    private boolean isUserRegistered(Connection conn, String uuid) throws SQLException {
        //noinspection SqlNoDataSourceInspection
        String sql = "SELECT 1 FROM users WHERE mc_uuid = ? LIMIT 1";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, uuid);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static String sanitizeError(String value) {
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

    public record DbSyncResult(int batchSize, int matchedUsers, long totalAffectedRows) {
    }

    public record DatabaseHealth(boolean ok, long latencyMs, String error) {
    }
}
