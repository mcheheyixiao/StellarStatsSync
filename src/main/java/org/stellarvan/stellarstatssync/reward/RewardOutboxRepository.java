package org.stellarvan.stellarstatssync.reward;

import org.bukkit.Bukkit;
import org.stellarvan.stellarstatssync.DatabaseManager;
import org.stellarvan.stellarstatssync.StellarStatsSync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class RewardOutboxRepository {

    private final StellarStatsSync plugin;
    private final DatabaseManager databaseManager;

    public RewardOutboxRepository(StellarStatsSync plugin, DatabaseManager databaseManager) {
        this.plugin = plugin;
        this.databaseManager = databaseManager;
    }

    public List<RewardOutboxEntry> fetchPendingBatch(String serverId, int maxAttempts, int limit) throws SQLException {
        String sql = """
                SELECT id, request_id, website_user_id, player_uuid, player_name, server_id, source, reward_type, reward_payload_json, attempts
                FROM stellar_reward_outbox
                WHERE status = 'pending'
                  AND server_id = ?
                  AND attempts < ?
                ORDER BY created_at ASC
                LIMIT ?
                """;

        List<RewardOutboxEntry> entries = new ArrayList<>();
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, serverId);
            statement.setInt(2, maxAttempts);
            statement.setInt(3, limit);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    entries.add(new RewardOutboxEntry(
                            resultSet.getLong("id"),
                            resultSet.getString("request_id"),
                            resultSet.getString("website_user_id"),
                            resultSet.getString("player_uuid"),
                            resultSet.getString("player_name"),
                            resultSet.getString("server_id"),
                            resultSet.getString("source"),
                            resultSet.getString("reward_type"),
                            resultSet.getString("reward_payload_json"),
                            resultSet.getInt("attempts")
                    ));
                }
            }
        }
        return entries;
    }

    public boolean claimPending(long id) throws SQLException {
        String sql = """
                UPDATE stellar_reward_outbox
                SET status = 'processing',
                    attempts = attempts + 1,
                    processing_at = NOW(),
                    updated_at = NOW()
                WHERE id = ?
                  AND status = 'pending'
                """;
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, id);
            return statement.executeUpdate() == 1;
        }
    }

    public void markDelivered(long id) throws SQLException {
        String sql = """
                UPDATE stellar_reward_outbox
                SET status = 'delivered',
                    last_error = NULL,
                    delivered_at = NOW(),
                    updated_at = NOW()
                WHERE id = ?
                """;
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, id);
            statement.executeUpdate();
        }
    }

    public void markPending(long id, String lastError) throws SQLException {
        String sql = """
                UPDATE stellar_reward_outbox
                SET status = 'pending',
                    last_error = ?,
                    updated_at = NOW()
                WHERE id = ?
                """;
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, truncate(lastError));
            statement.setLong(2, id);
            statement.executeUpdate();
        }
    }

    public void markFailed(long id, String lastError) throws SQLException {
        String sql = """
                UPDATE stellar_reward_outbox
                SET status = 'failed',
                    last_error = ?,
                    updated_at = NOW()
                WHERE id = ?
                """;
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, truncate(lastError));
            statement.setLong(2, id);
            statement.executeUpdate();
        }
    }

    public int recoverStaleProcessingTasks(String serverId, int timeoutSeconds) throws SQLException {
        String sql = """
                UPDATE stellar_reward_outbox
                SET status = 'pending',
                    last_error = 'Recovered stale processing task',
                    updated_at = NOW()
                WHERE status = 'processing'
                  AND server_id = ?
                  AND processing_at < DATE_SUB(NOW(), INTERVAL ? SECOND)
                """;
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, serverId);
            statement.setInt(2, timeoutSeconds);
            return statement.executeUpdate();
        }
    }

    public RewardOutboxCounts fetchCounts(String serverId) throws SQLException {
        String sql = """
                SELECT
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
                    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing_count,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                    SUM(CASE WHEN status = 'delivered' AND delivered_at >= CURDATE() THEN 1 ELSE 0 END) AS delivered_today_count
                FROM stellar_reward_outbox
                WHERE server_id = ?
                """;
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, serverId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return RewardOutboxCounts.unavailable();
                }
                return new RewardOutboxCounts(
                        resultSet.getInt("pending_count"),
                        resultSet.getInt("processing_count"),
                        resultSet.getInt("failed_count"),
                        resultSet.getInt("delivered_today_count")
                );
            }
        }
    }

    public CompletableFuture<RewardOutboxCounts> fetchCountsAsync(String serverId) {
        CompletableFuture<RewardOutboxCounts> future = new CompletableFuture<>();
        try {
            Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    future.complete(fetchCounts(serverId));
                } catch (Exception ex) {
                    future.completeExceptionally(ex);
                }
            });
        } catch (Exception ex) {
            future.completeExceptionally(ex);
        }
        return future;
    }

    private static String truncate(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String trimmed = value.trim();
        return trimmed.length() > 500 ? trimmed.substring(0, 500) : trimmed;
    }

    public record RewardOutboxCounts(
            int pending,
            int processing,
            int failed,
            int deliveredToday
    ) {
        public static RewardOutboxCounts unavailable() {
            return new RewardOutboxCounts(-1, -1, -1, -1);
        }
    }
}
