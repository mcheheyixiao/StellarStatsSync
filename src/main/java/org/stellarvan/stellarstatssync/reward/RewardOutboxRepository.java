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

    private static final String TABLE_NAME = "stellar_reward_outbox";
    private static final String INDEX_STATUS_CREATED = "idx_stellar_reward_outbox_status_created";
    private static final String INDEX_PLAYER_STATUS = "idx_stellar_reward_outbox_player_status";
    private static final String INDEX_USER_CREATED = "idx_stellar_reward_outbox_user_created";
    private static final String INDEX_DAILY_UNIQUE = "uq_stellar_reward_outbox_daily";

    private final StellarStatsSync plugin;
    private final DatabaseManager databaseManager;

    public RewardOutboxRepository(StellarStatsSync plugin, DatabaseManager databaseManager) {
        this.plugin = plugin;
        this.databaseManager = databaseManager;
    }

    public void ensureSchema() throws SQLException {
        try (Connection connection = databaseManager.openConnection()) {
            String schema = resolveSchemaName(connection);
            if (schema == null || schema.isBlank()) {
                throw new SQLException("Unable to resolve current database schema for reward outbox.");
            }

            if (!tableExists(connection, schema, TABLE_NAME)) {
                createTable(connection);
            }

            ensureColumn(connection, schema, "sign_date", "ALTER TABLE `" + TABLE_NAME + "` ADD COLUMN `sign_date` DATE NULL DEFAULT NULL");
            ensureColumn(connection, schema, "processing_at", "ALTER TABLE `" + TABLE_NAME + "` ADD COLUMN `processing_at` DATETIME NULL DEFAULT NULL");
            ensureColumn(connection, schema, "delivered_at", "ALTER TABLE `" + TABLE_NAME + "` ADD COLUMN `delivered_at` DATETIME NULL DEFAULT NULL");
            ensureColumn(connection, schema, "attempts", "ALTER TABLE `" + TABLE_NAME + "` ADD COLUMN `attempts` INT UNSIGNED NOT NULL DEFAULT 0");
            ensureColumn(connection, schema, "last_error", "ALTER TABLE `" + TABLE_NAME + "` ADD COLUMN `last_error` VARCHAR(500) NULL DEFAULT NULL");

            ensureIndex(connection, schema, INDEX_STATUS_CREATED,
                    "CREATE INDEX `" + INDEX_STATUS_CREATED + "` ON `" + TABLE_NAME + "` (`status`, `created_at`)");
            ensureIndex(connection, schema, INDEX_PLAYER_STATUS,
                    "CREATE INDEX `" + INDEX_PLAYER_STATUS + "` ON `" + TABLE_NAME + "` (`player_uuid`, `status`)");
            ensureIndex(connection, schema, INDEX_USER_CREATED,
                    "CREATE INDEX `" + INDEX_USER_CREATED + "` ON `" + TABLE_NAME + "` (`website_user_id`, `created_at`)");
            ensureIndex(connection, schema, INDEX_DAILY_UNIQUE,
                    "CREATE UNIQUE INDEX `" + INDEX_DAILY_UNIQUE + "` ON `" + TABLE_NAME + "` (`player_uuid`, `server_id`, `source`, `sign_date`)");
        }
    }

    public List<RewardOutboxEntry> fetchPendingBatch(String serverId, int maxAttempts, int limit) throws SQLException {
        String sql = """
                SELECT id, request_id, website_user_id, player_uuid, player_name, server_id, source, reward_type, reward_payload_json, attempts, sign_date
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
                            resultSet.getInt("attempts"),
                            resultSet.getString("sign_date")
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

    public boolean hasDeliveredRewardForDay(RewardOutboxEntry entry) throws SQLException {
        if (entry == null || entry.playerUuid() == null || entry.playerUuid().isBlank()
                || entry.serverId() == null || entry.serverId().isBlank()
                || entry.source() == null || entry.source().isBlank()
                || entry.signDate() == null || entry.signDate().isBlank()) {
            return false;
        }

        String sql = """
                SELECT 1
                FROM stellar_reward_outbox
                WHERE id <> ?
                  AND status = 'delivered'
                  AND player_uuid = ?
                  AND server_id = ?
                  AND source = ?
                  AND sign_date = ?
                LIMIT 1
                """;
        try (Connection connection = databaseManager.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, entry.id());
            statement.setString(2, entry.playerUuid());
            statement.setString(3, entry.serverId());
            statement.setString(4, entry.source());
            statement.setString(5, entry.signDate());
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
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

    private void createTable(Connection connection) throws SQLException {
        String sql = """
                CREATE TABLE IF NOT EXISTS `stellar_reward_outbox` (
                    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    `request_id` VARCHAR(100) NULL DEFAULT NULL,
                    `website_user_id` VARCHAR(64) NULL DEFAULT NULL,
                    `player_uuid` CHAR(36) NOT NULL,
                    `player_name` VARCHAR(32) NOT NULL,
                    `server_id` VARCHAR(64) NOT NULL,
                    `source` VARCHAR(32) NOT NULL DEFAULT 'web',
                    `reward_type` VARCHAR(64) NULL DEFAULT NULL,
                    `reward_payload_json` LONGTEXT NOT NULL,
                    `status` VARCHAR(16) NOT NULL DEFAULT 'pending',
                    `attempts` INT UNSIGNED NOT NULL DEFAULT 0,
                    `last_error` VARCHAR(500) NULL DEFAULT NULL,
                    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    `processing_at` DATETIME NULL DEFAULT NULL,
                    `delivered_at` DATETIME NULL DEFAULT NULL,
                    `sign_date` DATE NULL DEFAULT NULL,
                    PRIMARY KEY (`id`),
                    KEY `idx_stellar_reward_outbox_status_created` (`status`, `created_at`),
                    KEY `idx_stellar_reward_outbox_player_status` (`player_uuid`, `status`),
                    KEY `idx_stellar_reward_outbox_user_created` (`website_user_id`, `created_at`),
                    UNIQUE KEY `uq_stellar_reward_outbox_daily` (`player_uuid`, `server_id`, `source`, `sign_date`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    private void ensureColumn(Connection connection, String schema, String columnName, String ddl) throws SQLException {
        if (columnExists(connection, schema, TABLE_NAME, columnName)) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            statement.execute();
        }
    }

    private void ensureIndex(Connection connection, String schema, String indexName, String ddl) throws SQLException {
        if (indexExists(connection, schema, TABLE_NAME, indexName)) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            statement.execute();
        }
    }

    private boolean tableExists(Connection connection, String schema, String tableName) throws SQLException {
        String sql = """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = ?
                  AND table_name = ?
                LIMIT 1
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, schema);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private boolean columnExists(Connection connection, String schema, String tableName, String columnName) throws SQLException {
        String sql = """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = ?
                  AND table_name = ?
                  AND column_name = ?
                LIMIT 1
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, schema);
            statement.setString(2, tableName);
            statement.setString(3, columnName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private boolean indexExists(Connection connection, String schema, String tableName, String indexName) throws SQLException {
        String sql = """
                SELECT 1
                FROM information_schema.statistics
                WHERE table_schema = ?
                  AND table_name = ?
                  AND index_name = ?
                LIMIT 1
                """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, schema);
            statement.setString(2, tableName);
            statement.setString(3, indexName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private String resolveSchemaName(Connection connection) throws SQLException {
        String schema = connection.getCatalog();
        if (schema != null && !schema.isBlank()) {
            return schema.trim();
        }

        try (PreparedStatement statement = connection.prepareStatement("SELECT DATABASE()");
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                String resolved = resultSet.getString(1);
                return resolved == null ? "" : resolved.trim();
            }
        }
        return "";
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
