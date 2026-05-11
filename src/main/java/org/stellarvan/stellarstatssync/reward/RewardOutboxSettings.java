package org.stellarvan.stellarstatssync.reward;

import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;

public record RewardOutboxSettings(
        boolean enabled,
        int pollIntervalSeconds,
        int batchSize,
        int maxAttempts,
        int processingTimeoutSeconds,
        String serverId,
        SweetMailSettings sweetMail,
        CommandSettings commands,
        boolean allowNotificationOnlyMail,
        boolean debug
) {
    public static RewardOutboxSettings fromConfig(FileConfiguration config) {
        ConfigurationSection root = config.getConfigurationSection("reward-outbox");
        boolean enabled = root == null || root.getBoolean("enabled", true);
        int pollIntervalSeconds = Math.max(1, root == null ? 15 : root.getInt("poll-interval-seconds", 15));
        int batchSize = Math.max(1, root == null ? 20 : root.getInt("batch-size", 20));
        int maxAttempts = Math.max(1, root == null ? 5 : root.getInt("max-attempts", 5));
        int processingTimeoutSeconds = Math.max(30, root == null ? 120 : root.getInt("processing-timeout-seconds", 120));
        String serverId = normalizeString(root == null ? "stellar-main" : root.getString("server-id", "stellar-main"), "stellar-main");

        ConfigurationSection sweetMail = root == null ? null : root.getConfigurationSection("sweetmail");
        SweetMailSettings sweetMailSettings = new SweetMailSettings(
                sweetMail == null || sweetMail.getBoolean("enabled", true),
                sweetMail == null || sweetMail.getBoolean("require-plugin", true),
                normalizeString(sweetMail == null ? "繁星World" : sweetMail.getString("system-sender", "繁星World"), "繁星World"),
                normalizeString(sweetMail == null ? "BOOK" : sweetMail.getString("default-icon", "BOOK"), "BOOK")
        );

        ConfigurationSection commands = root == null ? null : root.getConfigurationSection("commands");
        CommandSettings commandSettings = new CommandSettings(
                commands == null || commands.getBoolean("enabled", true),
                commands == null || commands.getBoolean("run-on-main-thread", true)
        );

        boolean allowNotificationOnlyMail = root != null && root.getBoolean("allow-notification-only-mail", false);
        boolean debug = root != null && root.getBoolean("debug", false);
        return new RewardOutboxSettings(
                enabled,
                pollIntervalSeconds,
                batchSize,
                maxAttempts,
                processingTimeoutSeconds,
                serverId,
                sweetMailSettings,
                commandSettings,
                allowNotificationOnlyMail,
                debug
        );
    }

    private static String normalizeString(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value.trim();
    }

    public record SweetMailSettings(
            boolean enabled,
            boolean requirePlugin,
            String systemSender,
            String defaultIcon
    ) {
    }

    public record CommandSettings(
            boolean enabled,
            boolean runOnMainThread
    ) {
    }
}
