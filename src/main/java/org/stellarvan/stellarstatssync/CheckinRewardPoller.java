package org.stellarvan.stellarstatssync;

import org.bukkit.Bukkit;
import org.bukkit.command.ConsoleCommandSender;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.scheduler.BukkitTask;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

final class CheckinRewardPoller {

    private static final long TICKS_PER_SECOND = 20L;
    private static final int MIN_POLL_INTERVAL_SECONDS = 5;
    private static final int MAX_BATCH_SIZE = 50;
    private static final int MAX_COMMANDS = 20;
    private static final int MAX_COMMAND_LENGTH = 512;
    private static final long FETCH_WARN_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(60L);
    private static final long RECENT_ACK_TTL_MILLIS = TimeUnit.MINUTES.toMillis(10L);
    private static final Pattern USERNAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]{1,16}$");

    private final StellarStatsSync plugin;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean pollRunning = new AtomicBoolean(false);
    private final Set<Long> processingIds = ConcurrentHashMap.newKeySet();
    private final Map<Long, CachedAck> recentAckCache = new ConcurrentHashMap<>();
    private final boolean enabled;
    private final boolean debug;
    private final boolean retryFailed;
    private final int batchSize;
    private final long pollIntervalTicks;
    private final CheckinDeliveryClient client;
    private final String maskedApiBase;
    private final String disabledReason;

    private volatile BukkitTask pollTask;
    private volatile long lastFetchWarnAt = 0L;
    private volatile long lastPollAt = 0L;
    private volatile String lastError = "";

    CheckinRewardPoller(StellarStatsSync plugin) {
        this.plugin = plugin;

        ConfigurationSection checkin = plugin.getConfig().getConfigurationSection("checkin");
        if (checkin == null) {
            this.enabled = false;
            this.debug = false;
            this.retryFailed = true;
            this.batchSize = 20;
            this.pollIntervalTicks = 15L * TICKS_PER_SECOND;
            this.client = null;
            this.maskedApiBase = "-";
            this.disabledReason = "missing checkin config section";
            return;
        }

        this.debug = checkin.getBoolean("debug", false);
        if (!checkin.getBoolean("enabled", true)) {
            this.enabled = false;
            this.retryFailed = checkin.getBoolean("retry-failed", true);
            this.batchSize = clampBatchSize(checkin.getInt("batch-size", 20));
            this.pollIntervalTicks = clampPollIntervalSeconds(checkin.getInt("poll-interval-seconds", 15)) * TICKS_PER_SECOND;
            this.client = null;
            this.maskedApiBase = maskForDoctor(resolveBaseUrl(plugin.getConfig(), checkin));
            this.disabledReason = "disabled by config";
            return;
        }

        this.retryFailed = checkin.getBoolean("retry-failed", true);
        int configuredPollSeconds = checkin.getInt("poll-interval-seconds", 15);
        int configuredBatchSize = checkin.getInt("batch-size", 20);
        this.pollIntervalTicks = clampPollIntervalSeconds(configuredPollSeconds) * TICKS_PER_SECOND;
        this.batchSize = clampBatchSize(configuredBatchSize);

        String dispatcher = trimToEmpty(checkin.getString("command-dispatcher", "console"));
        if (!dispatcher.isBlank() && !"console".equalsIgnoreCase(dispatcher)) {
            logWarn("Unsupported checkin.command-dispatcher '" + dispatcher + "', falling back to console.");
        }
        if (configuredPollSeconds < MIN_POLL_INTERVAL_SECONDS) {
            logWarn("checkin.poll-interval-seconds is below " + MIN_POLL_INTERVAL_SECONDS + ", clamped to " + MIN_POLL_INTERVAL_SECONDS + ".");
        }
        if (configuredBatchSize > MAX_BATCH_SIZE) {
            logWarn("checkin.batch-size is above " + MAX_BATCH_SIZE + ", clamped to " + MAX_BATCH_SIZE + ".");
        }

        String baseUrl = resolveBaseUrl(plugin.getConfig(), checkin);
        if (isUnset(baseUrl)) {
            this.enabled = false;
            this.client = null;
            this.maskedApiBase = "-";
            this.disabledReason = "missing checkin API base URL";
            logWarn("Checkin reward poller disabled: missing API base URL.");
            return;
        }

        String token = resolveToken(plugin.getConfig(), checkin);
        if (isUnset(token) || "CHANGE_ME".equalsIgnoreCase(token.trim())) {
            this.enabled = false;
            this.client = null;
            this.maskedApiBase = maskForDoctor(baseUrl);
            this.disabledReason = "missing token";
            logWarn("Checkin reward poller disabled: token is missing.");
            return;
        }

        int requestTimeoutSeconds = Math.max(1, checkin.getInt("request-timeout-seconds", 8));

        CheckinDeliveryClient resolvedClient = null;
        boolean resolvedEnabled = false;
        String resolvedDisabledReason = "";
        try {
            resolvedClient = new CheckinDeliveryClient(baseUrl, token, requestTimeoutSeconds);
            resolvedEnabled = true;
        } catch (IllegalArgumentException ex) {
            resolvedDisabledReason = ex.getMessage();
            logWarn("Checkin reward poller disabled: " + ex.getMessage());
        }

        this.client = resolvedClient;
        this.maskedApiBase = resolvedClient != null ? resolvedClient.getMaskedBaseUrl() : maskForDoctor(baseUrl);
        this.enabled = resolvedEnabled;
        this.disabledReason = resolvedDisabledReason;
    }

    public void start() {
        if (!enabled) {
            logInfo("Checkin reward poller not started: " + disabledReason + ".");
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }

        this.pollTask = Bukkit.getScheduler().runTaskTimerAsynchronously(
                plugin,
                this::pollOnce,
                TICKS_PER_SECOND,
                pollIntervalTicks
        );
        logInfo("Checkin reward poller started. interval=" + (pollIntervalTicks / TICKS_PER_SECOND) + "s, batchSize=" + batchSize + ".");
    }

    public void shutdown() {
        started.set(false);
        pollRunning.set(false);
        cancelTask(pollTask);
        pollTask = null;
        processingIds.clear();
        recentAckCache.clear();
    }

    public CheckinDoctorSnapshot getDoctorSnapshot() {
        String reason = disabledReason == null || disabledReason.isBlank()
                ? "-"
                : sanitizeForDoctor(disabledReason);
        String error = sanitizeForDoctor(lastError);
        return new CheckinDoctorSnapshot(
                enabled,
                started.get(),
                pollRunning.get(),
                processingIds.size(),
                recentAckCache.size(),
                maskedApiBase,
                reason,
                lastPollAt,
                lastFetchWarnAt,
                error
        );
    }

    private void pollOnce() {
        if (!enabled || !started.get() || plugin.isShuttingDown()) {
            return;
        }
        if (!pollRunning.compareAndSet(false, true)) {
            logDebug("Previous checkin poll is still running, skipping this tick.");
            return;
        }
        lastPollAt = System.currentTimeMillis();

        cleanupExpiredAckCache();
        client.fetchPendingDeliveries(batchSize).whenComplete((deliveries, throwable) -> {
            pollRunning.set(false);
            if (!started.get() || plugin.isShuttingDown()) {
                return;
            }
            if (throwable != null) {
                lastError = sanitizeForDoctor(summarize(throwable));
                logFetchFailure(throwable);
                return;
            }
            lastPollAt = System.currentTimeMillis();
            lastError = "";
            if (deliveries == null || deliveries.isEmpty()) {
                logDebug("No pending checkin deliveries.");
                return;
            }

            for (CheckinDeliveryClient.Delivery delivery : deliveries) {
                handleDelivery(delivery);
            }
        });
    }

    private void handleDelivery(CheckinDeliveryClient.Delivery delivery) {
        if (delivery == null) {
            return;
        }
        if (plugin.isShuttingDown()) {
            return;
        }

        long deliveryId = delivery.id();
        CachedAck cachedAck = recentAckCache.get(deliveryId);
        if (cachedAck != null && !cachedAck.isExpired()) {
            logDebug("Delivery " + deliveryId + " already handled in-memory, resending cached ACK.");
            sendAckAsync(cachedAck.toAck(deliveryId), true);
            return;
        }

        if (!processingIds.add(deliveryId)) {
            logDebug("Delivery " + deliveryId + " is already being processed.");
            return;
        }

        String validationError = validateDelivery(delivery);
        if (validationError != null) {
            finishProcessing(new CheckinDeliveryClient.DeliveryAck(deliveryId, false, validationError));
            return;
        }

        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                if (!started.get() || plugin.isShuttingDown()) {
                    processingIds.remove(deliveryId);
                    return;
                }

                CheckinDeliveryClient.DeliveryAck result = executeDelivery(delivery);
                finishProcessing(result);
            });
        } catch (IllegalStateException ex) {
            processingIds.remove(deliveryId);
            logWarn("Failed to schedule delivery " + deliveryId + " on the main thread: " + summarize(ex));
            logDebugThrowable(ex);
        }
    }

    private void finishProcessing(CheckinDeliveryClient.DeliveryAck ack) {
        cacheAckIfNeeded(ack);
        sendAckAsync(ack, false);
    }

    private void sendAckAsync(CheckinDeliveryClient.DeliveryAck ack, boolean cachedReplay) {
        client.acknowledge(ack).whenComplete((ignored, throwable) -> {
            if (!cachedReplay) {
                processingIds.remove(ack.deliveryId());
            }

            if (throwable != null) {
                logWarn("Failed to ACK checkin delivery " + ack.deliveryId() + ": " + summarize(throwable));
                logDebugThrowable(throwable);
                return;
            }

            if (!ack.success() && retryFailed) {
                recentAckCache.remove(ack.deliveryId());
            }
            logDebug("ACK completed for delivery " + ack.deliveryId() + " (success=" + ack.success() + ").");
        });
    }

    private void cacheAckIfNeeded(CheckinDeliveryClient.DeliveryAck ack) {
        if (!ack.success() && retryFailed) {
            return;
        }
        recentAckCache.put(ack.deliveryId(), new CachedAck(ack.success(), ack.message(), System.currentTimeMillis()));
    }

    private CheckinDeliveryClient.DeliveryAck executeDelivery(CheckinDeliveryClient.Delivery delivery) {
        ConsoleCommandSender console = Bukkit.getConsoleSender();
        List<String> commands = delivery.reward().commands();
        for (int index = 0; index < commands.size(); index++) {
            String preparedCommand = prepareCommand(commands.get(index), delivery.username(), delivery.mcUuid());
            if (preparedCommand == null || preparedCommand.isBlank()) {
                return new CheckinDeliveryClient.DeliveryAck(delivery.id(), false, "invalid command at index " + (index + 1));
            }
            if (preparedCommand.length() > MAX_COMMAND_LENGTH) {
                return new CheckinDeliveryClient.DeliveryAck(delivery.id(), false, "command too long at index " + (index + 1));
            }

            try {
                boolean success = Bukkit.dispatchCommand(console, preparedCommand);
                if (!success) {
                    return new CheckinDeliveryClient.DeliveryAck(delivery.id(), false, "command failed at index " + (index + 1));
                }
                logDebug("Executed checkin delivery " + delivery.id() + " command #" + (index + 1) + ": " + preparedCommand);
            } catch (Exception ex) {
                logWarn("Command execution failed for delivery " + delivery.id() + " at index " + (index + 1) + ": " + summarize(ex));
                logDebugThrowable(ex);
                return new CheckinDeliveryClient.DeliveryAck(delivery.id(), false, "command exception at index " + (index + 1));
            }
        }
        return new CheckinDeliveryClient.DeliveryAck(delivery.id(), true, "executed");
    }

    private String validateDelivery(CheckinDeliveryClient.Delivery delivery) {
        if (delivery.id() <= 0L) {
            return "invalid delivery id";
        }
        if (!USERNAME_PATTERN.matcher(trimToEmpty(delivery.username())).matches()) {
            return "invalid username";
        }
        if (!isValidUuid(delivery.mcUuid())) {
            return "invalid uuid";
        }
        if (delivery.reward() == null || delivery.reward().commands() == null) {
            return "invalid commands";
        }

        List<String> commands = delivery.reward().commands();
        if (commands.isEmpty()) {
            return "commands missing";
        }
        if (commands.size() > MAX_COMMANDS) {
            return "too many commands";
        }

        for (String command : commands) {
            if (command == null || command.isBlank()) {
                return "blank command";
            }
            if (command.length() > MAX_COMMAND_LENGTH) {
                return "command too long";
            }
            if (command.indexOf('\n') >= 0 || command.indexOf('\r') >= 0) {
                return "command contains newline";
            }
        }
        return null;
    }

    private void cleanupExpiredAckCache() {
        long now = System.currentTimeMillis();
        recentAckCache.entrySet().removeIf(entry -> now - entry.getValue().createdAt() > RECENT_ACK_TTL_MILLIS);
    }

    private void logFetchFailure(Throwable throwable) {
        long now = System.currentTimeMillis();
        if (now - lastFetchWarnAt >= FETCH_WARN_INTERVAL_MILLIS) {
            lastFetchWarnAt = now;
            logWarn("Failed to poll checkin deliveries: " + summarize(throwable));
        } else {
            logDebug("Failed to poll checkin deliveries: " + summarize(throwable) + " (suppressed by 60s rate limit)");
        }
        logDebugThrowable(throwable);
    }

    private static int clampPollIntervalSeconds(int value) {
        return Math.max(MIN_POLL_INTERVAL_SECONDS, value);
    }

    private static int clampBatchSize(int value) {
        return Math.max(1, Math.min(MAX_BATCH_SIZE, value));
    }

    private static boolean isValidUuid(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        try {
            UUID.fromString(value.trim());
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private static boolean isUnset(String value) {
        return value == null || value.isBlank();
    }

    private static String prepareCommand(String rawCommand, String username, String mcUuid) {
        if (rawCommand == null) {
            return null;
        }

        String command = rawCommand.trim();
        while (command.startsWith("/")) {
            command = command.substring(1).trim();
        }
        if (command.isBlank()) {
            return null;
        }
        return command
                .replace("{player}", username)
                .replace("{uuid}", mcUuid);
    }

    private static String resolveBaseUrl(FileConfiguration config, ConfigurationSection checkin) {
        String[] candidates = new String[] {
                checkin.getString("api-base-url"),
                config.getString("api.base-url"),
                config.getString("api-url"),
                config.getString("server-url")
        };
        for (String candidate : candidates) {
            if (!isUnset(candidate)) {
                return candidate.trim();
            }
        }
        return "";
    }

    private static String resolveToken(FileConfiguration config, ConfigurationSection checkin) {
        String[] candidates = new String[] {
                checkin.getString("token"),
                config.getString("api.token"),
                config.getString("token"),
                config.getString("websocket.auth_token")
        };
        for (String candidate : candidates) {
            if (!isUnset(candidate)) {
                return candidate.trim();
            }
        }
        return "";
    }

    private static String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private static String summarize(Throwable throwable) {
        if (throwable == null) {
            return "unknown error";
        }
        Throwable root = throwable;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        String message = root.getMessage();
        if (message == null || message.isBlank()) {
            return root.getClass().getSimpleName();
        }
        return message.trim();
    }

    private static String sanitizeForDoctor(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String sanitized = value.trim();
        sanitized = sanitized.replaceAll("(?i)(password|pwd|token|auth_token)\\s*[=:]\\s*[^\\s,;]+", "$1=***");
        sanitized = sanitized.replaceAll("(?i)(authorization\\s*:\\s*bearer\\s+)[^\\s,;]+", "$1***");
        if (sanitized.length() > 160) {
            sanitized = sanitized.substring(0, 160) + "...";
        }
        return sanitized;
    }

    private static String maskForDoctor(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String masked = value.trim();
        masked = masked.replaceAll("(?i)(token|auth_token|password)\\s*=\\s*([^&#\\s]+)", "$1=***");
        if (masked.length() > 160) {
            masked = masked.substring(0, 160) + "...";
        }
        return masked;
    }

    private void logInfo(String message) {
        plugin.getLogger().info("[Checkin] " + message);
    }

    private void logWarn(String message) {
        plugin.getLogger().warning("[Checkin] " + message);
    }

    private void logDebug(String message) {
        if (debug || StellarStatsSync.isDebug()) {
            plugin.getLogger().info("[Checkin][Debug] " + message);
        }
    }

    private void logDebugThrowable(Throwable throwable) {
        if (debug || StellarStatsSync.isDebug()) {
            plugin.getLogger().log(java.util.logging.Level.INFO, "[Checkin][Debug] Stack trace", throwable);
        }
    }

    private static void cancelTask(BukkitTask task) {
        if (task != null) {
            task.cancel();
        }
    }

    public record CheckinDoctorSnapshot(
            boolean enabled,
            boolean started,
            boolean pollRunning,
            int processingCount,
            int recentAckCacheSize,
            String maskedApiBase,
            String disabledReason,
            long lastPollAt,
            long lastFetchWarnAt,
            String lastError
    ) {
    }

    private record CachedAck(boolean success, String message, long createdAt) {
        private boolean isExpired() {
            return System.currentTimeMillis() - createdAt > RECENT_ACK_TTL_MILLIS;
        }

        private CheckinDeliveryClient.DeliveryAck toAck(long deliveryId) {
            return new CheckinDeliveryClient.DeliveryAck(deliveryId, success, message);
        }
    }
}
