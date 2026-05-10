package org.stellarvan.stellarstatssync.reward;

import com.google.gson.Gson;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import org.bukkit.scheduler.BukkitTask;
import org.stellarvan.stellarstatssync.DatabaseManager;
import org.stellarvan.stellarstatssync.StellarStatsSync;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class RewardOutboxWorker {

    private static final String DEFAULT_MAIL_TITLE = "每日签到奖励";
    private static final String PARTIAL_RISK_MESSAGE = "\u53ef\u80fd\u90e8\u5206\u53d1\u653e\uff0c\u9700\u8981\u4eba\u5de5\u590d\u6838\u3002";

    private final StellarStatsSync plugin;
    private final Gson gson;
    private final RewardOutboxSettings settings;
    private final RewardOutboxRepository repository;
    private final SweetMailRewardDispatcher sweetMailDispatcher;
    private final AtomicBoolean polling = new AtomicBoolean(false);

    private volatile BukkitTask pollTask;
    private volatile long lastPollAt = 0L;
    private volatile String lastError = "-";
    private volatile boolean schemaReady = false;
    private volatile String schemaError = "-";
    private volatile RewardOutboxRepository.RewardOutboxCounts lastCounts = RewardOutboxRepository.RewardOutboxCounts.unavailable();

    public RewardOutboxWorker(StellarStatsSync plugin, DatabaseManager databaseManager) {
        this.plugin = plugin;
        this.gson = new Gson();
        this.settings = RewardOutboxSettings.fromConfig(plugin.getConfig());
        this.repository = new RewardOutboxRepository(plugin, databaseManager);
        this.sweetMailDispatcher = new SweetMailRewardDispatcher(plugin, settings.sweetMail());
    }

    public void start() {
        if (!settings.commands().runOnMainThread()) {
            plugin.getLogger().warning("[RewardOutbox] commands.run-on-main-thread=false is ignored. Commands still run on the main thread.");
        }

        if (!settings.enabled()) {
            plugin.getLogger().info("[RewardOutbox] Disabled by config.");
            return;
        }

        try {
            repository.ensureSchema();
            schemaReady = true;
            schemaError = "-";
        } catch (Exception ex) {
            schemaReady = false;
            schemaError = sanitizeError("Reward outbox schema check failed: " + summarize(ex));
            lastError = schemaError;
            plugin.getLogger().warning("[RewardOutbox] " + schemaError);
            if (isDebugEnabled()) {
                plugin.getLogger().log(Level.WARNING, "[RewardOutbox][Debug] Schema ensure failure", ex);
            }
            return;
        }

        long intervalTicks = Math.max(20L, settings.pollIntervalSeconds() * 20L);
        this.pollTask = Bukkit.getScheduler().runTaskTimerAsynchronously(
                plugin,
                this::pollSafely,
                20L,
                intervalTicks
        );
        Bukkit.getScheduler().runTaskAsynchronously(plugin, this::pollSafely);
        plugin.getLogger().info("[RewardOutbox] Worker started. serverId=" + settings.serverId()
                + ", intervalSeconds=" + settings.pollIntervalSeconds()
                + ", batchSize=" + settings.batchSize()
                + ", maxAttempts=" + settings.maxAttempts());
    }

    public void shutdown() {
        BukkitTask task = pollTask;
        pollTask = null;
        if (task != null) {
            task.cancel();
        }
    }

    public boolean isEnabled() {
        return settings.enabled();
    }

    public RewardOutboxDoctorSnapshot getCachedDoctorSnapshot() {
        RewardOutboxRepository.RewardOutboxCounts counts = lastCounts;
        return new RewardOutboxDoctorSnapshot(
                settings.enabled(),
                schemaReady,
                schemaError,
                settings.serverId(),
                sweetMailDispatcher.isPluginInstalled(),
                settings.sweetMail().enabled() && sweetMailDispatcher.isPluginEnabled(),
                settings.commands().enabled(),
                counts.pending(),
                counts.processing(),
                counts.failed(),
                counts.deliveredToday(),
                lastPollAt,
                lastError
        );
    }

    public CompletableFuture<RewardOutboxDoctorSnapshot> collectDoctorSnapshotAsync() {
        RewardOutboxDoctorSnapshot cached = getCachedDoctorSnapshot();
        if (!plugin.isEnabled() || plugin.isShuttingDown() || !schemaReady) {
            return CompletableFuture.completedFuture(cached);
        }
        return repository.fetchCountsAsync(settings.serverId()).handle((counts, throwable) -> {
            if (throwable == null && counts != null) {
                lastCounts = counts;
                return new RewardOutboxDoctorSnapshot(
                        cached.enabled(),
                        cached.schemaReady(),
                        cached.schemaError(),
                        cached.serverId(),
                        cached.sweetMailInstalled(),
                        cached.sweetMailEnabled(),
                        cached.commandsEnabled(),
                        counts.pending(),
                        counts.processing(),
                        counts.failed(),
                        counts.deliveredToday(),
                        cached.lastPollAt(),
                        cached.lastError()
                );
            }
            String mergedError = mergeErrors(cached.lastError(), summarize(throwable));
            return new RewardOutboxDoctorSnapshot(
                    cached.enabled(),
                    cached.schemaReady(),
                    cached.schemaError(),
                    cached.serverId(),
                    cached.sweetMailInstalled(),
                    cached.sweetMailEnabled(),
                    cached.commandsEnabled(),
                    cached.pending(),
                    cached.processing(),
                    cached.failed(),
                    cached.deliveredToday(),
                    cached.lastPollAt(),
                    mergedError
            );
        });
    }

    private void pollSafely() {
        if (!settings.enabled() || plugin.isShuttingDown() || !schemaReady) {
            return;
        }
        if (!polling.compareAndSet(false, true)) {
            logDebug("Skipped reward outbox poll because the previous cycle is still running.");
            return;
        }

        try {
            doPollCycle();
        } catch (Exception ex) {
            lastError = sanitizeError("Reward outbox poll failed: " + summarize(ex));
            plugin.getLogger().warning("[RewardOutbox] " + lastError);
            if (isDebugEnabled()) {
                plugin.getLogger().log(Level.WARNING, "[RewardOutbox][Debug] Poll failure", ex);
            }
        } finally {
            lastPollAt = System.currentTimeMillis();
            polling.set(false);
        }
    }

    private void doPollCycle() throws Exception {
        String cycleLastError = "-";
        int recovered = repository.recoverStaleProcessingTasks(settings.serverId(), settings.processingTimeoutSeconds());
        if (recovered > 0) {
            logDebug("Recovered stale processing tasks: " + recovered);
        }

        List<RewardOutboxEntry> candidates = repository.fetchPendingBatch(
                settings.serverId(),
                settings.maxAttempts(),
                settings.batchSize()
        );

        int claimed = 0;
        int delivered = 0;
        int failed = 0;
        int requeued = 0;

        for (RewardOutboxEntry candidate : candidates) {
            if (plugin.isShuttingDown()) {
                break;
            }
            if (!repository.claimPending(candidate.id())) {
                continue;
            }

            claimed++;
            int currentAttempt = candidate.attempts() + 1;
            ProcessResult result = processClaimedEntry(candidate);
            if (result.delivered()) {
                repository.markDelivered(candidate.id());
                delivered++;
            } else {
                String failureMessage = sanitizeError(result.error());
                cycleLastError = failureMessage;
                boolean markFailed = result.forceFailed() || currentAttempt >= settings.maxAttempts();
                if (markFailed) {
                    repository.markFailed(candidate.id(), failureMessage);
                    failed++;
                } else {
                    repository.markPending(candidate.id(), failureMessage);
                    requeued++;
                }
            }
            logEntrySummary(candidate, currentAttempt, result);
        }

        try {
            lastCounts = repository.fetchCounts(settings.serverId());
        } catch (Exception ex) {
            cycleLastError = mergeErrors(cycleLastError, "Failed to refresh reward outbox counters: " + summarize(ex));
        }

        lastError = sanitizeError(cycleLastError);
        logDebug("Poll summary: recovered=" + recovered
                + ", claimed=" + claimed
                + ", delivered=" + delivered
                + ", failed=" + failed
                + ", requeued=" + requeued);
    }

    private ProcessResult processClaimedEntry(RewardOutboxEntry entry) {
        try {
            if (repository.hasDeliveredRewardForDay(entry)) {
                return ProcessResult.failure(
                        "A delivered reward already exists for the same player/server/source/sign_date. " + PARTIAL_RISK_MESSAGE,
                        true,
                        false
                );
            }
        } catch (Exception ex) {
            return ProcessResult.failure("Failed to check duplicate daily reward state: " + summarize(ex), false, false);
        }

        RewardPayload payload;
        try {
            payload = RewardPayload.parse(gson, entry.rewardPayloadJson());
        } catch (Exception ex) {
            return ProcessResult.failure("Failed to parse reward_payload_json: " + summarize(ex), false, false);
        }

        if (payload.isEmpty()) {
            return ProcessResult.failure("Reward payload is empty. No mail items or commands were provided.", false, false);
        }

        PlaceholderContext placeholders = PlaceholderContext.from(entry, payload.meta());
        List<String> resolvedCommands;
        try {
            resolvedCommands = resolveCommands(payload.commands(), placeholders);
        } catch (RewardDispatchException ex) {
            return ProcessResult.failure(ex.getMessage(), !ex.retryable(), ex.partialRisk());
        }

        String resolvedTitle = placeholders.replace(resolveMailTitle(payload));
        String resolvedIcon = resolveMailIcon(payload);
        List<String> resolvedMailContent = resolveMailContent(payload, placeholders);

        try {
            return runOnMainThread(() -> dispatchOnMainThread(payload, placeholders, resolvedTitle, resolvedIcon, resolvedMailContent, resolvedCommands));
        } catch (Exception ex) {
            return ProcessResult.failure("Failed to dispatch reward on the main thread: " + summarize(ex), false, false);
        }
    }

    private ProcessResult dispatchOnMainThread(
            RewardPayload payload,
            PlaceholderContext placeholders,
            String title,
            String icon,
            List<String> content,
            List<String> commands
    ) {
        boolean mailDelivered = false;
        try {
            List<ItemStack> attachments = buildMailAttachments(payload.items());
            if (payload.hasMail()) {
                UUID playerUuid = placeholders.requirePlayerUuid();
                sweetMailDispatcher.sendSystemMail(
                        playerUuid,
                        settings.sweetMail().systemSender(),
                        icon,
                        title,
                        content,
                        attachments
                );
                mailDelivered = true;
            }

            if (!commands.isEmpty()) {
                executeCommands(commands);
            }

            return ProcessResult.success();
        } catch (RewardDispatchException ex) {
            String message = ex.getMessage();
            if (mailDelivered && !ex.partialRisk()) {
                message = mergeErrors(message, PARTIAL_RISK_MESSAGE);
            }
            boolean forceFailed = mailDelivered || ex.partialRisk();
            return ProcessResult.failure(message, forceFailed, forceFailed);
        } catch (Exception ex) {
            String message = summarize(ex);
            if (mailDelivered) {
                message = mergeErrors(message, PARTIAL_RISK_MESSAGE);
            }
            return ProcessResult.failure(message, mailDelivered, mailDelivered);
        }
    }

    private List<String> resolveCommands(List<String> rawCommands, PlaceholderContext placeholders) throws RewardDispatchException {
        if (rawCommands == null || rawCommands.isEmpty()) {
            return List.of();
        }
        if (!settings.commands().enabled()) {
            throw new RewardDispatchException("Reward payload contains commands but reward-outbox.commands.enabled=false.", true, false);
        }

        List<String> resolved = new ArrayList<>(rawCommands.size());
        for (String rawCommand : rawCommands) {
            if (rawCommand == null || rawCommand.isBlank()) {
                continue;
            }
            if (rawCommand.contains("{player}") && placeholders.playerName().isBlank()) {
                throw new RewardDispatchException("Reward command requires {player}, but player_name is blank.", false, false);
            }
            if (rawCommand.contains("{uuid}")) {
                placeholders.requirePlayerUuid();
            }
            String command = placeholders.replace(rawCommand).trim();
            if (command.startsWith("/")) {
                command = command.substring(1).trim();
            }
            if (command.isBlank()) {
                throw new RewardDispatchException("Reward command becomes blank after placeholder replacement.", false, false);
            }
            resolved.add(command);
        }
        return List.copyOf(resolved);
    }

    private List<String> resolveMailContent(RewardPayload payload, PlaceholderContext placeholders) {
        List<String> content = payload.mail().content();
        if (content == null || content.isEmpty()) {
            return List.of(
                    placeholders.replace("你完成了 {date} 的每日签到。"),
                    placeholders.replace("连续签到：{continuous} 天"),
                    placeholders.replace("累计签到：{total} 次"),
                    "奖励已投递，请在邮箱中领取附件。"
            );
        }

        List<String> resolved = new ArrayList<>(content.size());
        for (String line : content) {
            if (line != null) {
                resolved.add(placeholders.replace(line));
            }
        }
        return List.copyOf(resolved);
    }

    private String resolveMailTitle(RewardPayload payload) {
        String title = payload.mail().title();
        if (title == null || title.isBlank()) {
            return DEFAULT_MAIL_TITLE;
        }
        return title.trim();
    }

    private String resolveMailIcon(RewardPayload payload) {
        String icon = payload.mail().icon();
        if (icon == null || icon.isBlank()) {
            return settings.sweetMail().defaultIcon();
        }
        return icon.trim();
    }

    private List<ItemStack> buildMailAttachments(List<RewardPayload.Item> items) throws RewardDispatchException {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        if (!settings.sweetMail().enabled()) {
            throw new RewardDispatchException("Reward payload contains mail items but reward-outbox.sweetmail.enabled=false.", true, false);
        }

        List<ItemStack> attachments = new ArrayList<>(items.size());
        for (RewardPayload.Item item : items) {
            if (item == null) {
                continue;
            }
            String materialKey = normalizeMaterialKey(item.type());
            if (materialKey.isBlank()) {
                throw new RewardDispatchException("Reward item type is blank.", false, false);
            }
            Material material = Material.matchMaterial(materialKey);
            if (material == null || material.isAir()) {
                throw new RewardDispatchException("Unsupported reward item type: " + item.type(), false, false);
            }
            int amount = item.amount();
            int maxStackSize = Math.max(1, material.getMaxStackSize());
            if (amount < 1 || amount > maxStackSize) {
                throw new RewardDispatchException(
                        "Invalid reward item amount for " + material.name() + ": " + amount + " (max " + maxStackSize + ")",
                        false,
                        false
                );
            }
            attachments.add(new ItemStack(material, amount));
        }
        return List.copyOf(attachments);
    }

    private void executeCommands(List<String> commands) throws RewardDispatchException {
        int executed = 0;
        for (String command : commands) {
            try {
                boolean ok = Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command);
                if (!ok) {
                    throw new IllegalStateException("dispatchCommand returned false");
                }
                executed++;
            } catch (Exception ex) {
                String detail = summarize(ex);
                if (executed > 0) {
                    throw new RewardDispatchException(
                            "Reward command execution failed after " + executed + " command(s). " + PARTIAL_RISK_MESSAGE
                                    + " Failing command: " + command + ". Reason: " + detail,
                            false,
                            true,
                            ex
                    );
                }
                throw new RewardDispatchException(
                        "Reward command execution failed before completion. Command: " + command + ". Reason: " + detail,
                        true,
                        false,
                        ex
                );
            }
        }
    }

    private <T> T runOnMainThread(Callable<T> callable) throws Exception {
        Objects.requireNonNull(callable, "callable");
        if (Bukkit.isPrimaryThread()) {
            return callable.call();
        }
        try {
            return Bukkit.getScheduler().callSyncMethod(plugin, callable).get(60L, TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new IllegalStateException("Main-thread reward dispatch failed.", cause);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for main-thread reward dispatch.", ex);
        } catch (TimeoutException ex) {
            throw new IllegalStateException("Timed out while waiting for main-thread reward dispatch.", ex);
        }
    }

    private void logEntrySummary(RewardOutboxEntry entry, int currentAttempt, ProcessResult result) {
        if (!isDebugEnabled()) {
            return;
        }
        plugin.getLogger().info("[RewardOutbox][Debug] id=" + entry.id()
                + ", requestId=" + safeValue(entry.requestId())
                + ", player=" + safeValue(entry.playerName())
                + ", rewardType=" + safeValue(entry.rewardType())
                + ", signDate=" + safeValue(entry.signDate())
                + ", attempt=" + currentAttempt
                + ", delivered=" + result.delivered()
                + ", forceFailed=" + result.forceFailed()
                + ", error=" + sanitizeError(result.error()));
    }

    private void logDebug(String message) {
        if (isDebugEnabled()) {
            plugin.getLogger().info("[RewardOutbox][Debug] " + message);
        }
    }

    private boolean isDebugEnabled() {
        return settings.debug() || StellarStatsSync.isDebug();
    }

    private static String normalizeMaterialKey(String rawType) {
        if (rawType == null || rawType.isBlank()) {
            return "";
        }
        String normalized = rawType.trim();
        if (normalized.regionMatches(true, 0, "minecraft:", 0, "minecraft:".length())) {
            normalized = normalized.substring("minecraft:".length());
        }
        return normalized.toUpperCase(Locale.ROOT);
    }

    private static String safeValue(String value) {
        return value == null || value.isBlank() ? "-" : value;
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
        return message == null || message.isBlank() ? root.getClass().getSimpleName() : message.trim();
    }

    private static String mergeErrors(String first, String second) {
        String left = sanitizeError(first);
        String right = sanitizeError(second);
        if ("-".equals(left)) {
            return right;
        }
        if ("-".equals(right) || left.equals(right)) {
            return left;
        }
        return sanitizeError(left + " | " + right);
    }

    private static String sanitizeError(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String sanitized = value.trim();
        sanitized = sanitized.replaceAll("(?i)(password|pwd|token|auth_token|secret)\\s*[=:]\\s*[^\\s,;]+", "$1=***");
        sanitized = sanitized.replaceAll("(?i)(authorization\\s*:\\s*bearer\\s+)[^\\s,;]+", "$1***");
        sanitized = sanitized.replaceAll("(?i)jdbc:mysql://[^\\s,;]+", "jdbc:mysql://***");
        if (sanitized.length() > 240) {
            sanitized = sanitized.substring(0, 240) + "...";
        }
        return sanitized;
    }

    public record RewardOutboxDoctorSnapshot(
            boolean enabled,
            boolean schemaReady,
            String schemaError,
            String serverId,
            boolean sweetMailInstalled,
            boolean sweetMailEnabled,
            boolean commandsEnabled,
            int pending,
            int processing,
            int failed,
            int deliveredToday,
            long lastPollAt,
            String lastError
    ) {
    }

    private record PlaceholderContext(
            String requestId,
            String playerUuidRaw,
            String playerName,
            String signDate,
            int continuous,
            int total
    ) {
        private static PlaceholderContext from(RewardOutboxEntry entry, RewardPayload.Meta meta) {
            String payloadSignDate = meta == null || meta.signDate() == null || meta.signDate().isBlank()
                    ? ""
                    : meta.signDate().trim();
            String entrySignDate = entry.signDate() == null || entry.signDate().isBlank()
                    ? ""
                    : entry.signDate().trim();
            String signDate = payloadSignDate.isBlank() ? entrySignDate : payloadSignDate;
            return new PlaceholderContext(
                    entry.requestId() == null ? "" : entry.requestId().trim(),
                    entry.playerUuid() == null ? "" : entry.playerUuid().trim(),
                    entry.playerName() == null ? "" : entry.playerName().trim(),
                    signDate,
                    meta == null ? 0 : meta.continuous(),
                    meta == null ? 0 : meta.total()
            );
        }

        private UUID requirePlayerUuid() throws RewardDispatchException {
            if (playerUuidRaw == null || playerUuidRaw.isBlank()) {
                throw new RewardDispatchException("player_uuid is blank.", false, false);
            }
            try {
                return UUID.fromString(playerUuidRaw);
            } catch (IllegalArgumentException ex) {
                throw new RewardDispatchException("Invalid player_uuid: " + playerUuidRaw, false, false, ex);
            }
        }

        private String replace(String value) {
            String resolved = value == null ? "" : value;
            resolved = resolved.replace("{player}", playerName == null ? "" : playerName);
            resolved = resolved.replace("{uuid}", playerUuidRaw == null ? "" : playerUuidRaw);
            resolved = resolved.replace("{request_id}", requestId == null ? "" : requestId);
            resolved = resolved.replace("{date}", signDate == null ? "" : signDate);
            resolved = resolved.replace("{continuous}", Integer.toString(Math.max(0, continuous)));
            resolved = resolved.replace("{total}", Integer.toString(Math.max(0, total)));
            return resolved;
        }
    }

    private record ProcessResult(boolean delivered, boolean forceFailed, String error) {
        private static ProcessResult success() {
            return new ProcessResult(true, false, "-");
        }

        private static ProcessResult failure(String error, boolean forceFailed, boolean partialRisk) {
            String suffix = partialRisk ? " " + PARTIAL_RISK_MESSAGE : "";
            return new ProcessResult(false, forceFailed, sanitizeError(error + suffix));
        }
    }
}
