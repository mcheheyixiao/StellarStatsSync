package org.stellarvan.stellarstatssync.bridge.litesignin;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.entity.Player;
import org.bukkit.event.Event;
import org.bukkit.event.EventPriority;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.PluginManager;
import org.bukkit.scheduler.BukkitTask;
import org.stellarvan.stellarstatssync.StellarStatsSync;
import org.stellarvan.stellarstatssync.WebSocketSyncManager;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

public final class LiteSignInBridge {

    private static final String PROVIDER_NAME = "LiteSignIn";
    private static final String STORAGE_CLASS_NAME = "studio.trc.bukkit.litesignin.api.Storage";
    private static final String PLAYER_SIGNIN_EVENT_CLASS_NAME = "studio.trc.bukkit.litesignin.event.custom.PlayerSignInEvent";

    private final StellarStatsSync plugin;
    private final WebSocketSyncManager webSocketSyncManager;
    private final SignInRequestHandler requestHandler;
    private final LiteSignInListener listener;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Map<UUID, RequestContext> requestContexts = new ConcurrentHashMap<>();

    private volatile BukkitTask cleanupTask;
    private volatile ReflectionAccess reflectionAccess;
    private volatile boolean enabled;
    private volatile boolean requirePlayerOnline;
    private volatile boolean listenLiteSignInEvents;
    private volatile boolean sendGameSignInUpdates;
    private volatile boolean debug;
    private volatile long requestContextTtlMillis;
    private volatile boolean listenerRegistered;
    private volatile String disabledReason = "-";

    public LiteSignInBridge(StellarStatsSync plugin, WebSocketSyncManager webSocketSyncManager) {
        this.plugin = plugin;
        this.webSocketSyncManager = webSocketSyncManager;
        this.requestHandler = new SignInRequestHandler(this);
        this.listener = new LiteSignInListener(this);
        reloadConfiguration();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        reloadConfiguration();
        scheduleCleanupTask();
        refreshProviderState(true);
    }

    public void shutdown() {
        started.set(false);
        listenerRegistered = false;
        requestContexts.clear();
        BukkitTask task = cleanupTask;
        cleanupTask = null;
        if (task != null) {
            task.cancel();
        }
    }

    public SignInDoctorSnapshot getDoctorSnapshot() {
        ProviderState state = refreshProviderState(false);
        return new SignInDoctorSnapshot(
                enabled,
                PROVIDER_NAME,
                state.providerAvailable(),
                state.providerEnabled(),
                listenerRegistered && state.providerEnabled(),
                requirePlayerOnline,
                sendGameSignInUpdates,
                requestContexts.size(),
                disabledReason
        );
    }

    public void handleSignInRequest(String requestId, Object rawPayload) {
        Runnable task = () -> {
            SignInRequestHandler.SignInRequestOutcome outcome;
            try {
                outcome = requestHandler.handle(requestId, rawPayload);
            } catch (Exception ex) {
                String detail = summarize(ex);
                logWarn("LiteSignIn request handling failed: " + detail);
                logDebugThrowable(ex);
                outcome = SignInRequestHandler.SignInRequestOutcome.failure(
                        4500,
                        "litesignin_api_failed",
                        detail,
                        null
                );
            }
            sendSignInResult(requestId, outcome);
        };

        if (Bukkit.isPrimaryThread()) {
            task.run();
            return;
        }

        try {
            Bukkit.getScheduler().runTask(plugin, task);
        } catch (IllegalStateException ex) {
            String detail = summarize(ex);
            logWarn("Failed to schedule signin.request on the main thread: " + detail);
            logDebugThrowable(ex);
            sendSignInResult(
                    requestId,
                    SignInRequestHandler.SignInRequestOutcome.failure(
                            4500,
                            "litesignin_api_failed",
                            "Failed to schedule sign-in handling on the main thread.",
                            null
                    )
            );
        }
    }

    void handleLiteSignInEvent(Event event) {
        if (!enabled || !listenLiteSignInEvents || event == null || plugin.isShuttingDown()) {
            return;
        }

        PendingSignInEvent pendingEvent;
        try {
            pendingEvent = extractPendingSignInEvent(event);
        } catch (Exception ex) {
            logWarn("Failed to inspect LiteSignIn event: " + summarize(ex));
            logDebugThrowable(ex);
            return;
        }

        if (pendingEvent == null) {
            return;
        }

        try {
            Bukkit.getScheduler().runTask(plugin, () -> emitSignInUpdated(pendingEvent));
        } catch (IllegalStateException ex) {
            logWarn("Failed to schedule signin.updated after LiteSignIn event: " + summarize(ex));
            logDebugThrowable(ex);
        }
    }

    boolean isEnabledByConfig() {
        return enabled;
    }

    boolean isRequirePlayerOnline() {
        return requirePlayerOnline;
    }

    boolean isDebugEnabled() {
        return debug || StellarStatsSync.isDebug();
    }

    String getServerId() {
        return webSocketSyncManager.getServerId();
    }

    Player findOnlinePlayer(UUID uuid) {
        return uuid == null ? null : Bukkit.getPlayer(uuid);
    }

    ProviderState refreshProviderState(boolean warnIfUnavailable) {
        if (!enabled) {
            disabledReason = "disabled_by_config";
            return new ProviderState(false, false);
        }

        Plugin provider = plugin.getServer().getPluginManager().getPlugin(PROVIDER_NAME);
        boolean available = provider != null;
        boolean providerEnabled = available && provider.isEnabled();

        if (!providerEnabled) {
            reflectionAccess = null;
            listenerRegistered = false;
            disabledReason = "LiteSignIn is not installed or not enabled.";
            if (warnIfUnavailable) {
                logWarn(disabledReason);
            }
            return new ProviderState(available, false);
        }

        try {
            ReflectionAccess access = reflectionAccess;
            if (access == null) {
                access = ReflectionAccess.resolve(provider);
                reflectionAccess = access;
            }
            if (listenLiteSignInEvents) {
                ensureListenerRegistered(access);
            }
            disabledReason = "-";
            return new ProviderState(true, true);
        } catch (ReflectiveOperationException ex) {
            reflectionAccess = null;
            listenerRegistered = false;
            disabledReason = "Failed to resolve LiteSignIn API: " + summarize(ex);
            if (warnIfUnavailable) {
                logWarn(disabledReason);
            }
            logDebugThrowable(ex);
            return new ProviderState(true, false);
        }
    }

    Object getStorageForOnlinePlayer(Player player) {
        if (player == null) {
            return null;
        }
        ReflectionAccess access = requireReflectionAccess();
        if (access == null) {
            return null;
        }
        try {
            return access.storageGetByPlayer.invoke(null, player);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Failed to load LiteSignIn storage for player " + player.getUniqueId(), ex);
        }
    }

    Object getStorageByUuid(UUID uuid) {
        if (uuid == null) {
            return null;
        }
        ReflectionAccess access = requireReflectionAccess();
        if (access == null) {
            return null;
        }
        try {
            return access.storageGetByUuid.invoke(null, uuid);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Failed to load LiteSignIn storage for UUID " + uuid, ex);
        }
    }

    boolean isAlreadySigned(Object storage) {
        if (storage == null) {
            return false;
        }
        ReflectionAccess access = requireReflectionAccess();
        if (access == null) {
            return false;
        }
        try {
            return Boolean.TRUE.equals(access.alreadySignIn.invoke(storage));
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Failed to query LiteSignIn sign-in state.", ex);
        }
    }

    SignInSnapshot captureSnapshot(Object storage, UUID uuid, String preferredName) {
        if (storage == null || uuid == null) {
            return null;
        }
        ReflectionAccess access = requireReflectionAccess();
        if (access == null) {
            return null;
        }

        try {
            boolean signedToday = Boolean.TRUE.equals(access.alreadySignIn.invoke(storage));
            int continuous = asInt(access.getContinuousSignIn.invoke(storage));
            int total = asInt(access.getCumulativeNumber.invoke(storage));
            int year = asInt(access.getYear.invoke(storage));
            int month = asInt(access.getMonth.invoke(storage));
            int day = asInt(access.getDay.invoke(storage));
            int hour = asInt(access.getHour.invoke(storage));
            int minute = asInt(access.getMinute.invoke(storage));
            int second = asInt(access.getSecond.invoke(storage));
            String playerName = normalizePlayerName(preferredName);
            if (playerName.isBlank()) {
                playerName = normalizePlayerName(asString(access.getName.invoke(storage)));
            }
            if (playerName.isBlank()) {
                playerName = normalizePlayerName(resolvePlayerName(uuid));
            }

            return new SignInSnapshot(
                    uuid,
                    playerName,
                    signedToday,
                    continuous,
                    total,
                    formatDate(year, month, day),
                    formatDateTime(year, month, day, hour, minute, second)
            );
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalStateException("Failed to read LiteSignIn sign-in data.", ex);
        }
    }

    SignInSnapshot executeOnlineSignIn(Object storage, Player player, String source, String requestId) {
        if (storage == null || player == null) {
            return null;
        }
        ReflectionAccess access = requireReflectionAccess();
        if (access == null) {
            return null;
        }

        UUID uuid = player.getUniqueId();
        rememberRequestContext(uuid, source, requestId);
        try {
            access.signIn.invoke(storage);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            requestContexts.remove(uuid);
            throw new IllegalStateException("LiteSignIn signIn() invocation failed.", ex);
        }

        SignInSnapshot snapshot = captureSnapshot(storage, uuid, player.getName());
        if (snapshot == null || !snapshot.signedToday()) {
            requestContexts.remove(uuid);
            throw new IllegalStateException("LiteSignIn did not confirm the sign-in.");
        }
        return snapshot;
    }

    void logWarn(String message) {
        plugin.getLogger().warning("[SignIn] " + message);
    }

    void logDebug(String message) {
        if (isDebugEnabled()) {
            plugin.getLogger().info("[SignIn][Debug] " + message);
        }
    }

    private void sendSignInResult(String requestId, SignInRequestHandler.SignInRequestOutcome outcome) {
        if (outcome == null) {
            return;
        }
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("ok", outcome.ok());
        data.put("status", outcome.status());
        if (outcome.payload() != null && !outcome.payload().isEmpty()) {
            data.put("payload", outcome.payload());
        }
        if (outcome.message() != null && !outcome.message().isBlank()) {
            data.put("message", outcome.message());
        }

        webSocketSyncManager.sendCustomEnvelope(
                "signin.result",
                outcome.ok(),
                outcome.code(),
                outcome.message(),
                data,
                requestId
        );
    }

    private void sendSignInUpdated(Map<String, Object> payload) {
        webSocketSyncManager.sendCustomEnvelope(
                "signin.updated",
                true,
                0,
                "ok",
                payload,
                null
        );
    }

    private void emitSignInUpdated(PendingSignInEvent pendingEvent) {
        if (plugin.isShuttingDown()) {
            return;
        }

        cleanupExpiredRequestContexts();
        RequestContext context = takeRequestContext(pendingEvent.playerUuid());
        String source = context != null ? context.source() : "game";
        if ("game".equalsIgnoreCase(source) && !sendGameSignInUpdates) {
            return;
        }

        try {
            Object storage = getStorageByUuid(pendingEvent.playerUuid());
            if (storage == null) {
                throw new IllegalStateException("LiteSignIn returned null player storage.");
            }
            SignInSnapshot snapshot = captureSnapshot(storage, pendingEvent.playerUuid(), resolvePlayerName(pendingEvent.playerUuid()));
            if (snapshot == null) {
                throw new IllegalStateException("Failed to capture LiteSignIn snapshot.");
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("serverId", getServerId());
            payload.put("playerUuid", snapshot.playerUuid().toString());
            payload.put("playerName", snapshot.playerName());
            payload.put("signedToday", snapshot.signedToday());
            payload.put("continuous", snapshot.continuous());
            payload.put("total", snapshot.total());
            payload.put("source", source);
            payload.put("usingRetroactiveCard", pendingEvent.usingRetroactiveCard());
            payload.put("signDate", pendingEvent.signDate());
            if (context != null && context.requestId() != null && !context.requestId().isBlank()) {
                payload.put("originRequestId", context.requestId());
            }

            if (isDebugEnabled()) {
                logDebug("Sending signin.updated payload: " + payload);
            }
            sendSignInUpdated(payload);
        } catch (Exception ex) {
            logWarn("Failed to publish signin.updated: " + summarize(ex));
            logDebugThrowable(ex);
        }
    }

    private PendingSignInEvent extractPendingSignInEvent(Event event) throws ReflectiveOperationException {
        ReflectionAccess access = requireReflectionAccess();
        if (access == null || !access.playerSignInEventClass.isInstance(event)) {
            return null;
        }

        Object uuidValue = access.eventGetUuid.invoke(event);
        if (!(uuidValue instanceof UUID playerUuid)) {
            return null;
        }

        Object signDateValue = access.eventGetDate.invoke(event);
        String signDate = formatDate(signDateValue, access);
        boolean usingRetroactiveCard = Boolean.TRUE.equals(access.eventUsingRetroactiveCard.invoke(event));
        return new PendingSignInEvent(playerUuid, signDate, usingRetroactiveCard);
    }

    private void ensureListenerRegistered(ReflectionAccess access) {
        if (listenerRegistered || access == null) {
            return;
        }
        PluginManager pluginManager = plugin.getServer().getPluginManager();
        pluginManager.registerEvent(
                access.playerSignInEventClass,
                listener,
                EventPriority.MONITOR,
                (ignored, event) -> listener.handle(event),
                plugin,
                true
        );
        listenerRegistered = true;
        logDebug("LiteSignIn PlayerSignInEvent listener registered.");
    }

    private void rememberRequestContext(UUID uuid, String source, String requestId) {
        if (uuid == null) {
            return;
        }
        cleanupExpiredRequestContexts();
        requestContexts.put(uuid, new RequestContext(
                normalizeSource(source),
                requestId == null ? "" : requestId.trim(),
                System.currentTimeMillis() + requestContextTtlMillis
        ));
    }

    private RequestContext takeRequestContext(UUID uuid) {
        if (uuid == null) {
            return null;
        }
        RequestContext context = requestContexts.remove(uuid);
        if (context == null || context.expireAt() < System.currentTimeMillis()) {
            return null;
        }
        return context;
    }

    private void reloadConfiguration() {
        ConfigurationSection signIn = plugin.getConfig().getConfigurationSection("signin");
        this.enabled = signIn == null || signIn.getBoolean("enabled", true);
        this.requirePlayerOnline = signIn == null || signIn.getBoolean("require_player_online", true);
        this.listenLiteSignInEvents = signIn == null || signIn.getBoolean("listen_litesignin_events", true);
        this.sendGameSignInUpdates = signIn == null || signIn.getBoolean("send_game_signin_updates", true);
        this.debug = signIn != null && signIn.getBoolean("debug", false);
        long ttlSeconds = signIn != null ? signIn.getLong("request_context_ttl_seconds", 15L) : 15L;
        this.requestContextTtlMillis = Math.max(5L, ttlSeconds) * 1000L;
    }

    private void scheduleCleanupTask() {
        long intervalTicks = Math.max(20L, (requestContextTtlMillis / 1000L) * 20L);
        this.cleanupTask = Bukkit.getScheduler().runTaskTimerAsynchronously(
                plugin,
                this::cleanupExpiredRequestContexts,
                intervalTicks,
                intervalTicks
        );
    }

    private void cleanupExpiredRequestContexts() {
        long now = System.currentTimeMillis();
        requestContexts.entrySet().removeIf(entry -> entry.getValue().expireAt() < now);
    }

    private ReflectionAccess requireReflectionAccess() {
        ProviderState state = refreshProviderState(false);
        if (!state.providerEnabled()) {
            return null;
        }
        return reflectionAccess;
    }

    private String resolvePlayerName(UUID uuid) {
        Player player = Bukkit.getPlayer(uuid);
        if (player != null) {
            return normalizePlayerName(player.getName());
        }
        OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);
        return offlinePlayer == null ? "" : normalizePlayerName(offlinePlayer.getName());
    }

    private void logDebugThrowable(Throwable throwable) {
        if (isDebugEnabled() && throwable != null) {
            plugin.getLogger().log(Level.INFO, "[SignIn][Debug] Stack trace", throwable);
        }
    }

    private static int asInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String string) {
            try {
                return Integer.parseInt(string.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return 0;
    }

    private static String asString(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static String normalizePlayerName(String value) {
        return value == null ? "" : value.trim();
    }

    private static String normalizeSource(String value) {
        String source = value == null || value.isBlank() ? "web" : value.trim().toLowerCase(Locale.ROOT);
        return Objects.equals(source, "game") ? "game" : "web";
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

    private static String formatDate(Object signInDate, ReflectionAccess access) throws ReflectiveOperationException {
        if (signInDate == null || access == null) {
            return "";
        }
        int year = asInt(access.signInDateGetYear.invoke(signInDate));
        int month = asInt(access.signInDateGetMonth.invoke(signInDate));
        int day = asInt(access.signInDateGetDay.invoke(signInDate));
        return formatDate(year, month, day);
    }

    private static String formatDate(int year, int month, int day) {
        if (year <= 0 || month <= 0 || day <= 0) {
            return "";
        }
        return String.format(Locale.ROOT, "%04d-%02d-%02d", year, month, day);
    }

    private static String formatDateTime(int year, int month, int day, int hour, int minute, int second) {
        if (year <= 0 || month <= 0 || day <= 0) {
            return "";
        }
        return String.format(Locale.ROOT, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
    }

    public record SignInDoctorSnapshot(
            boolean enabled,
            String provider,
            boolean providerAvailable,
            boolean providerEnabled,
            boolean eventListening,
            boolean requirePlayerOnline,
            boolean sendGameSignInUpdates,
            int requestContextSize,
            String disabledReason
    ) {
    }

    record ProviderState(boolean providerAvailable, boolean providerEnabled) {
    }

    record SignInSnapshot(
            UUID playerUuid,
            String playerName,
            boolean signedToday,
            int continuous,
            int total,
            String signDate,
            String lastSignInAt
    ) {
    }

    private record PendingSignInEvent(UUID playerUuid, String signDate, boolean usingRetroactiveCard) {
    }

    private record RequestContext(String source, String requestId, long expireAt) {
    }

    private static final class ReflectionAccess {

        private final Method storageGetByPlayer;
        private final Method storageGetByUuid;
        private final Method alreadySignIn;
        private final Method signIn;
        private final Method getContinuousSignIn;
        private final Method getCumulativeNumber;
        private final Method getYear;
        private final Method getMonth;
        private final Method getDay;
        private final Method getHour;
        private final Method getMinute;
        private final Method getSecond;
        private final Method getName;
        private final Class<? extends Event> playerSignInEventClass;
        private final Method eventGetUuid;
        private final Method eventGetDate;
        private final Method eventUsingRetroactiveCard;
        private final Method signInDateGetYear;
        private final Method signInDateGetMonth;
        private final Method signInDateGetDay;

        private ReflectionAccess(
                Method storageGetByPlayer,
                Method storageGetByUuid,
                Method alreadySignIn,
                Method signIn,
                Method getContinuousSignIn,
                Method getCumulativeNumber,
                Method getYear,
                Method getMonth,
                Method getDay,
                Method getHour,
                Method getMinute,
                Method getSecond,
                Method getName,
                Class<? extends Event> playerSignInEventClass,
                Method eventGetUuid,
                Method eventGetDate,
                Method eventUsingRetroactiveCard,
                Method signInDateGetYear,
                Method signInDateGetMonth,
                Method signInDateGetDay
        ) {
            this.storageGetByPlayer = storageGetByPlayer;
            this.storageGetByUuid = storageGetByUuid;
            this.alreadySignIn = alreadySignIn;
            this.signIn = signIn;
            this.getContinuousSignIn = getContinuousSignIn;
            this.getCumulativeNumber = getCumulativeNumber;
            this.getYear = getYear;
            this.getMonth = getMonth;
            this.getDay = getDay;
            this.getHour = getHour;
            this.getMinute = getMinute;
            this.getSecond = getSecond;
            this.getName = getName;
            this.playerSignInEventClass = playerSignInEventClass;
            this.eventGetUuid = eventGetUuid;
            this.eventGetDate = eventGetDate;
            this.eventUsingRetroactiveCard = eventUsingRetroactiveCard;
            this.signInDateGetYear = signInDateGetYear;
            this.signInDateGetMonth = signInDateGetMonth;
            this.signInDateGetDay = signInDateGetDay;
        }

        @SuppressWarnings("unchecked")
        private static ReflectionAccess resolve(Plugin provider) throws ReflectiveOperationException {
            ClassLoader classLoader = provider.getClass().getClassLoader();
            Class<?> storageClass = Class.forName(STORAGE_CLASS_NAME, true, classLoader);
            Method storageGetByPlayer = storageClass.getMethod("getPlayer", Player.class);
            Method storageGetByUuid = storageClass.getMethod("getPlayer", UUID.class);
            Method alreadySignIn = storageClass.getMethod("alreadySignIn");
            Method signIn = storageClass.getMethod("signIn");
            Method getContinuousSignIn = storageClass.getMethod("getContinuousSignIn");
            Method getCumulativeNumber = storageClass.getMethod("getCumulativeNumber");
            Method getYear = storageClass.getMethod("getYear");
            Method getMonth = storageClass.getMethod("getMonth");
            Method getDay = storageClass.getMethod("getDay");
            Method getHour = storageClass.getMethod("getHour");
            Method getMinute = storageClass.getMethod("getMinute");
            Method getSecond = storageClass.getMethod("getSecond");
            Method getName = storageClass.getMethod("getName");

            Class<? extends Event> playerSignInEventClass =
                    (Class<? extends Event>) Class.forName(PLAYER_SIGNIN_EVENT_CLASS_NAME, true, classLoader);
            Method eventGetUuid = playerSignInEventClass.getMethod("getUUID");
            Method eventGetDate = playerSignInEventClass.getMethod("getDate");
            Method eventUsingRetroactiveCard = playerSignInEventClass.getMethod("usingRetroactiveCard");

            Class<?> signInDateClass = eventGetDate.getReturnType();
            Method signInDateGetYear = signInDateClass.getMethod("getYear");
            Method signInDateGetMonth = signInDateClass.getMethod("getMonth");
            Method signInDateGetDay = signInDateClass.getMethod("getDay");

            return new ReflectionAccess(
                    storageGetByPlayer,
                    storageGetByUuid,
                    alreadySignIn,
                    signIn,
                    getContinuousSignIn,
                    getCumulativeNumber,
                    getYear,
                    getMonth,
                    getDay,
                    getHour,
                    getMinute,
                    getSecond,
                    getName,
                    playerSignInEventClass,
                    eventGetUuid,
                    eventGetDate,
                    eventUsingRetroactiveCard,
                    signInDateGetYear,
                    signInDateGetMonth,
                    signInDateGetDay
            );
        }
    }
}
