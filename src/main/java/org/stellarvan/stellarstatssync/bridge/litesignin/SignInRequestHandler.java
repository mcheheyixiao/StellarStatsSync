package org.stellarvan.stellarstatssync.bridge.litesignin;

import org.bukkit.entity.Player;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

final class SignInRequestHandler {

    private static final int CODE_INVALID_REQUEST = 4100;
    private static final int CODE_INVALID_UUID = 4101;
    private static final int CODE_PLUGIN_MISSING = 4102;
    private static final int CODE_PLAYER_OFFLINE = 4103;
    private static final int CODE_PROVIDER_FAILURE = 4500;

    private final LiteSignInBridge bridge;

    SignInRequestHandler(LiteSignInBridge bridge) {
        this.bridge = bridge;
    }

    SignInRequestOutcome handle(String requestId, Object rawPayload) {
        if (!bridge.isEnabledByConfig()) {
            return SignInRequestOutcome.failure(
                    CODE_INVALID_REQUEST,
                    "bridge_disabled",
                    "LiteSignIn bridge is disabled by config.",
                    null
            );
        }

        if (requestId == null || requestId.isBlank()) {
            bridge.logWarn("Received invalid signin.request: missing requestId.");
            return SignInRequestOutcome.failure(
                    CODE_INVALID_REQUEST,
                    "invalid_request",
                    "Missing requestId in signin.request.",
                    null
            );
        }

        if (!(rawPayload instanceof Map<?, ?> payload)) {
            bridge.logWarn("Received invalid signin.request " + requestId + ": payload is not an object.");
            return SignInRequestOutcome.failure(
                    CODE_INVALID_REQUEST,
                    "invalid_payload",
                    "signin.request payload must be an object.",
                    null
            );
        }

        if (bridge.isDebugEnabled()) {
            bridge.logDebug("Received signin.request " + requestId + " payload: " + payload);
        }

        String playerUuidRaw = asNonBlankString(payload.get("playerUuid"));
        if (playerUuidRaw == null) {
            bridge.logWarn("Received invalid signin.request " + requestId + ": missing playerUuid.");
            return SignInRequestOutcome.failure(
                    CODE_INVALID_UUID,
                    "invalid_player_uuid",
                    "playerUuid is required.",
                    null
            );
        }

        UUID playerUuid;
        try {
            playerUuid = UUID.fromString(playerUuidRaw);
        } catch (IllegalArgumentException ex) {
            bridge.logWarn("Received invalid signin.request " + requestId + ": invalid playerUuid " + playerUuidRaw + ".");
            return SignInRequestOutcome.failure(
                    CODE_INVALID_UUID,
                    "invalid_player_uuid",
                    "Invalid playerUuid: " + playerUuidRaw,
                    null
            );
        }

        LiteSignInBridge.ProviderState state = bridge.refreshProviderState(true);
        if (!state.providerEnabled()) {
            return SignInRequestOutcome.failure(
                    CODE_PLUGIN_MISSING,
                    "plugin_missing_litesignin",
                    "LiteSignIn is not installed or not enabled.",
                    null
            );
        }

        boolean requireOnline = bridge.isRequirePlayerOnline() || asBoolean(payload.get("requireOnline"), true);
        Player player = bridge.findOnlinePlayer(playerUuid);
        if (player == null) {
            bridge.logWarn("Website sign-in rejected for offline player " + playerUuid + ".");
            return SignInRequestOutcome.failure(
                    CODE_PLAYER_OFFLINE,
                    "player_offline",
                    requireOnline
                            ? "Player must be online for website sign-in in phase 1."
                            : "Offline sign-in is not implemented in phase 1.",
                    null
            );
        }

        Object storage = bridge.getStorageForOnlinePlayer(player);
        if (storage == null) {
            bridge.logWarn("LiteSignIn returned null storage for online player " + playerUuid + ".");
            return SignInRequestOutcome.failure(
                    CODE_PROVIDER_FAILURE,
                    "litesignin_api_failed",
                    "LiteSignIn returned null player storage.",
                    null
            );
        }

        Object websiteUserId = normalizeWebsiteUserId(payload.get("websiteUserId"));
        String source = normalizeSource(payload.get("source"));

        if (bridge.isAlreadySigned(storage)) {
            LiteSignInBridge.SignInSnapshot snapshot = bridge.captureSnapshot(storage, playerUuid, player.getName());
            return SignInRequestOutcome.success(
                    "already_signed",
                    buildBasePayload(snapshot, websiteUserId, source, false)
            );
        }

        try {
            LiteSignInBridge.SignInSnapshot snapshot = bridge.executeOnlineSignIn(storage, player, source, requestId);
            Map<String, Object> resultPayload = buildBasePayload(snapshot, websiteUserId, source, true);
            resultPayload.put("rewardMode", "litesignin_online");
            return SignInRequestOutcome.success("signed", resultPayload);
        } catch (Exception ex) {
            bridge.logWarn("LiteSignIn API failed for request " + requestId + ": " + ex.getMessage());
            return SignInRequestOutcome.failure(
                    CODE_PROVIDER_FAILURE,
                    "litesignin_api_failed",
                    ex.getMessage(),
                    null
            );
        }
    }

    private Map<String, Object> buildBasePayload(
            LiteSignInBridge.SignInSnapshot snapshot,
            Object websiteUserId,
            String source,
            boolean includeLastSignInAt
    ) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (websiteUserId != null) {
            payload.put("websiteUserId", websiteUserId);
        }
        payload.put("serverId", bridge.getServerId());
        payload.put("playerUuid", snapshot.playerUuid().toString());
        payload.put("playerName", snapshot.playerName());
        payload.put("signedToday", snapshot.signedToday());
        payload.put("continuous", snapshot.continuous());
        payload.put("total", snapshot.total());
        payload.put("source", source);
        payload.put("signDate", snapshot.signDate());
        if (includeLastSignInAt && snapshot.lastSignInAt() != null && !snapshot.lastSignInAt().isBlank()) {
            payload.put("lastSignInAt", snapshot.lastSignInAt());
        }
        return payload;
    }

    private static Object normalizeWebsiteUserId(Object value) {
        if (value instanceof Number number) {
            long asLong = number.longValue();
            return asLong == number.doubleValue() ? asLong : number.doubleValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            return string.trim();
        }
        return null;
    }

    private static String normalizeSource(Object value) {
        String source = value == null ? "web" : String.valueOf(value).trim().toLowerCase(Locale.ROOT);
        return "game".equals(source) ? "game" : "web";
    }

    private static String asNonBlankString(Object value) {
        if (value == null) {
            return null;
        }
        String string = String.valueOf(value).trim();
        return string.isBlank() ? null : string;
    }

    private static boolean asBoolean(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String string) {
            String normalized = string.trim().toLowerCase(Locale.ROOT);
            if ("true".equals(normalized)) {
                return true;
            }
            if ("false".equals(normalized)) {
                return false;
            }
        }
        return defaultValue;
    }

    record SignInRequestOutcome(boolean ok, int code, String status, String message, Map<String, Object> payload) {

        static SignInRequestOutcome success(String status, Map<String, Object> payload) {
            return new SignInRequestOutcome(true, 0, status, "ok", payload);
        }

        static SignInRequestOutcome failure(int code, String status, String message, Map<String, Object> payload) {
            return new SignInRequestOutcome(false, Math.max(1, code), status, message, payload);
        }
    }
}
