package org.stellarvan.stellarstatssync;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.annotations.SerializedName;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

final class CheckinDeliveryClient {

    private static final int RESPONSE_PREVIEW_LIMIT = 200;

    private final HttpClient httpClient;
    private final Gson gson;
    private final URI deliveriesEndpoint;
    private final URI acknowledgeEndpoint;
    private final String token;
    private final Duration requestTimeout;

    CheckinDeliveryClient(String baseUrl, String token, int requestTimeoutSeconds) {
        String normalizedBaseUrl = normalizeBaseUrl(baseUrl);
        this.deliveriesEndpoint = URI.create(normalizedBaseUrl + "/api/plugin/checkin/deliveries");
        this.acknowledgeEndpoint = URI.create(normalizedBaseUrl + "/api/plugin/checkin/deliveries/ack");
        this.token = token == null ? "" : token.trim();
        this.requestTimeout = Duration.ofSeconds(Math.max(1, requestTimeoutSeconds));
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(this.requestTimeout)
                .build();
        this.gson = new Gson();
    }

    public CompletableFuture<List<Delivery>> fetchPendingDeliveries(int limit) {
        String payload = gson.toJson(new DeliveriesRequestPayload(limit));
        return send("POST", deliveriesEndpoint, payload)
                .thenApply(this::requireSuccessfulResponse)
                .thenApply(this::parseDeliveriesResponse);
    }

    public CompletableFuture<Void> acknowledge(DeliveryAck ack) {
        String payload = gson.toJson(new AckPayload(ack.deliveryId(), ack.success(), ack.message()));
        return send("POST", acknowledgeEndpoint, payload)
                .thenApply(this::requireSuccessfulResponse)
                .thenApply(this::validateAckResponse)
                .thenApply(ignored -> null);
    }

    private CompletableFuture<HttpResponse<String>> send(String method, URI uri, String jsonBody) {
        return sendInternal(method, uri, jsonBody, true)
                .thenCompose(response -> {
                    if (!shouldRetryWithQueryToken(response.statusCode()) || token.isBlank()) {
                        return CompletableFuture.completedFuture(response);
                    }

                    URI tokenUri = appendQueryParam(uri, "token", token);
                    return sendInternal(method, tokenUri, jsonBody, false)
                            .thenCompose(fallback -> {
                                if (!shouldRetryWithQueryToken(fallback.statusCode()) || token.isBlank()) {
                                    return CompletableFuture.completedFuture(fallback);
                                }
                                URI authTokenUri = appendQueryParam(uri, "auth_token", token);
                                return sendInternal(method, authTokenUri, jsonBody, false);
                            });
                });
    }

    private CompletableFuture<HttpResponse<String>> sendInternal(String method, URI uri, String jsonBody, boolean useAuthorizationHeader) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(requestTimeout)
                .header("Accept", "application/json")
                .header("User-Agent", "StellarStatsSync");

        if (useAuthorizationHeader && !token.isBlank()) {
            builder.header("Authorization", "Bearer " + token);
        }

        if ("POST".equalsIgnoreCase(method)) {
            builder.header("Content-Type", "application/json; charset=utf-8");
            builder.POST(HttpRequest.BodyPublishers.ofString(jsonBody == null ? "" : jsonBody, StandardCharsets.UTF_8));
        } else {
            builder.GET();
        }

        return httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private String requireSuccessfulResponse(HttpResponse<String> response) {
        int statusCode = response.statusCode();
        if (statusCode >= 200 && statusCode < 300) {
            return response.body();
        }
        if (statusCode == 405 && isDeliveriesRequest(response)) {
            throw new IllegalStateException(
                    "Checkin deliveries endpoint rejected request with 405. "
                            + "The plugin should use POST /api/plugin/checkin/deliveries. "
                            + "Please verify the running jar is updated. Response: "
                            + preview(response.body())
            );
        }
        throw new IllegalStateException("HTTP " + statusCode + ": " + preview(response.body()));
    }

    private boolean isDeliveriesRequest(HttpResponse<String> response) {
        if (response == null || response.request() == null || response.request().uri() == null) {
            return false;
        }
        String path = response.request().uri().getPath();
        return path != null && path.endsWith("/api/plugin/checkin/deliveries");
    }

    private List<Delivery> parseDeliveriesResponse(String body) {
        try {
            DeliveriesResponse response = gson.fromJson(body, DeliveriesResponse.class);
            if (response == null) {
                throw new IllegalStateException("Empty response body.");
            }
            if (!response.success) {
                throw new IllegalStateException(nonBlankOrDefault(response.message, "API returned success=false."));
            }
            if (response.deliveries == null || response.deliveries.isEmpty()) {
                return List.of();
            }

            List<Delivery> deliveries = new ArrayList<>(response.deliveries.size());
            for (DeliveryPayload payload : response.deliveries) {
                if (payload == null) {
                    continue;
                }

                Reward reward = new Reward(toCommands(payload.reward));
                deliveries.add(new Delivery(
                        payload.id == null ? 0L : payload.id,
                        payload.username,
                        payload.mcUuid,
                        reward
                ));
            }
            return Collections.unmodifiableList(deliveries);
        } catch (JsonParseException ex) {
            throw new IllegalStateException("Invalid JSON response: " + ex.getMessage(), ex);
        }
    }

    private String validateAckResponse(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }

        try {
            GenericApiResponse response = gson.fromJson(body, GenericApiResponse.class);
            if (response != null && !response.success) {
                throw new IllegalStateException(nonBlankOrDefault(response.message, "ACK rejected by API."));
            }
        } catch (JsonParseException ignored) {
            // Some endpoints may return a plain success body. A 2xx status is enough in that case.
        }
        return body;
    }

    private List<String> toCommands(RewardPayload reward) {
        if (reward == null || reward.commands == null) {
            return null;
        }
        if (!(reward.commands instanceof List<?> rawCommands)) {
            return null;
        }
        if (rawCommands.isEmpty()) {
            return List.of();
        }

        List<String> commands = new ArrayList<>(rawCommands.size());
        for (Object value : rawCommands) {
            commands.add(value instanceof String string ? string : null);
        }
        return Collections.unmodifiableList(commands);
    }

    private static boolean shouldRetryWithQueryToken(int statusCode) {
        return statusCode == 401 || statusCode == 403;
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("Checkin API base URL is blank.");
        }
        String trimmed = baseUrl.trim();
        URI uri = URI.create(trimmed);
        String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("Unsupported checkin API scheme: " + scheme);
        }
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }

    private static URI appendQueryParam(URI uri, String key, String value) {
        String rawQuery = uri.getRawQuery();
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        if (rawQuery != null) {
            String lowerQuery = rawQuery.toLowerCase(Locale.ROOT);
            String lowerKey = encodedKey.toLowerCase(Locale.ROOT) + "=";
            if (lowerQuery.startsWith(lowerKey)
                    || lowerQuery.contains("&" + lowerKey)) {
                return uri;
            }
        }

        String rawUri = uri.toString();
        String fragment = "";
        int fragmentIndex = rawUri.indexOf('#');
        if (fragmentIndex >= 0) {
            fragment = rawUri.substring(fragmentIndex);
            rawUri = rawUri.substring(0, fragmentIndex);
        }

        String separator = rawUri.contains("?") ? "&" : "?";
        return URI.create(rawUri + separator + encodedKey + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8) + fragment);
    }

    private static String nonBlankOrDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value.trim();
    }

    private static String preview(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        String normalized = body.replace('\n', ' ').replace('\r', ' ').trim();
        if (normalized.length() <= RESPONSE_PREVIEW_LIMIT) {
            return normalized;
        }
        return normalized.substring(0, RESPONSE_PREVIEW_LIMIT) + "...";
    }

    public record Delivery(long id, String username, String mcUuid, Reward reward) {
    }

    public record Reward(List<String> commands) {
    }

    public record DeliveryAck(long deliveryId, boolean success, String message) {
    }

    private static final class DeliveriesResponse {
        private boolean success;
        private String message;
        private List<DeliveryPayload> deliveries;
    }

    private static final class DeliveryPayload {
        private Long id;
        private String username;
        @SerializedName(value = "mc_uuid", alternate = {"mcUuid"})
        private String mcUuid;
        private RewardPayload reward;
    }

    private static final class RewardPayload {
        private Object commands;
    }

    private static final class GenericApiResponse {
        private boolean success = true;
        private String message;
    }

    private static final class AckPayload {
        @SerializedName("delivery_id")
        private final long deliveryId;
        private final boolean success;
        private final String message;

        private AckPayload(long deliveryId, boolean success, String message) {
            this.deliveryId = deliveryId;
            this.success = success;
            this.message = message;
        }
    }

    private static final class DeliveriesRequestPayload {
        private final int limit;

        private DeliveriesRequestPayload(int limit) {
            this.limit = Math.max(1, limit);
        }
    }
}

