package org.stellarvan.stellarstatssync.websocket.dto;

import java.util.LinkedHashMap;
import java.util.UUID;

public class MessageEnvelope {

    public String type;
    public boolean success;
    public int code;
    public String message;
    public String requestId;
    public long timestamp;
    public Object data;
    public Object payload;

    @SuppressWarnings("unused")
    public static MessageEnvelope success(String type, Object data) {
        return success(type, data, null);
    }

    public static MessageEnvelope success(String type, Object data, String requestId) {
        MessageEnvelope msg = new MessageEnvelope();
        msg.type = type;
        msg.success = true;
        msg.code = 0;
        msg.message = "ok";
        msg.requestId = normalizeRequestId(requestId);
        msg.timestamp = System.currentTimeMillis();
        Object safeData = normalizeData(data);
        msg.data = safeData;
        msg.payload = safeData;
        return msg;
    }

    @SuppressWarnings("unused")
    // Reserved overload for call sites that do not provide requestId explicitly.
    public static MessageEnvelope error(String type, int code, String message) {
        return error(type, code, message, null);
    }

    public static MessageEnvelope error(String type, int code, String message, String requestId) {
        MessageEnvelope msg = new MessageEnvelope();
        msg.type = type;
        msg.success = false;
        msg.code = code;
        msg.message = message == null ? "" : message;
        msg.requestId = normalizeRequestId(requestId);
        msg.timestamp = System.currentTimeMillis();
        Object emptyData = normalizeData(null);
        msg.data = emptyData;
        msg.payload = emptyData;
        return msg;
    }

    public void ensureCompatibility() {
        if (requestId == null || requestId.isBlank()) {
            requestId = generateRequestId();
        }
        if (timestamp <= 0L) {
            timestamp = System.currentTimeMillis();
        }
        Object source = data != null ? data : payload;
        Object safeData = normalizeData(source);
        data = safeData;
        payload = safeData;
    }

    private static Object normalizeData(Object data) {
        return data == null ? new LinkedHashMap<String, Object>() : data;
    }

    private static String normalizeRequestId(String requestId) {
        if (requestId == null || requestId.isBlank()) {
            return generateRequestId();
        }
        return requestId;
    }

    private static String generateRequestId() {
        return UUID.randomUUID().toString();
    }
}
