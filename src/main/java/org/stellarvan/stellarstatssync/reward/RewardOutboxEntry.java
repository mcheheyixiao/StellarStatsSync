package org.stellarvan.stellarstatssync.reward;

public record RewardOutboxEntry(
        long id,
        String requestId,
        String websiteUserId,
        String playerUuid,
        String playerName,
        String serverId,
        String source,
        String rewardType,
        String rewardPayloadJson,
        int attempts,
        String signDate
) {
}
