package org.stellarvan.stellarstatssync.bridge.litesignin;

import org.bukkit.event.Event;
import org.bukkit.event.Listener;

public final class LiteSignInListener implements Listener {

    private final LiteSignInBridge bridge;

    public LiteSignInListener(LiteSignInBridge bridge) {
        this.bridge = bridge;
    }

    void handle(Event event) {
        bridge.handleLiteSignInEvent(event);
    }
}
