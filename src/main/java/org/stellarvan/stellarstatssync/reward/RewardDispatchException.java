package org.stellarvan.stellarstatssync.reward;

public final class RewardDispatchException extends Exception {

    private final boolean retryable;
    private final boolean partialRisk;

    public RewardDispatchException(String message, boolean retryable, boolean partialRisk) {
        super(message);
        this.retryable = retryable;
        this.partialRisk = partialRisk;
    }

    public RewardDispatchException(String message, boolean retryable, boolean partialRisk, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
        this.partialRisk = partialRisk;
    }

    public boolean retryable() {
        return retryable;
    }

    public boolean partialRisk() {
        return partialRisk;
    }
}
