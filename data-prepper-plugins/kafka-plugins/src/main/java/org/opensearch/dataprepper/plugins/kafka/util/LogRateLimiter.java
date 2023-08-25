package org.opensearch.dataprepper.plugins.kafka.util;

// Poor-man implementation of rate-limiter for logging error messages.
// Todo: Use token-bucket as a generic rate-limiter.
public class LogRateLimiter {
    public static int MILLIS_PER_SECOND = 1000;
    public static int MAX_LOGS_PER_SECOND = 1000;
    private int tokens;
    private long lastMs;
    private long replenishInterval;

    public LogRateLimiter(final int ratePerSecond, final long currentMs) {
        if (ratePerSecond  < 0 || ratePerSecond > MAX_LOGS_PER_SECOND) {
            throw new IllegalArgumentException(
                    String.format("Invalid arguments. ratePerSecond should be >0 and less than %s", MAX_LOGS_PER_SECOND)
            );
        }
        replenishInterval = MILLIS_PER_SECOND / ratePerSecond;
        lastMs = currentMs;
        tokens = 1;
    }

    public boolean isAllowed(final long currentMs) {
        if ((currentMs- lastMs) >= replenishInterval) {
            tokens = 1;
            lastMs = currentMs;
        }

        if (tokens == 0) {
            return false;
        }

        tokens--;
        return true;
    }
}
