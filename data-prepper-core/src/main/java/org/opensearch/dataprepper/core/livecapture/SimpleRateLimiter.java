/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple rate limiter implementation for live capture functionality.
 * Uses a token bucket approach to limit events per second.
 */
public class SimpleRateLimiter {
    private final double eventsPerSecond;
    private final AtomicLong lastRefillTime;
    private final AtomicLong availableTokens;
    private final long maxTokens;
    
    public SimpleRateLimiter(double eventsPerSecond) {
        this.eventsPerSecond = eventsPerSecond;
        this.maxTokens = Math.max(1, (long) eventsPerSecond);
        this.availableTokens = new AtomicLong(maxTokens);
        this.lastRefillTime = new AtomicLong(System.currentTimeMillis());
    }
    
    /**
     * Attempts to acquire a token for rate limiting.
     * 
     * @return true if a token was acquired, false otherwise
     */
    public boolean tryAcquire() {
        refillTokens();
        
        long currentTokens = availableTokens.get();
        if (currentTokens > 0) {
            return availableTokens.compareAndSet(currentTokens, currentTokens - 1);
        }
        return false;
    }
    
    /**
     * Refills tokens based on elapsed time since last refill.
     */
    private void refillTokens() {
        long currentTime = System.currentTimeMillis();
        long lastRefill = lastRefillTime.get();
        long elapsedTime = currentTime - lastRefill;
        
        if (elapsedTime > 0) {
            long tokensToAdd = (long) ((elapsedTime / 1000.0) * eventsPerSecond);
            if (tokensToAdd > 0) {
                if (lastRefillTime.compareAndSet(lastRefill, currentTime)) {
                    long currentTokens = availableTokens.get();
                    long newTokens = Math.min(maxTokens, currentTokens + tokensToAdd);
                    availableTokens.set(newTokens);
                }
            }
        }
    }
    
    /**
     * @return the configured events per second rate
     */
    public double getRate() {
        return eventsPerSecond;
    }
}