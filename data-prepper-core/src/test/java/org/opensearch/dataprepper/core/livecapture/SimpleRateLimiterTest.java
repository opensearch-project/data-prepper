/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class SimpleRateLimiterTest {

    private SimpleRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        rateLimiter = new SimpleRateLimiter(2.0); // 2 events per second
    }

    @Test
    void testGetRate() {
        assertThat(rateLimiter.getRate(), equalTo(2.0));
    }

    @Test
    void testTryAcquireInitialTokens() {
        // Should be able to acquire at least some tokens initially
        boolean acquired = rateLimiter.tryAcquire();
        assertThat(acquired, is(true));
    }

    @Test
    void testTryAcquireExhaustsTokens() {
        // Exhaust all available tokens
        int acquiredCount = 0;
        for (int i = 0; i < 10; i++) {
            if (rateLimiter.tryAcquire()) {
                acquiredCount++;
            }
        }
        
        // Should have acquired at least 1 token (the initial tokens)
        assertThat(acquiredCount >= 1, is(true));
        
        // Next attempt should fail as tokens are exhausted
        boolean acquired = rateLimiter.tryAcquire();
        assertThat(acquired, is(false));
    }

    @Test
    void testTokenRefillAfterTime() throws InterruptedException {
        // Exhaust tokens
        while (rateLimiter.tryAcquire()) {
            // Keep acquiring until exhausted
        }
        
        // Verify no tokens available
        assertThat(rateLimiter.tryAcquire(), is(false));
        
        // Wait for tokens to refill
        Thread.sleep(600); // Wait 600ms, should allow at least 1 token at 2 EPS
        
        // Should be able to acquire again
        boolean acquired = rateLimiter.tryAcquire();
        assertThat(acquired, is(true));
    }

    @Test
    void testHighRateRateLimiter() {
        SimpleRateLimiter highRateLimit = new SimpleRateLimiter(100.0);
        
        assertThat(highRateLimit.getRate(), equalTo(100.0));
        
        // Should be able to acquire multiple tokens
        int acquiredCount = 0;
        for (int i = 0; i < 50; i++) {
            if (highRateLimit.tryAcquire()) {
                acquiredCount++;
            }
        }
        
        // Should have acquired many tokens with high rate
        assertThat(acquiredCount >= 10, is(true));
    }

    @Test
    void testLowRateRateLimiter() {
        SimpleRateLimiter lowRateLimit = new SimpleRateLimiter(0.1); // 0.1 events per second
        
        assertThat(lowRateLimit.getRate(), equalTo(0.1));
        
        // Should be able to acquire at least 1 token initially
        boolean firstAcquire = lowRateLimit.tryAcquire();
        assertThat(firstAcquire, is(true));
        
        // Subsequent acquire should fail immediately
        boolean secondAcquire = lowRateLimit.tryAcquire();
        assertThat(secondAcquire, is(false));
    }
}