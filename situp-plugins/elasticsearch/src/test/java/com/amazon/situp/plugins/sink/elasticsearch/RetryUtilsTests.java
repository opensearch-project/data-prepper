package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RetryUtilsTests {
    @Test
    public void testWithExpBackoff() throws InterruptedException {
        final long start = 50;
        final int testNumOfRetries = 3;
        final RetryUtils retryUtils = new RetryUtils(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(start), testNumOfRetries).iterator());
        final long startTime = System.currentTimeMillis();
        for (int i = 0; i < testNumOfRetries; i++) {
            assertTrue(retryUtils.hasNext());
            assertTrue(retryUtils.next());
            final long currTime = System.currentTimeMillis();
            assertTrue(currTime - startTime >= start + 10 * ((int) Math.exp(0.8d * i) - 1));
        }
        assertFalse(retryUtils.hasNext());
        assertFalse(retryUtils.next());
    }
}
