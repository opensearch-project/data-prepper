/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.common.unit.TimeValue;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackOffUtilsTests {
    @Test
    public void testWithExpBackoff() throws InterruptedException {
        final long start = 50;
        final int testNumOfRetries = 3;
        final Iterator<TimeValue> timeValueIterator =
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(start), testNumOfRetries).iterator();
        final BackOffUtils backOffUtils = new BackOffUtils(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(start), testNumOfRetries).iterator());
        final long startTime = System.currentTimeMillis();
        // first attempt
        assertTrue(backOffUtils.hasNext());
        assertTrue(backOffUtils.next());
        for (int i = 0; i < testNumOfRetries; i++) {
            assertTrue(backOffUtils.hasNext());
            assertTrue(backOffUtils.next());
            final long currTime = System.currentTimeMillis();
            assertTrue(currTime - startTime >= timeValueIterator.next().getMillis());
        }
        assertFalse(backOffUtils.hasNext());
        assertFalse(backOffUtils.next());
    }
}
