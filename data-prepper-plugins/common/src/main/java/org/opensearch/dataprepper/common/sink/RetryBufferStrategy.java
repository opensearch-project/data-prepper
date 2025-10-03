/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;
import com.linecorp.armeria.client.retry.Backoff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public abstract class RetryBufferStrategy extends DefaultBufferStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RetryBufferStrategy.class);
    private static final long INITIAL_DELAY_MS = 10;
    private static final long MAXIMUM_DELAY_MS = Duration.ofMinutes(10).toMillis();

    @Override
    public void flushBuffer() {
        int retryCount = 1;
        Object failedStatus = null;
        int maxRetries = getMaxRetries();
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY_MS, MAXIMUM_DELAY_MS).withMaxAttempts(maxRetries);
        long startTime = System.nanoTime();
        while (retryCount <= maxRetries) {
            failedStatus = doFlushOnce(failedStatus);
            if (failedStatus == null) {
                break;
            }
            final long delayMillis = backoff.nextDelayMillis(retryCount);
            if (delayMillis < 0) {
                break;
            }
            try {
                Thread.sleep(delayMillis);
            } catch (final InterruptedException e){}
            retryCount++;
        }
        if (failedStatus != null) {
            addFailedObjectsToDlqList(failedStatus);
        } else {
            recordLatency((double)System.nanoTime() - startTime);
        }
    }
    
    public abstract void addFailedObjectsToDlqList(Object failedStatus);
    public abstract Object doFlushOnce(Object failedStatus);
    public abstract int getMaxRetries();
}