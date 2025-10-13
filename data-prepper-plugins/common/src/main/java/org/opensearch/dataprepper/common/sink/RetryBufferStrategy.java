/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import com.linecorp.armeria.client.retry.Backoff;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public abstract class RetryBufferStrategy extends DefaultBufferStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RetryBufferStrategy.class);
    private static final long INITIAL_DELAY_MS = 10;
    private static final long MAXIMUM_DELAY_MS = Duration.ofMinutes(10).toMillis();
    private final int maxRetries;

    public RetryBufferStrategy(final long maxEventSize, final long maxEvents, final long maxRequestSize, final long flushIntervalMs, final int maxRetries) {
        super(maxEventSize, maxEvents, maxRequestSize, flushIntervalMs);
        this.maxRetries = maxRetries;
    }

    @Override
    public boolean flushBuffer(final SinkMetrics sinkMetrics) {
        int retryCount = 1;
        Object failedStatus = null;
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY_MS, MAXIMUM_DELAY_MS)
                                       .withMaxAttempts(maxRetries);
        while (retryCount <= maxRetries) {
            try {
                failedStatus = doFlush(failedStatus);
            } catch (Exception e) {
                LOG.warn(NOISY, "Failed to flush.", e);
            }
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
        sinkMetrics.incrementRetries(retryCount);
        if (failedStatus != null) {
            addFailedObjectsToDlqList(failedStatus);
            return false;
        } 
        return true;
    }
    
    public abstract void addFailedObjectsToDlqList(Object failedStatus);
}
