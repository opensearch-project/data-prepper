/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import com.linecorp.armeria.client.retry.Backoff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public abstract class SqsSinkExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SqsSinkExecutor.class);
    private static final long INITIAL_DELAY_MS = 10;
    private static final long MAXIMUM_DELAY_MS = Duration.ofMinutes(10).toMillis();

    public void execute(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            lock();
            try {
                if (exceedsFlushTimeInterval()) {
                    flushBuffer();
                }
            } finally {
                unlock();
            }
            pushDLQList();
            return;
        }
        lock();
        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();
                try {
                    long estimatedSize = getEstimatedSize(event);
                    if (exceedsMaxEventSizeThreshold(estimatedSize)) {
                        throw new RuntimeException("Event size exceeds max allowed event size");
                    }
                    if (willExceedMaxBatchSize(event, estimatedSize)) {
                        flushBuffer();
                    }
                    boolean reachedMaxEventsLimit = addToBuffer(event, estimatedSize);
                    if (reachedMaxEventsLimit) {
                        flushBuffer();
                    } 
                } catch (Exception ex) {
                    addEventToDLQList(event, ex);
                }
            }
            pushDLQList();
        } finally {
            unlock();
        }
    }

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
            pushFailedObjectsToDlq(failedStatus);
        } else {
            recordLatency(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
    }
    
    public abstract void pushFailedObjectsToDlq(Object failedStatus);
    public abstract void pushDLQList();
    public abstract void addEventToDLQList(final Event event, Throwable ex);
    public abstract Object  doFlushOnce(Object failedStatus);
    public abstract int     getMaxRetries();
    public abstract boolean addToBuffer(final Event event, final long estimatedSize) throws Exception;
    public abstract boolean exceedsFlushTimeInterval();
    public abstract boolean willExceedMaxBatchSize(final Event event, final long estimatedSize) throws Exception;
    public abstract boolean exceedsMaxEventSizeThreshold(final long estimatedSize);
    public abstract long getEstimatedSize(final Event event) throws Exception;
    public abstract void recordLatency(long amount, TimeUnit timeUnit);

    public abstract void lock();
    public abstract void unlock();

}

