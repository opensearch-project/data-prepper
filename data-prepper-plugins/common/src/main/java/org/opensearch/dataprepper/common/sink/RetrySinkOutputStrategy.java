/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import com.linecorp.armeria.client.retry.Backoff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

public abstract class RetrySinkOutputStrategy extends DefaultSinkOutputStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RetrySinkOutputStrategy.class);
    private static final long INITIAL_DELAY_MS = 10;
    private static final long MAXIMUM_DELAY_MS = Duration.ofMinutes(10).toMillis();
    public RetrySinkOutputStrategy(LockStrategy lockStrategy, BufferStrategy bufferStrategy) {
        super(lockStrategy, bufferStrategy);
    }

    
    public abstract void addFailedObjectsToDlqList(Object failedStatus);
    public abstract Object  doFlushOnce(Object failedStatus);
    public abstract int     getMaxRetries();
}
