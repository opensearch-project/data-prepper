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

public interface SinkOutputStrategy {
    public void execute(Collection<Record<Event>> records);
    public void flushBuffer();
    public void pushDLQList();
    public void addEventToDLQList(final Event event, Throwable ex);
    public boolean addToBuffer(final Event event, final long estimatedSize) throws Exception;
    public boolean exceedsFlushTimeInterval();
    public boolean willExceedMaxRequestSizeBytes(final Event event, final long estimatedSize) throws Exception;
    public boolean exceedsMaxEventSizeThreshold(final long estimatedSize);
    public long getEstimatedSize(final Event event) throws Exception;
    public void lock();
    public void unlock();

}


