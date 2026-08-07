/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class DefaultSinkOutputStrategy implements SinkBufferEntryProvider, SinkDlqHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSinkOutputStrategy.class);
    private final LockStrategy lockStrategy;
    private final SinkBuffer sinkBuffer;
    private final SinkMetrics sinkMetrics;
    private final SinkFlushContext sinkFlushContext;

    public DefaultSinkOutputStrategy(final LockStrategy lockStrategy, final SinkBuffer sinkBuffer, final SinkFlushContext sinkFlushContext, final SinkMetrics sinkMetrics) {
        this.lockStrategy = lockStrategy;
        this.sinkBuffer = sinkBuffer;
        this.sinkMetrics = sinkMetrics;
        this.sinkFlushContext = sinkFlushContext;
    }

    public void flushBuffer() {
        long startTime = System.nanoTime();
        // getFlushableBuffer() should return the buffer contents
        SinkFlushableBuffer flushableBuffer = sinkBuffer.getFlushableBuffer(sinkFlushContext);
        if (flushableBuffer == null) {
            return;
        }
        List<Event> events = flushableBuffer.getEvents();
        try {      
            SinkFlushResult flushResult = flushableBuffer.flush();
            if (flushResult == null) { // success
                sinkMetrics.recordRequestLatency(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                for (final Event event: events) {
                    event.getEventHandle().release(true);
                }
            } else {    
                // flush Result should contain the events that are 
                // failed to be delivered, so that these events can be forwarded to DLQ
                addFailedEventsToDlq(flushResult.getEvents(), flushResult.getException(), flushResult.getStatusCode());
            }           
        } catch (Exception e) {
            // Add list of events to DLQ
            sinkMetrics.incrementRequestsFailedCounter(1);
            sinkMetrics.incrementEventsFailedCounter(events.size());
            addFailedEventsToDlq(events, e, 0);
        }              
    }

    public void execute(Collection<Record<Event>> records) {
        lockStrategy.lock();
        try {
            if (sinkBuffer.exceedsFlushTimeInterval()) {
                flushBuffer();
            }
            if (records == null || records.isEmpty()) {
                return;
            }
            for (Record<Event> record : records) {
                final Event event = record.getData();
                try {
                    // getSinkBufferEntry() is a sink method that may use codec to get
                    // the estimated size of the event
                    SinkBufferEntry bufferEntry = getSinkBufferEntry(event);

                    // Check if individual event exceeds sink's max event size
                    if (bufferEntry.exceedsMaxEventSizeThreshold()) {
                        throw new RuntimeException("Event size exceeds max allowed event size");
                    }

                    // Check if adding this event to the batch buffer, would exceed the batch
                    // buffer's max bytes threshold, if yes, flush the batch buffer before adding
                    // new buffer entry
                    if (sinkBuffer.willExceedMaxRequestSizeBytes(bufferEntry)) {
                        flushBuffer();
                    }

                    if (!sinkBuffer.addToBuffer(bufferEntry)) {
                        throw new RuntimeException("Failed to add event to sink buffer");
                    }

                    // Check if after adding the event, max events in a batch threshold exceeded
                    // If yes, flush the batch buffer
                    if (sinkBuffer.isMaxEventsLimitReached()) {
                        flushBuffer();
                    } 
                } catch (Exception ex) {
                    LOG.warn(NOISY, "Failed process the event ", ex);
                    addFailedEventsToDlq(List.of(event), ex, 0);
                }
            }   
        } finally {     
            flushDlqList();
            lockStrategy.unlock();
        }                   
    }

}
