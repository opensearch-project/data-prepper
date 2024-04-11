/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.buffer;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import java.util.List;


/**
 * Base Record Buffer writer that transform the source data into a JacksonEvent,
 * and then writes to buffer.
 */
public abstract class RecordBufferWriter {
    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    public RecordBufferWriter(final BufferAccumulator<Record<Event>> bufferAccumulator) {
        this.bufferAccumulator = bufferAccumulator;
    }

    public abstract void writeToBuffer(final AcknowledgementSet acknowledgementSet,
                                       final List<Event> records);

    void flushBuffer() throws Exception {
        bufferAccumulator.flush();
    }

    /**
     * Add event record to buffer
     *
     * @param acknowledgementSet      acknowledgmentSet keeps track of set of events
     * @param record                  record to be written to buffer
     * @throws Exception Exception if failed to write to buffer.
     */
    public void addToBuffer(final AcknowledgementSet acknowledgementSet,
                            final Event record) throws Exception {
        if (acknowledgementSet != null) {
            acknowledgementSet.add(record);
        }

        bufferAccumulator.add(new Record<>(record));
    }
}
