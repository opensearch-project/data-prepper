/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.buffer;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.RecordConverter;
import java.util.List;


/**
 * Base Record Buffer writer that transform the source data into a JacksonEvent,
 * and then writes to buffer.
 */
public abstract class RecordBufferWriter {
    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    private final CollectionConfig collectionConfig;
    private final RecordConverter recordConverter;

    public RecordBufferWriter(final BufferAccumulator<Record<Event>> bufferAccumulator,
                              final CollectionConfig collectionConfig,
                              final RecordConverter recordConverter) {
        this.bufferAccumulator = bufferAccumulator;
        this.collectionConfig = collectionConfig;
        this.recordConverter = recordConverter;
    }

    public abstract void writeToBuffer(final AcknowledgementSet acknowledgementSet,
                                       final List<String> records);

    void flushBuffer() throws Exception {
        bufferAccumulator.flush();
    }

    /**
     * Add event record to buffer
     *
     * @param acknowledgementSet      acknowledgmentSet keeps track of set of events
     * @param record                  record to be written to buffer
     * @param eventCreationTimeMillis Creation timestamp of the event
     * @param eventVersionNumber      Event version number to handle conflicts
     * @param eventName               Event name
     * @throws Exception Exception if failed to write to buffer.
     */
    public void addToBuffer(final AcknowledgementSet acknowledgementSet,
                            final String record,
                            final long eventCreationTimeMillis,
                            final long eventVersionNumber,
                            final String eventName) throws Exception {
        final Event event = recordConverter.convert(record, eventCreationTimeMillis, eventVersionNumber, eventName);

        if (acknowledgementSet != null) {
            acknowledgementSet.add(event);
        }

        bufferAccumulator.add(new Record<>(event));
    }

    /**
     * Add event record to buffer
     *
     * @param acknowledgementSet      acknowledgmentSet keeps track of set of events
     * @param record                  record to be written to buffer
     * @param eventCreationTimeMillis Creation timestamp of the event
     * @param eventVersionNumber      Event version number to handle conflicts
     * @throws Exception Exception if failed to write to buffer.
     */
    public void addToBuffer(final AcknowledgementSet acknowledgementSet,
                            final String record,
                            final long eventCreationTimeMillis,
                            final long eventVersionNumber) throws Exception {
        addToBuffer(acknowledgementSet, record, eventCreationTimeMillis, eventVersionNumber, null);
    }
}
