package org.opensearch.dataprepper.plugins.mongo.buffer;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Record Buffer writer that transform the source data into a JacksonEvent,
 * and then writes to buffer.
 */
public class RecordBufferWriter {
    private static final Logger LOG = LoggerFactory.getLogger(RecordBufferWriter.class);
    static final String RECORDS_PROCESSED_COUNT = "recordsProcessed";
    static final String RECORDS_PROCESSING_ERROR_COUNT = "recordProcessingErrors";
    private final PluginMetrics pluginMetrics;
    private final Counter recordSuccessCounter;
    private final Counter recordErrorCounter;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    private RecordBufferWriter(final BufferAccumulator<Record<Event>> bufferAccumulator,
                               final PluginMetrics pluginMetrics) {
        this.bufferAccumulator = bufferAccumulator;
        this.pluginMetrics = pluginMetrics;
        this.recordSuccessCounter = pluginMetrics.counter(RECORDS_PROCESSED_COUNT);
        this.recordErrorCounter = pluginMetrics.counter(RECORDS_PROCESSING_ERROR_COUNT);
    }

    public static RecordBufferWriter create(final BufferAccumulator<Record<Event>> bufferAccumulator,
                                            final PluginMetrics pluginMetrics) {
        return new RecordBufferWriter(bufferAccumulator, pluginMetrics);
    }

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

    public void writeToBuffer(final AcknowledgementSet acknowledgementSet,
                              final List<Event> records) {

        int eventCount = 0;
        for (final Event record : records) {
            try {
                addToBuffer(acknowledgementSet, record);
                eventCount++;
            } catch (Exception e) {
                // will this cause too many logs?
                LOG.error("Failed to add event to buffer due to {}", e.getMessage());
            }
        }

        try {
            flushBuffer();
            recordSuccessCounter.increment(eventCount);
        } catch (Exception e) {
            LOG.error("Failed to write {} events to buffer due to {}", eventCount, e.getMessage());
            recordErrorCounter.increment(eventCount);
        }
    }
}
