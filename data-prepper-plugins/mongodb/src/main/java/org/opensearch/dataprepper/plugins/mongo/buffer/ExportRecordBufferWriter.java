package org.opensearch.dataprepper.plugins.mongo.buffer;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class ExportRecordBufferWriter extends RecordBufferWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ExportRecordBufferWriter.class);
    static final Duration VERSION_OVERLAP_TIME_FOR_EXPORT = Duration.ofMinutes(5);

    static final String EXPORT_RECORDS_PROCESSED_COUNT = "exportRecordsProcessed";
    static final String EXPORT_RECORDS_PROCESSING_ERROR_COUNT = "exportRecordProcessingErrors";
    private final PluginMetrics pluginMetrics;
    private final Counter exportRecordSuccessCounter;
    private final Counter exportRecordErrorCounter;

    private ExportRecordBufferWriter(final BufferAccumulator<Record<Event>> bufferAccumulator,
                                    final PluginMetrics pluginMetrics) {
        super(bufferAccumulator);
        this.pluginMetrics = pluginMetrics;
        this.exportRecordSuccessCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT);
        this.exportRecordErrorCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT);
    }

    public static ExportRecordBufferWriter create(final BufferAccumulator<Record<Event>> bufferAccumulator,
                                                  final PluginMetrics pluginMetrics) {
        return new ExportRecordBufferWriter(bufferAccumulator, pluginMetrics);
    }

    @Override
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
            exportRecordSuccessCounter.increment(eventCount);
        } catch (Exception e) {
            LOG.error("Failed to write {} events to buffer due to {}", eventCount, e.getMessage());
            exportRecordErrorCounter.increment(eventCount);
        }
    }
}
