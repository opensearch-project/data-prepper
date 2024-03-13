package org.opensearch.dataprepper.plugins.mongo.buffer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.RecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class ExportRecordBufferWriter extends RecordBufferWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ExportRecordBufferWriter.class);
    static final Duration VERSION_OVERLAP_TIME_FOR_EXPORT = Duration.ofMinutes(5);

    static final String EXPORT_RECORDS_PROCESSED_COUNT = "exportRecordsProcessed";
    static final String EXPORT_RECORDS_PROCESSING_ERROR_COUNT = "exportRecordProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";
    private final PluginMetrics pluginMetrics;
    private final long exportStartTime;
    private final Counter exportRecordSuccessCounter;
    private final Counter exportRecordErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    public ExportRecordBufferWriter(final BufferAccumulator<Record<Event>> bufferAccumulator,
                                    final CollectionConfig collectionConfig,
                                    final RecordConverter recordConverter,
                                    final PluginMetrics pluginMetrics,
                                    final long exportStartTime) {
        super(bufferAccumulator, collectionConfig, recordConverter);
        this.pluginMetrics = pluginMetrics;
        this.exportRecordSuccessCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT);
        this.exportRecordErrorCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT);
        this.bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        this.bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        this.exportStartTime = exportStartTime;
    }

    @Override
    public void writeToBuffer(final AcknowledgementSet acknowledgementSet,
                              final List<String> records) {

        int eventCount = 0;
        for (final String record : records) {
            final long bytes = record.getBytes().length;
            bytesReceivedSummary.record(bytes);
            try {
                // The version number is the export time minus some overlap to ensure new stream events still get priority
                final long eventVersionNumber = (exportStartTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis()) * 1_000;
                addToBuffer(acknowledgementSet, record, exportStartTime, eventVersionNumber);
                bytesProcessedSummary.record(bytes);
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
