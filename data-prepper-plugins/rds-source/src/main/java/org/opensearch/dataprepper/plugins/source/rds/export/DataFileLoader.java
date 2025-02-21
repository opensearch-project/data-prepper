/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHelper;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHelper;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

public class DataFileLoader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileLoader.class);

    static final Duration VERSION_OVERLAP_TIME_FOR_EXPORT = Duration.ofMinutes(5);
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;
    static final String EXPORT_RECORDS_TOTAL_COUNT = "exportRecordsTotal";
    static final String EXPORT_RECORDS_PROCESSED_COUNT = "exportRecordsProcessed";
    static final String EXPORT_RECORDS_PROCESSING_ERROR_COUNT = "exportRecordsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";

    private final DataFilePartition dataFilePartition;
    private final String bucket;
    private final String objectKey;
    private final S3ObjectReader objectReader;
    private final InputCodec codec;
    private final Buffer<Record<Event>> buffer;
    private final ExportRecordConverter recordConverter;
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final AcknowledgementSet acknowledgementSet;
    private final Duration acknowledgmentTimeout;
    private final DbTableMetadata dbTableMetadata;
    private final Counter exportRecordsTotalCounter;
    private final Counter exportRecordSuccessCounter;
    private final Counter exportRecordErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    private DataFileLoader(final DataFilePartition dataFilePartition,
                           final InputCodec codec,
                           final Buffer<Record<Event>> buffer,
                           final S3ObjectReader objectReader,
                           final ExportRecordConverter recordConverter,
                           final PluginMetrics pluginMetrics,
                           final EnhancedSourceCoordinator sourceCoordinator,
                           final AcknowledgementSet acknowledgementSet,
                           final Duration acknowledgmentTimeout,
                           final DbTableMetadata dbTableMetadata) {
        this.dataFilePartition = dataFilePartition;
        bucket = dataFilePartition.getBucket();
        objectKey = dataFilePartition.getKey();
        this.objectReader = objectReader;
        this.codec = codec;
        this.buffer = buffer;
        this.recordConverter = recordConverter;
        this.sourceCoordinator = sourceCoordinator;
        this.acknowledgementSet = acknowledgementSet;
        this.acknowledgmentTimeout = acknowledgmentTimeout;
        this.dbTableMetadata = dbTableMetadata;

        exportRecordsTotalCounter = pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT);
        exportRecordSuccessCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT);
        exportRecordErrorCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
    }

    public static DataFileLoader create(final DataFilePartition dataFilePartition,
                                        final InputCodec codec,
                                        final Buffer<Record<Event>> buffer,
                                        final S3ObjectReader objectReader,
                                        final ExportRecordConverter recordConverter,
                                        final PluginMetrics pluginMetrics,
                                        final EnhancedSourceCoordinator sourceCoordinator,
                                        final AcknowledgementSet acknowledgementSet,
                                        final Duration acknowledgmentTimeout,
                                        final DbTableMetadata dbTableMetadata) {
        return new DataFileLoader(dataFilePartition, codec, buffer, objectReader, recordConverter,
                pluginMetrics, sourceCoordinator, acknowledgementSet, acknowledgmentTimeout, dbTableMetadata);
    }

    @Override
    public void run() {
        LOG.info(SENSITIVE, "Start loading s3://{}/{}", bucket, objectKey);

        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);

        AtomicLong eventCount = new AtomicLong();
        try (InputStream inputStream = objectReader.readFile(bucket, objectKey)) {
            codec.parse(inputStream, record -> {
                try {
                    exportRecordsTotalCounter.increment();
                    final Event event = record.getData();
                    final String string = event.toJsonString();
                    final long bytes = string.getBytes().length;
                    bytesReceivedSummary.record(bytes);

                    DataFileProgressState progressState = dataFilePartition.getProgressState().get();

                    final String fullTableName = progressState.getFullSourceTableName();
                    final List<String> primaryKeys = progressState.getPrimaryKeyMap().getOrDefault(fullTableName, List.of());
                    transformEvent(event, fullTableName, EngineType.fromString(progressState.getEngineType()));

                    final long snapshotTime = progressState.getSnapshotTime();
                    final long eventVersionNumber = snapshotTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis();
                    final Event transformedEvent = recordConverter.convert(
                            event,
                            progressState.getSourceDatabase(),
                            progressState.getSourceSchema(),
                            progressState.getSourceTable(),
                            OpenSearchBulkActions.INDEX,
                            primaryKeys,
                            snapshotTime,
                            eventVersionNumber,
                            null);

                    if (acknowledgementSet != null) {
                        acknowledgementSet.add(transformedEvent);
                    }

                    bufferAccumulator.add(new Record<>(transformedEvent));
                    eventCount.getAndIncrement();
                    bytesProcessedSummary.record(bytes);
                } catch (Exception e) {
                    LOG.error(SENSITIVE, "Failed to process record from object s3://{}/{}", bucket, objectKey, e);
                    throw new RuntimeException(e);
                }
            });

            LOG.info(SENSITIVE, "Completed loading object s3://{}/{} to buffer", bucket, objectKey);
        } catch (Exception e) {
            LOG.error(SENSITIVE, "Failed to load object s3://{}/{} to buffer", bucket, objectKey, e);
            throw new RuntimeException(e);
        }

        try {
            bufferAccumulator.flush();
            if (acknowledgementSet != null) {
                sourceCoordinator.saveProgressStateForPartition(dataFilePartition, acknowledgmentTimeout);
                acknowledgementSet.complete();
            }
            exportRecordSuccessCounter.increment(eventCount.get());
        } catch (Exception e) {
            LOG.error("Failed to write events to buffer", e);
            exportRecordErrorCounter.increment(eventCount.get());
        }
    }

    private void transformEvent(final Event event, final String fullTableName, final EngineType engineType) {
        if (engineType.isMySql()) {
            Map<String, String> columnDataTypeMap = dbTableMetadata.getTableColumnDataTypeMap().get(fullTableName);
            for (Map.Entry<String, Object> entry : event.toMap().entrySet()) {
                final Object data = MySQLDataTypeHelper.getDataByColumnType(MySQLDataType.byDataType(columnDataTypeMap.get(entry.getKey())), entry.getKey(),
                        entry.getValue(), null);
                event.put(entry.getKey(), data);
            }
        }
        if (engineType.isPostgres()) {
            Map<String, String> columnDataTypeMap = dbTableMetadata.getTableColumnDataTypeMap().get(fullTableName);
            for (Map.Entry<String, Object> entry : event.toMap().entrySet()) {
                final Object data = PostgresDataTypeHelper.getDataByColumnType(PostgresDataType.byDataType(columnDataTypeMap.get(entry.getKey())), entry.getKey(),
                        entry.getValue());
                event.put(entry.getKey(), data);
            }
        }
    }
}
