/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

/**
 * Factory class for DataFileLoader thread.
 */
public class DataFileLoaderFactory {
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final EnhancedSourceCoordinator coordinator;

    private final S3ObjectReader objectReader;
    private final PluginMetrics pluginMetrics;
    private final Buffer<Record<Event>> buffer;

    public DataFileLoaderFactory(EnhancedSourceCoordinator coordinator, S3Client s3Client, PluginMetrics pluginMetrics, final Buffer<Record<Event>> buffer) {
        this.coordinator = coordinator;
        this.pluginMetrics = pluginMetrics;
        this.buffer = buffer;
        objectReader = new S3ObjectReader(s3Client);
    }

    public Runnable createDataFileLoader(DataFilePartition dataFilePartition, TableInfo tableInfo) {
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        ExportRecordConverter recordProcessor = new ExportRecordConverter(bufferAccumulator, tableInfo, pluginMetrics);

        DataFileCheckpointer checkpointer = new DataFileCheckpointer(coordinator, dataFilePartition);

        // Start a data loader thread.
        DataFileLoader loader = DataFileLoader.builder()
                .s3ObjectReader(objectReader)
                .bucketName(dataFilePartition.getBucket())
                .key(dataFilePartition.getKey())
                .recordConverter(recordProcessor)
                .checkpointer(checkpointer)
                .startLine(dataFilePartition.getProgressState().get().getLoaded())
                .build();

        return loader;
    }

}
