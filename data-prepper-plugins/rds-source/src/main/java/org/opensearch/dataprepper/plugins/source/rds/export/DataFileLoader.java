/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class DataFileLoader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileLoader.class);

    private final DataFilePartition dataFilePartition;
    private final String bucket;
    private final String objectKey;
    private final S3ObjectReader objectReader;
    private final InputCodec codec;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final ExportRecordConverter recordConverter;

    private DataFileLoader(final DataFilePartition dataFilePartition,
                   final InputCodec codec,
                   final BufferAccumulator<Record<Event>> bufferAccumulator,
                   final S3ObjectReader objectReader,
                   final ExportRecordConverter recordConverter) {
        this.dataFilePartition = dataFilePartition;
        bucket = dataFilePartition.getBucket();
        objectKey = dataFilePartition.getKey();
        this.objectReader = objectReader;
        this.codec = codec;
        this.bufferAccumulator = bufferAccumulator;
        this.recordConverter = recordConverter;
    }

    public static DataFileLoader create(final DataFilePartition dataFilePartition,
                                        final InputCodec codec,
                                        final BufferAccumulator<Record<Event>> bufferAccumulator,
                                        final S3ObjectReader objectReader,
                                        final ExportRecordConverter recordConverter) {
        return new DataFileLoader(dataFilePartition, codec, bufferAccumulator, objectReader, recordConverter);
    }

    @Override
    public void run() {
        LOG.info("Start loading s3://{}/{}", bucket, objectKey);

        try (InputStream inputStream = objectReader.readFile(bucket, objectKey)) {

            codec.parse(inputStream, record -> {
                try {
                    final String tableName = dataFilePartition.getProgressState().get().getSourceTable();
                    // TODO: primary key to be obtained by querying database schema
                    final String primaryKeyName = "id";
                    Record<Event> transformedRecord = new Record<>(recordConverter.convert(record, tableName, primaryKeyName));
                    bufferAccumulator.add(transformedRecord);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            LOG.info("Completed loading object s3://{}/{} to buffer", bucket, objectKey);
        } catch (Exception e) {
            LOG.error("Failed to load object s3://{}/{} to buffer", bucket, objectKey, e);
            throw new RuntimeException(e);
        }

        try {
            bufferAccumulator.flush();
        } catch (Exception e) {
            LOG.error("Failed to write events to buffer", e);
        }
    }
}
