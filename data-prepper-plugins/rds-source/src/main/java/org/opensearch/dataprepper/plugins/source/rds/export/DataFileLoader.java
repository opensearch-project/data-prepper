/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.parquet.ParquetInputCodec;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.InputStream;
import java.time.Duration;

public class DataFileLoader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileLoader.class);
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final DataFilePartition dataFilePartition;
    private final String bucket;
    private final String objectKey;
    private final S3ObjectReader objectReader;
    private final InputCodec codec;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    public DataFileLoader(final DataFilePartition dataFilePartition,
                          final S3Client s3Client,
                          final EventFactory eventFactory,
                          final Buffer<Record<Event>> buffer) {
        this.dataFilePartition = dataFilePartition;
        bucket = dataFilePartition.getBucket();
        objectKey = dataFilePartition.getKey();
        objectReader = new S3ObjectReader(s3Client);
        codec = new ParquetInputCodec(eventFactory);
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
    }

    @Override
    public void run() {
        LOG.info("Start loading s3://{}/{}", bucket, objectKey);

        try (InputStream inputStream = objectReader.readFile(bucket, objectKey)) {

            codec.parse(inputStream, record -> {
                try {
                    bufferAccumulator.add(record);
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
