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
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.plugins.source.dynamodb.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * One loader per file.
 */
public class DataFileLoader implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileLoader.class);

    /**
     * A flag to interrupt the process
     */
    private static volatile boolean shouldStop = false;

    /**
     * Number of lines to be read in a batch
     */
    private static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Default regular checkpoint interval
     */
    private static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 2 * 60_000;

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final String bucketName;

    private final String key;

    private final ExportRecordConverter recordConverter;

    private final S3ObjectReader objectReader;

    private final DataFileCheckpointer checkpointer;

    /**
     * Start Line is the checkpoint
     */
    private final int startLine;

    private final AcknowledgementSet acknowledgementSet;

    private final Duration dataFileAcknowledgmentTimeout;

    private DataFileLoader(Builder builder) {
        this.objectReader = builder.objectReader;
        this.bucketName = builder.bucketName;
        this.key = builder.key;
        this.checkpointer = builder.checkpointer;
        this.startLine = builder.startLine;
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(builder.buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        recordConverter = new ExportRecordConverter(bufferAccumulator, builder.tableInfo, builder.pluginMetrics);
        this.acknowledgementSet = builder.acknowledgementSet;
        this.dataFileAcknowledgmentTimeout = builder.dataFileAcknowledgmentTimeout;
    }

    public static Builder builder(final S3ObjectReader s3ObjectReader, final PluginMetrics pluginMetrics, final Buffer<Record<Event>> buffer) {
        return new Builder(s3ObjectReader, pluginMetrics, buffer);
    }


    /**
     * Default Builder for DataFileLoader
     */
    static class Builder {

        private final S3ObjectReader objectReader;

        private final PluginMetrics pluginMetrics;

        private final Buffer<Record<Event>> buffer;

        private TableInfo tableInfo;


        private DataFileCheckpointer checkpointer;

        private String bucketName;

        private String key;

        private AcknowledgementSet acknowledgementSet;

        private Duration dataFileAcknowledgmentTimeout;

        private int startLine;

        public Builder(final S3ObjectReader objectReader, final PluginMetrics pluginMetrics, final Buffer<Record<Event>> buffer) {
            this.objectReader = objectReader;
            this.pluginMetrics = pluginMetrics;
            this.buffer = buffer;
        }

        public Builder tableInfo(TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public Builder checkpointer(DataFileCheckpointer checkpointer) {
            this.checkpointer = checkpointer;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder startLine(int startLine) {
            this.startLine = startLine;
            return this;
        }

        public Builder acknowledgmentSet(AcknowledgementSet acknowledgementSet) {
            this.acknowledgementSet = acknowledgementSet;
            return this;
        }

        public Builder acknowledgmentSetTimeout(Duration dataFileAcknowledgmentTimeout) {
            this.dataFileAcknowledgmentTimeout = dataFileAcknowledgmentTimeout;
            return this;
        }

        public DataFileLoader build() {
            return new DataFileLoader(this);
        }

    }


    @Override
    public void run() {
        LOG.info("Start loading s3://{}/{} with start line {}", bucketName, key, startLine);
        long lastCheckpointTime = System.currentTimeMillis();
        List<String> lines = new ArrayList<>();

        // line count regardless the start line number
        int lineCount = 0;
        int lastLineProcessed = 0;

        try {
            InputStream inputStream = objectReader.readFile(bucketName, key);
            GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
            BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream));

            String line;
            while ((line = reader.readLine()) != null) {

                if (shouldStop) {
                    checkpointer.checkpoint(lastLineProcessed);
                    LOG.debug("Should Stop flag is set to True, looks like shutdown has triggered");
                    throw new RuntimeException("Load is interrupted");
                }

                lineCount += 1;

                // process each line
                if (lineCount <= startLine) {
                    continue;
                }
                lines.add(line);

                if ((lineCount - startLine) % DEFAULT_BATCH_SIZE == 0) {
                    // LOG.debug("Write to buffer for line " + (lineCount - DEFAULT_BATCH_SIZE) + " to " + lineCount);
                    recordConverter.writeToBuffer(acknowledgementSet, lines);
                    lines.clear();
                    lastLineProcessed = lineCount;
                }

                if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                    LOG.debug("Perform regular checkpointing for Data File Loader");
                    checkpointer.checkpoint(lastLineProcessed);
                    lastCheckpointTime = System.currentTimeMillis();

                }

            }
            if (!lines.isEmpty()) {
                // Do final checkpoint.
                recordConverter.writeToBuffer(acknowledgementSet, lines);
                checkpointer.checkpoint(lineCount);
            }

            lines.clear();
            reader.close();
            gzipInputStream.close();
            inputStream.close();

            LOG.info("Completed loading s3://{}/{} to buffer", bucketName, key);

            if (acknowledgementSet != null) {
                checkpointer.updateDatafileForAcknowledgmentWait(dataFileAcknowledgmentTimeout);
                acknowledgementSet.complete();
            }
        } catch (Exception e) {
            checkpointer.checkpoint(lineCount);

            String errorMessage = String.format("Loading of s3://%s/%s completed with Exception: %S", bucketName, key, e.getMessage());
            throw new RuntimeException(errorMessage);
        }
    }

    /**
     * Currently, this is to stop all consumers.
     */
    public static void stopAll() {
        shouldStop = true;
    }
}
