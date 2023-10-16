/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.opensearch.dataprepper.plugins.source.dynamodb.converter.ExportRecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

    private final String bucketName;

    private final String key;

    private final ExportRecordConverter recordConverter;

    private final S3ObjectReader s3ObjectReader;

    private final DataFileCheckpointer checkpointer;

    // Start Line is the checkpoint
    private final int startLine;

    private DataFileLoader(Builder builder) {
        this.s3ObjectReader = builder.s3ObjectReader;
        this.recordConverter = builder.recordConverter;
        this.bucketName = builder.bucketName;
        this.key = builder.key;
        this.checkpointer = builder.checkpointer;
        this.startLine = builder.startLine;
    }

    public static Builder builder() {
        return new Builder();
    }


    /**
     * Default Builder for DataFileLoader
     */
    static class Builder {

        private S3ObjectReader s3ObjectReader;

        private ExportRecordConverter recordConverter;

        private DataFileCheckpointer checkpointer;

        private String bucketName;

        private String key;

        private int startLine;

        public Builder s3ObjectReader(S3ObjectReader s3ObjectReader) {
            this.s3ObjectReader = s3ObjectReader;
            return this;
        }

        public Builder recordConverter(ExportRecordConverter recordConverter) {
            this.recordConverter = recordConverter;
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

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(s3ObjectReader.readFile(bucketName, key))) {
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
                    recordConverter.writeToBuffer(lines);
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
                recordConverter.writeToBuffer(lines);
                checkpointer.checkpoint(lineCount);
            }

            lines.clear();
            reader.close();
            LOG.info("Complete loading s3://{}/{}", bucketName, key);
        } catch (Exception e) {
            checkpointer.checkpoint(lineCount);
            String errorMessage = String.format("Loading of s3://{}/{} completed with Exception: {}", bucketName, key, e.getMessage());
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
