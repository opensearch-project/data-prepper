/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NavigableSet;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Implements the local file buffer type.
 */
public class LocalFileBuffer implements BufferType {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBuffer.class);
    private S3Client s3Client;
    private S3SinkConfig s3SinkConfig;
    private File fileAbsolutePath;

    public LocalFileBuffer() {
    }

    /**
     * @param s3Client
     * @param s3SinkConfig
     */
    public LocalFileBuffer(final S3Client s3Client, final S3SinkConfig s3SinkConfig) {
        this.s3Client = s3Client;
        this.s3SinkConfig = s3SinkConfig;
    }

    /**
     * @param bufferedEventSet
     * @return boolean
     * @throws InterruptedException
     */
    public boolean localFileAccumulate(final NavigableSet<String> bufferedEventSet) throws InterruptedException {
        boolean isFileUploadedToS3 = Boolean.FALSE;
        String s3ObjectFileName = ObjectKey.objectFileName(s3SinkConfig);
        File file = new File(s3ObjectFileName);
        try (BufferedWriter eventWriter = new BufferedWriter(new FileWriter(s3ObjectFileName))) {
            for (String event : bufferedEventSet) {
                eventWriter.write(event);
            }
            fileAbsolutePath = file.getAbsoluteFile();
            eventWriter.flush();
            isFileUploadedToS3 = uploadToAmazonS3(s3SinkConfig, s3Client, RequestBody.fromFile(fileAbsolutePath));
        } catch (IOException e) {
            LOG.error("Events unable to save into : {}", s3SinkConfig.getBufferType(), e);
        } finally {
            removeTemporaryFile();
        }
        return isFileUploadedToS3;
    }

    /**
     * Remove local file after successfully upload to Amazon s3 bucket.
     */
    private void removeTemporaryFile() {
        if (fileAbsolutePath != null) {
            try {
                boolean isLocalFileDeleted = Files.deleteIfExists(Paths.get(fileAbsolutePath.toString()));
                if (isLocalFileDeleted) {
                    LOG.info("Local file deleted successfully {}", fileAbsolutePath);
                } else {
                    LOG.warn("Local file not deleted {}", fileAbsolutePath);
                }
            } catch (IOException e) {
                LOG.error("Local file unable to deleted {}", fileAbsolutePath, e);
            }
        }
    }
}