/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A buffer can hold local file data and flushing it to S3.
 */
public class LocalFileBuffer implements Buffer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileBuffer.class);
    private final OutputStream outputStream;
    private final S3Client s3Client;
    private final Supplier<String> bucketSupplier;
    private final Supplier<String> keySupplier;
    private int eventCount;
    private final StopWatch watch;
    private final File localFile;
    private boolean isCodecStarted;
    private String bucket;
    private String key;

    private String defaultBucket;


    LocalFileBuffer(final File tempFile,
                    final S3Client s3Client,
                    final Supplier<String> bucketSupplier,
                    final Supplier<String> keySupplier,
                    final String defaultBucket) throws FileNotFoundException {
        localFile = tempFile;
        outputStream = new BufferedOutputStream(new FileOutputStream(tempFile), 32 * 1024);
        this.s3Client = s3Client;
        this.bucketSupplier = bucketSupplier;
        this.keySupplier = keySupplier;
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        isCodecStarted = false;
        this.defaultBucket = defaultBucket;
    }

    @Override
    public long getSize() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            LOG.error("An exception occurred while flushing data to buffered output stream :", e);
        }
        return localFile.length();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    @Override
    public Duration getDuration(){
        return Duration.ofMillis(watch.getTime(TimeUnit.MILLISECONDS));
    }

    /**
     * Upload accumulated data to amazon s3.
     */
    @Override
    public void flushToS3() {
        flushAndCloseStream();
        BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, RequestBody.fromFile(localFile), getKey(), getBucket(), defaultBucket);
        removeTemporaryFile();
    }

    /**
     * Flushing the buffered data into the output stream.
     */
    protected void flushAndCloseStream(){
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.error("An exception occurred while flushing data to buffered output stream :", e);
        }
    }

    /**
     * Remove the local temp file after flushing data to s3.
     */
    protected void removeTemporaryFile() {
        if (localFile != null) {
            try {
                Files.deleteIfExists(Paths.get(localFile.toString()));
            } catch (IOException e) {
                LOG.error("Unable to delete Local file {}", localFile, e);
            }
        }
    }

    @Override
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    @Override
    public OutputStream getOutputStream() {
        return outputStream;
    }


    private String getBucket() {
        if(bucket == null)
            bucket = bucketSupplier.get();
        return bucket;
    }

    @Override
    public String getKey() {
        if(key == null)
            key = keySupplier.get();
        return key;
    }
}