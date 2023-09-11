/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A buffer can hold in memory data and flushing it to S3.
 */
public class InMemoryBuffer implements Buffer {

    private static final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private static final ByteArrayPositionOutputStream byteArrayPositionOutputStream = new ByteArrayPositionOutputStream(byteArrayOutputStream);
    private final S3Client s3Client;
    private final Supplier<String> bucketSupplier;
    private final Supplier<String> keySupplier;
    private int eventCount;
    private final StopWatch watch;
    private boolean isCodecStarted;
    private String bucket;
    private String key;

    InMemoryBuffer(S3Client s3Client, Supplier<String> bucketSupplier, Supplier<String> keySupplier) {
        this.s3Client = s3Client;
        this.bucketSupplier = bucketSupplier;
        this.keySupplier = keySupplier;
        byteArrayOutputStream.reset();
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        isCodecStarted = false;
    }

    @Override
    public long getSize() {
        return byteArrayOutputStream.size();
    }

    @Override
    public int getEventCount() {
        return eventCount;
    }

    public Duration getDuration() {
        return Duration.ofMillis(watch.getTime(TimeUnit.MILLISECONDS));
    }

    /**
     * Upload accumulated data to s3 bucket.
     */
    @Override
    public void flushToS3() {
        final byte[] byteArray = byteArrayOutputStream.toByteArray();
        s3Client.putObject(
                PutObjectRequest.builder().bucket(getBucket()).key(getKey()).build(),
                RequestBody.fromBytes(byteArray));
    }

    private String getBucket() {
        if(bucket == null)
            bucket = bucketSupplier.get();
        return bucket;
    }

    @Override
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    @Override
    public String getKey() {
        if(key == null)
            key = keySupplier.get();
        return key;
    }

    @Override
    public OutputStream getOutputStream() {
        return byteArrayPositionOutputStream;
    }
}