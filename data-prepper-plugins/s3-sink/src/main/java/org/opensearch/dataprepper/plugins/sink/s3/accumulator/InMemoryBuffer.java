/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.Function;

/**
 * A buffer can hold in memory data and flushing it to S3.
 */
public class InMemoryBuffer implements Buffer {

    private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    private final ByteArrayPositionOutputStream byteArrayPositionOutputStream = new ByteArrayPositionOutputStream(byteArrayOutputStream);
    private final S3AsyncClient s3Client;
    private final Supplier<String> bucketSupplier;
    private final Supplier<String> keySupplier;
    private final Function<Integer, Map<String,String>> metadataSupplier;

    private final BucketOwnerProvider bucketOwnerProvider;
    private int eventCount;
    private final StopWatch watch;
    private boolean isCodecStarted;
    private String bucket;
    private String key;

    private String defaultBucket;

    InMemoryBuffer(final S3AsyncClient s3Client,
                   final Supplier<String> bucketSupplier,
                   final Supplier<String> keySupplier,
                   final Function<Integer, Map<String, String>> metadataSupplier,
                   final String defaultBucket,
                   final BucketOwnerProvider bucketOwnerProvider) {
        this.s3Client = s3Client;
        this.bucketSupplier = bucketSupplier;
        this.keySupplier = keySupplier;
        this.metadataSupplier = metadataSupplier;
        byteArrayOutputStream.reset();
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        isCodecStarted = false;
        this.defaultBucket = defaultBucket;
        this.bucketOwnerProvider = bucketOwnerProvider;
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
    public Optional<CompletableFuture<?>> flushToS3(final Consumer<Boolean> consumeOnCompletion, final Consumer<Throwable> consumeOnException) {
        final byte[] byteArray = byteArrayOutputStream.toByteArray();
        return Optional.ofNullable(BufferUtilities.putObjectOrSendToDefaultBucket(s3Client, AsyncRequestBody.fromBytes(byteArray),
                consumeOnCompletion, consumeOnException,
                getKey(), getBucket(), defaultBucket, getMetadata(getEventCount()), bucketOwnerProvider));
    }

    private String getBucket() {
        if(bucket == null)
            bucket = bucketSupplier.get();
        return bucket;
    }

    private Map<String, String> getMetadata(int eventCount) {
        if (metadataSupplier != null) {
            return metadataSupplier.apply(getEventCount());
        } else {
            return Map.of();
        }
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
