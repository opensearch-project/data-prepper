/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.dataprepper.plugins.codec.parquet.S3OutputStream;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MultipartBuffer implements Buffer {

    private int eventCount;
    private final StopWatch watch;
    private boolean isCodecStarted;
    private S3OutputStream s3OutputStream;

    MultipartBuffer(S3OutputStream s3OutputStream) {
        this.s3OutputStream = s3OutputStream;
        eventCount = 0;
        watch = new StopWatch();
        watch.start();
        isCodecStarted = false;
    }

    @Override
    public long getSize() {
        try {
            return s3OutputStream.getPos();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
    public Optional<CompletableFuture<?>> flushToS3(final Consumer<Boolean> runOnCompletion, final Consumer<Throwable> runOnFailure) {
        return Optional.ofNullable(s3OutputStream.close(runOnCompletion, runOnFailure));
    }

    @Override
    public void setEventCount(int eventCount) {
        this.eventCount = eventCount;
    }

    @Override
    public String getKey() {
        return s3OutputStream.getKey();
    }

    @Override
    public S3OutputStream getOutputStream() {
        return s3OutputStream;
    }
}
