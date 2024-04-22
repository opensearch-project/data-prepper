/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

class CompressionBuffer implements Buffer {
    private final Buffer innerBuffer;
    private final CompressionEngine compressionEngine;
    private volatile OutputStream outputStream;

    CompressionBuffer(final Buffer innerBuffer, final CompressionEngine compressionEngine) {
        this.innerBuffer = Objects.requireNonNull(innerBuffer);
        this.compressionEngine = Objects.requireNonNull(compressionEngine);
    }

    @Override
    public long getSize() {
        return innerBuffer.getSize();
    }

    @Override
    public int getEventCount() {
        return innerBuffer.getEventCount();
    }

    @Override
    public Duration getDuration() {
        return innerBuffer.getDuration();
    }

    @Override
    public Optional<CompletableFuture<?>> flushToS3(final Consumer<Boolean> runOnCompletion, final Consumer<Throwable> runOnException) {
       return innerBuffer.flushToS3(runOnCompletion, runOnException);
    }

    @Override
    public OutputStream getOutputStream() {
        if(outputStream == null) {
            synchronized (this) {
                if(outputStream == null) {
                    final OutputStream innerBufferOutputStream = innerBuffer.getOutputStream();
                    try {
                        outputStream = compressionEngine.createOutputStream(innerBufferOutputStream);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return outputStream;
    }

    @Override
    public void setEventCount(final int eventCount) {
        innerBuffer.setEventCount(eventCount);
    }

    @Override
    public String getKey() {
        return innerBuffer.getKey();
    }
}
