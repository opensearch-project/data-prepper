/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

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
    public long getDuration() {
        return innerBuffer.getDuration();
    }

    @Override
    public void flushToS3() {
        innerBuffer.flushToS3();
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
