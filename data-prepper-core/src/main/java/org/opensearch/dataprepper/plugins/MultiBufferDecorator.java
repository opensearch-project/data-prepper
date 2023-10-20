/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Buffer decorator created for pipelines that make use of multiple buffers, such as PeerForwarder-enabled pipelines. The decorator
 * acts as a pass-through to the primary buffer for all methods except isEmpty, which verifies that all buffers are empty.
 *
 * @since 2.0
 */
public class MultiBufferDecorator<T extends Record<?>> implements Buffer<T> {
    private final Buffer primaryBuffer;
    private final List<Buffer> secondaryBuffers;

    public MultiBufferDecorator(final Buffer primaryBuffer, final List<Buffer> secondaryBuffers) {
        this.primaryBuffer = primaryBuffer;
        this.secondaryBuffers = secondaryBuffers;
    }

    @Override
    public void write(final T record, final int timeoutInMillis) throws TimeoutException {
        primaryBuffer.write(record, timeoutInMillis);
    }

    @Override
    public void writeAll(final Collection<T> records, final int timeoutInMillis) throws Exception {
        primaryBuffer.writeAll(records, timeoutInMillis);
    }

    @Override
    public void writeBytes(final byte[] bytes, final String key, int timeoutInMillis) throws Exception {
        primaryBuffer.writeBytes(bytes, key, timeoutInMillis);
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(final int timeoutInMillis) {
        return primaryBuffer.read(timeoutInMillis);
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {
        primaryBuffer.checkpoint(checkpointState);
    }

    @Override
    public boolean isByteBuffer() {
        return primaryBuffer.isByteBuffer();
    }

    @Override
    public boolean isEmpty() {
        return primaryBuffer.isEmpty() && secondaryBuffers.stream()
                .map(Buffer::isEmpty)
                .allMatch(result -> result == true);
    }

    @Override
    public Duration getDrainTimeout() {
        return Stream.concat(Stream.of(primaryBuffer), secondaryBuffers.stream())
                .map(Buffer::getDrainTimeout)
                .reduce(Duration.ZERO, Duration::plus);
    }

    @Override
    public void shutdown() {
        primaryBuffer.shutdown();
        secondaryBuffers.forEach(Buffer::shutdown);
    }
}
