/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.buffer;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * An implementation of {@link Buffer} which delegates all calls to a delgate
 * (or inner) buffer.
 * <p>
 * This class exists to help with writing decorators of the {@link Buffer} interface.
 *
 * @param <T> The type of data in the buffer
 *
 * @since 2.6
 */
public abstract class DelegatingBuffer<T extends Record<?>> implements Buffer<T> {
    private final Buffer<T> delegateBuffer;

    /**
     * Constructor for subclasses to use.
     *
     * @param delegateBuffer The delegate (or inner) buffer.
     *
     * @since 2.6
     */
    protected DelegatingBuffer(final Buffer<T> delegateBuffer) {
        this.delegateBuffer = Objects.requireNonNull(delegateBuffer);
    }

    @Override
    public void write(final T record, final int timeoutInMillis) throws TimeoutException {
        delegateBuffer.write(record, timeoutInMillis);
    }

    @Override
    public void writeAll(final Collection<T> records, final int timeoutInMillis) throws Exception {
        delegateBuffer.writeAll(records, timeoutInMillis);
    }

    @Override
    public void writeBytes(final byte[] bytes, final String key, final int timeoutInMillis) throws Exception {
        delegateBuffer.writeBytes(bytes, key, timeoutInMillis);
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(final int timeoutInMillis) {
        return delegateBuffer.read(timeoutInMillis);
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {
        delegateBuffer.checkpoint(checkpointState);
    }

    @Override
    public boolean isEmpty() {
        return delegateBuffer.isEmpty();
    }

    @Override
    public boolean isByteBuffer() {
        return delegateBuffer.isByteBuffer();
    }

    @Override
    public Duration getDrainTimeout() {
        return delegateBuffer.getDrainTimeout();
    }

    @Override
    public void shutdown() {
        delegateBuffer.shutdown();
    }
}
