/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.DelegatingBuffer;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Buffer decorator created for pipelines that make use of multiple buffers, such as PeerForwarder-enabled pipelines. The decorator
 * acts as a pass-through to the primary buffer for most methods except those that rely on a combination of the primary
 * and second. For example, isEmpty depends on all buffers being empty.
 *
 * @since 2.0
 */
class MultiBufferDecorator<T extends Record<?>> extends DelegatingBuffer<T> implements Buffer<T> {
    private final List<Buffer> allBuffers;

    MultiBufferDecorator(final Buffer primaryBuffer, final List<Buffer> secondaryBuffers) {
        super(primaryBuffer);
        allBuffers = new ArrayList<>(1 + secondaryBuffers.size());
        allBuffers.add(primaryBuffer);
        allBuffers.addAll(secondaryBuffers);
    }

    @Override
    public boolean isEmpty() {
        return allBuffers.stream().allMatch(Buffer::isEmpty);
    }

    @Override
    public Duration getDrainTimeout() {
        return allBuffers.stream()
                .map(Buffer::getDrainTimeout)
                .reduce(Duration.ZERO, Duration::plus);
    }

    @Override
    public Optional<Integer> getMaxRequestSize() {
        OptionalInt maxRequestSize = allBuffers.stream().filter(b -> b.getMaxRequestSize().isPresent()).mapToInt(b -> (Integer)b.getMaxRequestSize().get()).min();
        return  maxRequestSize.isPresent()  ? Optional.of(maxRequestSize.getAsInt()) : Optional.empty();
    }

    @Override
    public void shutdown() {
        allBuffers.forEach(Buffer::shutdown);
    }
}
