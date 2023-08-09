/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;

import java.util.Objects;

public class CompressionBufferFactory implements BufferFactory {
    private final BufferFactory innerBufferFactory;
    private final CompressionEngine compressionEngine;

    public CompressionBufferFactory(final BufferFactory innerBufferFactory, final CompressionEngine compressionEngine) {
        this.innerBufferFactory = Objects.requireNonNull(innerBufferFactory);
        this.compressionEngine = Objects.requireNonNull(compressionEngine);
    }

    @Override
    public Buffer getBuffer() {
        return new CompressionBuffer(innerBufferFactory.getBuffer(), compressionEngine);
    }
}
