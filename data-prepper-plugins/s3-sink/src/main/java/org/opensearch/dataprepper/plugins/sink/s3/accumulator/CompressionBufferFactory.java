/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Objects;
import java.util.function.Supplier;

public class CompressionBufferFactory implements BufferFactory {
    private final BufferFactory innerBufferFactory;
    private final CompressionEngine compressionEngine;
    private final boolean compressionInternal;

    public CompressionBufferFactory(final BufferFactory innerBufferFactory, final CompressionEngine compressionEngine, final OutputCodec codec) {
        this.innerBufferFactory = Objects.requireNonNull(innerBufferFactory);
        this.compressionEngine = Objects.requireNonNull(compressionEngine);
        compressionInternal = Objects.requireNonNull(codec).isCompressionInternal();
    }

    @Override
    public Buffer getBuffer(final S3Client s3Client,
                            final Supplier<String> bucketSupplier,
                            final Supplier<String> keySupplier,
                            final String defaultBucket) {
        final Buffer internalBuffer = innerBufferFactory.getBuffer(s3Client, bucketSupplier, keySupplier, defaultBucket);
        if(compressionInternal)
            return internalBuffer;

        return new CompressionBuffer(internalBuffer, compressionEngine);
    }
}
