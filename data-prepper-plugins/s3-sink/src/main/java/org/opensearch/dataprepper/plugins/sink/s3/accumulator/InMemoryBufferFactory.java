/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.Map;
import java.util.function.Supplier;
import java.util.function.Function;

public class InMemoryBufferFactory implements BufferFactory {
    @Override
    public Buffer getBuffer(final S3AsyncClient s3Client,
                            final Supplier<String> bucketSupplier,
                            final Supplier<String> keySupplier,
                            final String defaultBucket,
                            final Function<Integer, Map<String, String>> metadataSupplier,
                            final BucketOwnerProvider bucketOwnerProvider) {
        return new InMemoryBuffer(s3Client, bucketSupplier, keySupplier, metadataSupplier, defaultBucket, bucketOwnerProvider);
    }
}
