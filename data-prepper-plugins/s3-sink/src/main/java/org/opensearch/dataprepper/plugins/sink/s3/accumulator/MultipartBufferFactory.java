/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.codec.parquet.S3OutputStream;
import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.function.Supplier;

public class MultipartBufferFactory implements BufferFactory {
    @Override
    public Buffer getBuffer(final S3AsyncClient s3Client,
                            final Supplier<String> bucketSupplier,
                            final Supplier<String> keySupplier,
                            final String defaultBucket,
                            final BucketOwnerProvider bucketOwnerProvider) {
        return new MultipartBuffer(new S3OutputStream(s3Client, bucketSupplier, keySupplier, defaultBucket, bucketOwnerProvider));
    }
}
