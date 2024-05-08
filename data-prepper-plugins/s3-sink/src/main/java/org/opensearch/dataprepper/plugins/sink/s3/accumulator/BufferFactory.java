/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.opensearch.dataprepper.plugins.sink.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.function.Supplier;

public interface BufferFactory {
    Buffer getBuffer(S3AsyncClient s3Client, Supplier<String> bucketSupplier, Supplier<String> keySupplier, String defaultBucket, BucketOwnerProvider bucketOwnerProvider);
}
